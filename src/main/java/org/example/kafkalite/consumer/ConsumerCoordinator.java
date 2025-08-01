package org.example.kafkalite.consumer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.core.KafkaSingleSocketClient;
import org.example.kafkalite.protocol.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerCoordinator {
    private final String clientId;
    private final String groupId;
    private final ConsumerConfig config;
    private final List<String> subscribedTopics;
    
    private String coordinatorHost;
    private int coordinatorPort;
    private String memberId = "";
    private int generationId;
    private List<PartitionAssignment> assignments = new ArrayList<>();
    private ScheduledExecutorService heartbeatExecutor;
    public KafkaSingleSocketClient coordinatorSocket;
    
    public enum GroupState { UNJOINED, REBALANCING, STABLE }
    private volatile GroupState groupState = GroupState.UNJOINED;
    private volatile boolean isRejoining = false; // 新增：防止重复重新加入组
    
    public ConsumerCoordinator(String clientId, String groupId, ConsumerConfig config) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.config = config;
        this.subscribedTopics = new ArrayList<>();
    }
    
    public void initializeGroup(List<String> topics) {
        this.subscribedTopics.clear();
        this.subscribedTopics.addAll(topics);
        try {
            groupState = GroupState.REBALANCING;
            findCoordinator();
            this.coordinatorSocket = new KafkaSingleSocketClient(coordinatorHost, coordinatorPort);
            joinGroup();
            syncGroup();
            startHeartbeat();
        } catch (Exception e) {
            if (coordinatorSocket != null) coordinatorSocket.close();
            groupState = GroupState.UNJOINED;
            throw new RuntimeException(e);
        }
    }
    
    private void findCoordinator() {
        try {
            ByteBuffer request = FindCoordinatorRequestBuilder.build(clientId, groupId, 1);
            ByteBuffer response = KafkaSocketClient.sendAndReceive("localhost", 9092, request);
            FindCoordinatorResponseParser.CoordinatorInfo info = FindCoordinatorResponseParser.parse(response);
            
            if (info.getErrorCode() != 0) {
                throw new RuntimeException("Failed to find coordinator: error=" + info.getErrorCode());
            }
            
            this.coordinatorHost = info.getHost();
            this.coordinatorPort = info.getPort();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to find coordinator", e);
        }
    }
    
    private void joinGroup() {
        try {
            groupState = GroupState.REBALANCING;
            ByteBuffer request = JoinGroupRequestBuilder.build(clientId, groupId, memberId, subscribedTopics);
            ByteBuffer response = coordinatorSocket.sendAndReceive(request);
            JoinGroupResponseParser.JoinGroupResult result = JoinGroupResponseParser.parse(response);
            
            if (result.getErrorCode() != 0) {
                throw new RuntimeException("Failed to join group: error=" + result.getErrorCode());
            }
            
            this.memberId = result.getMemberId();
            this.generationId = result.getGenerationId();
            System.out.printf("[ConsumerCoordinator] joinGroup success: generationId=%d, memberId=%s\n", this.generationId, this.memberId);
            groupState = GroupState.STABLE;
            // System.out.println("[ConsumerCoordinator] Joined group: " + result);
            
        } catch (Exception e) {
            groupState = GroupState.UNJOINED;
            throw new RuntimeException("Failed to join group", e);
        }
    }
    
    private void syncGroup() {
        try {
            groupState = GroupState.REBALANCING;
            ByteBuffer request = SyncGroupRequestBuilder.build(clientId, groupId, generationId, memberId, subscribedTopics);
            ByteBuffer response = coordinatorSocket.sendAndReceive(request);
            SyncGroupResponseParser.SyncGroupResult result = SyncGroupResponseParser.parse(response);
            
            if (result.getErrorCode() != 0) {
                throw new RuntimeException("Failed to sync group: error=" + result.getErrorCode());
            }
            
            this.assignments = result.getAssignments();
            System.out.printf("[ConsumerCoordinator] syncGroup success: assignments=%s\n", this.assignments);
            groupState = GroupState.STABLE;
            // System.out.println("[ConsumerCoordinator] Synced group, assignments: " + result.getAssignments());
            
        } catch (Exception e) {
            groupState = GroupState.UNJOINED;
            throw new RuntimeException("Failed to sync group", e);
        }
    }
    
    private void startHeartbeat() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                // 如果正在重新加入组，跳过本次心跳
                if (isRejoining) {
                    System.out.println("[ConsumerCoordinator] Skipping heartbeat due to rejoin in progress");
                    return;
                }
                
                ByteBuffer request = HeartbeatRequestBuilder.build(clientId, groupId, generationId, memberId);
                System.out.printf("[HeartbeatRequestBuilder] 请求字节流: %s\n", bytesToHex(request));
                ByteBuffer response = coordinatorSocket.sendAndReceive(request);
                short errorCode = HeartbeatResponseParser.parse(response);
                System.out.printf("[HeartbeatResponse] errorCode=%d\n", errorCode);
                if (errorCode == 0) {
                    System.out.println("[ConsumerCoordinator] Heartbeat success");
                } else if (errorCode == 25) { // REBALANCE_IN_PROGRESS
                    System.out.println("[ConsumerCoordinator] Rebalance in progress, will rejoin group");
                    // 重新加入组
                    rejoinGroup();
                } else if (errorCode == 22) { // ILLEGAL_GENERATION
                    System.err.println("[ConsumerCoordinator] Illegal generation, will rejoin group");
                    // 重新加入组
                    rejoinGroup();
                } else {
                    System.err.println("[ConsumerCoordinator] Heartbeat failed with error: " + errorCode);
                    // 对于其他错误，也尝试重新加入组
                    rejoinGroup();
                }
                
            } catch (Exception e) {
                System.err.println("[ConsumerCoordinator] Failed to send heartbeat: " + e);
                // 心跳异常时也重新加入组
                rejoinGroup();
            }
        }, 0, config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }
    
    // 新增：重新加入组的方法
    private synchronized void rejoinGroup() {
        if (isRejoining) {
            System.out.println("[ConsumerCoordinator] Already re-joining, skipping...");
            return;
        }
        try {
            isRejoining = true; // 设置标志
            System.out.println("[ConsumerCoordinator] Rejoining group...");
            groupState = GroupState.REBALANCING;
            
            // 关闭旧的socket连接
            if (coordinatorSocket != null) {
                coordinatorSocket.close();
            }
            
            // 重新建立连接
            this.coordinatorSocket = new KafkaSingleSocketClient(coordinatorHost, coordinatorPort);
            
            // 重新加入组
            joinGroup();
            // 重新同步组
            syncGroup();
            
            System.out.println("[ConsumerCoordinator] Successfully rejoined group");
        } catch (Exception e) {
            System.err.println("[ConsumerCoordinator] Failed to rejoin group: " + e.getMessage());
            groupState = GroupState.UNJOINED;
        } finally {
            isRejoining = false; // 重置标志
        }
    }
    
    // 新增：公共方法，供OffsetManager调用
    public synchronized void triggerRejoinGroup() {
        System.out.println("[ConsumerCoordinator] Triggering rejoin group from external call");
        rejoinGroup();
    }
    
    // 新增：获取当前的coordinatorSocket
    public KafkaSingleSocketClient getCoordinatorSocket() {
        return coordinatorSocket;
    }
    
    public List<PartitionAssignment> getAssignments() {
        return assignments;
    }
    
    public int getGenerationId() {
        return generationId;
    }
    public String getMemberId() {
        return memberId;
    }
    
    public boolean isStable() {
        return groupState == GroupState.STABLE;
    }
    
    public boolean isRejoining() {
        return isRejoining;
    }
    
    public void close() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            try {
                heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (coordinatorSocket != null) {
            coordinatorSocket.close();
        }
    }

    // 工具方法：打印字节流
    private static String bytesToHex(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        buffer.mark();
        while (buffer.hasRemaining()) {
            sb.append(String.format("%02x ", buffer.get()));
        }
        buffer.reset();
        return sb.toString();
    }
} 