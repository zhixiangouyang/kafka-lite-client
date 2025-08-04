package org.example.kafkalite.consumer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.core.KafkaSingleSocketClient;
import org.example.kafkalite.protocol.*;
import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerCoordinator {
    private final String clientId;
    private final String groupId;
    private final ConsumerConfig config;
    private final List<String> subscribedTopics;
    private final List<String> bootstrapServers; // 新增：bootstrapServers
    
    private String coordinatorHost;
    private int coordinatorPort;
    private String memberId = "";
    private int generationId;
    private List<PartitionAssignment> assignments = new ArrayList<>();
    private ScheduledExecutorService heartbeatExecutor;
    public KafkaSingleSocketClient coordinatorSocket;
    
    // 新增：Leader选举和分区分配相关字段
    private boolean isLeader = false;
    private List<MemberInfo> allMembers = new ArrayList<>();
    private PartitionAssignor partitionAssignor = new RangeAssignor();
    private Runnable onSocketUpdatedCallback;
    
    private final MetadataManager metadataManager;
    
    public enum GroupState { UNJOINED, REBALANCING, STABLE }
    private volatile GroupState groupState = GroupState.UNJOINED;
    private volatile boolean isRejoining = false; // 新增：防止重复重新加入组
    
    public ConsumerCoordinator(String clientId, String groupId, ConsumerConfig config, List<String> bootstrapServers) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.config = config;
        this.bootstrapServers = bootstrapServers; // 新增：保存bootstrapServers
        this.subscribedTopics = new ArrayList<>();
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
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
            // 使用bootstrapServers而不是硬编码的localhost:9092
            String bootstrapServer = bootstrapServers.get(0);
            String[] parts = bootstrapServer.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, request);
            FindCoordinatorResponseParser.CoordinatorInfo info = FindCoordinatorResponseParser.parse(response);
            
            if (info.getErrorCode() != 0) {
                throw new RuntimeException("Failed to find coordinator: error=" + info.getErrorCode());
            }
            
            this.coordinatorHost = info.getHost();
            this.coordinatorPort = info.getPort();
            System.out.printf("[ConsumerCoordinator] Found coordinator: %s:%d\n", this.coordinatorHost, this.coordinatorPort);
            
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
            
            // 新增：Leader选举逻辑
            this.isLeader = this.memberId.equals(result.getLeaderId());
            
            // 新增：解析所有成员信息
            this.allMembers.clear();
            for (String memberId : result.getMembers()) {
                // 简化处理：假设所有成员都订阅相同的topics
                this.allMembers.add(new MemberInfo(memberId, subscribedTopics));
            }
            
            System.out.printf("[ConsumerCoordinator] joinGroup success: generationId=%d, memberId=%s, isLeader=%s, members=%s\n", 
                this.generationId, this.memberId, this.isLeader, result.getMembers());
            
        } catch (Exception e) {
            groupState = GroupState.UNJOINED;
            throw new RuntimeException("Failed to join group", e);
        }
    }
    
    private void syncGroup() {
        try {
            groupState = GroupState.REBALANCING;
            
            ByteBuffer request;
            if (isLeader) {
                // Leader需要计算分区分配
                Map<String, List<PartitionAssignment>> assignments = calculatePartitionAssignments();
                request = SyncGroupRequestBuilder.buildWithAssignments(clientId, groupId, generationId, assignments);
                System.out.printf("[ConsumerCoordinator] Leader sending assignments: %s\n", assignments);
            } else {
                // 非Leader发送空的分配
                request = SyncGroupRequestBuilder.build(clientId, groupId, generationId, memberId, subscribedTopics);
                System.out.println("[ConsumerCoordinator] Non-leader sending empty assignment");
            }
            
            ByteBuffer response = coordinatorSocket.sendAndReceive(request);
            SyncGroupResponseParser.SyncGroupResult result = SyncGroupResponseParser.parse(response);
            
            if (result.getErrorCode() != 0) {
                throw new RuntimeException("Failed to sync group: error=" + result.getErrorCode());
            }
            
            this.assignments = result.getAssignments();
            System.out.printf("[ConsumerCoordinator] syncGroup success: assignments=%s\n", this.assignments);
            groupState = GroupState.STABLE;
            
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
    
    // 新增：计算分区分配
    private Map<String, List<PartitionAssignment>> calculatePartitionAssignments() {
        try {
            // 获取所有topic的分区信息
            Map<String, List<Integer>> topicPartitions = getTopicPartitions();
            // 新增：打印每个topic的分区leaders
            for (String topic : topicPartitions.keySet()) {
                if (metadataManager != null) {
                    metadataManager.refreshMetadata(topic);
                    Map<Integer, String> leaders = metadataManager.getPartitionLeaders(topic);
                    System.out.println("[DEBUG] topic=" + topic + " 分区leaders: " + leaders);
                }
            }
            // 使用分区分配策略进行分配
            Map<String, List<PartitionAssignment>> assignments = partitionAssignor.assign(allMembers, topicPartitions);
            
            System.out.printf("[ConsumerCoordinator] Calculated assignments: %s\n", assignments);
            return assignments;
            
        } catch (Exception e) {
            System.err.println("[ConsumerCoordinator] Failed to calculate partition assignments: " + e.getMessage());
            // 返回空分配作为fallback
            Map<String, List<PartitionAssignment>> fallback = new HashMap<>();
            for (MemberInfo member : allMembers) {
                fallback.put(member.getMemberId(), new ArrayList<>());
            }
            return fallback;
        }
    }
    
    // 新增：获取topic分区信息
    private Map<String, List<Integer>> getTopicPartitions() {
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        for (String topic : subscribedTopics) {
            List<Integer> partitions = new ArrayList<>();
            if (metadataManager != null) {
                metadataManager.refreshMetadata(topic);
                Map<Integer, String> leaders = metadataManager.getPartitionLeaders(topic);
                if (leaders != null && !leaders.isEmpty()) {
                    partitions.addAll(leaders.keySet());
                }
            }
            if (partitions.isEmpty()) {
                partitions.add(0); // fallback，防止空分区
            }
            topicPartitions.put(topic, partitions);
        }
        return topicPartitions;
    }
    
    // 新增：设置socket更新回调
    public void setOnSocketUpdatedCallback(Runnable callback) {
        this.onSocketUpdatedCallback = callback;
    }
} 