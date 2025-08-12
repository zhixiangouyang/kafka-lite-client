package org.example.kafkalite.consumer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.core.KafkaSingleSocketClient;
import org.example.kafkalite.protocol.*;
import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.monitor.MetricsCollector;

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
    
    // 用于分区分配变更通知
    public final Object assignmentLock = new Object();
    
    private MetadataManager metadataManager;
    
    // 📊 指标收集器
    private final MetricsCollector metricsCollector;
    
    public enum GroupState { UNJOINED, REBALANCING, STABLE }
    private volatile GroupState groupState = GroupState.UNJOINED;
    private volatile boolean isRejoining = false; // 新增：防止重复重新加入组
    
    public ConsumerCoordinator(String clientId, String groupId, ConsumerConfig config, List<String> bootstrapServers) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.config = config;
        this.bootstrapServers = bootstrapServers; // 新增：保存bootstrapServers
        this.subscribedTopics = new ArrayList<>();
        this.metadataManager = null; // 将通过 setMetadataManager 注入
        this.metricsCollector = new MetricsCollector("consumer-coordinator", groupId);
    }
    
    // 新增：设置共享的 MetadataManager
    public void setMetadataManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }
    
    public void initializeGroup(List<String> topics) {
        this.subscribedTopics.clear();
        this.subscribedTopics.addAll(topics);
        
        // 添加JVM关闭钩子，确保在进程被强制终止时也能正确清理
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[ShutdownHook] JVM shutting down, cleaning up consumer group...");
            try {
                close();
            } catch (Exception e) {
                System.err.println("[ShutdownHook] Error during cleanup: " + e.getMessage());
            }
        }));
        
        try {
            groupState = GroupState.REBALANCING;
            System.out.printf("[DEBUG] initializeGroup: clientId=%s, groupId=%s, memberId=%s, topics=%s\n", clientId, groupId, memberId, topics);
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
        Exception lastException = null;
        
        // 尝试所有bootstrap servers找到协调器
        for (String bootstrapServer : bootstrapServers) {
        try {
            ByteBuffer request = FindCoordinatorRequestBuilder.build(clientId, groupId, 1);
            String[] parts = bootstrapServer.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
                
                System.out.printf("[ConsumerCoordinator] 尝试从broker %s:%d 查找协调器\n", host, port);
            ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, request);
            FindCoordinatorResponseParser.CoordinatorInfo info = FindCoordinatorResponseParser.parse(response);
            
                if (info.getErrorCode() == 0) {
            this.coordinatorHost = info.getHost();
            this.coordinatorPort = info.getPort();
                    System.out.printf("[ConsumerCoordinator] 成功找到协调器: %s:%d (通过broker %s:%d)\n",
                        this.coordinatorHost, this.coordinatorPort, host, port);
                    return; // 成功找到，直接返回
                } else {
                    System.out.printf("❌ [ConsumerCoordinator] Broker %s:%d 返回错误: errorCode=%d\n", 
                        host, port, info.getErrorCode());
                    lastException = new RuntimeException("Failed to find coordinator: error=" + info.getErrorCode());
                }
            
        } catch (Exception e) {
                System.out.printf("❌ [ConsumerCoordinator] 无法连接到broker %s: %s\n", bootstrapServer, e.getMessage());
                lastException = e;
            }
        }
        
        // 如果所有broker都失败了
        throw new RuntimeException("Failed to find coordinator from any broker", lastException);
    }
    
    private void joinGroup() {
        joinGroupWithRetry(0);
    }
    
    private void joinGroupWithRetry(int retryCount) {
        System.out.printf("[DEBUG] joinGroupWithRetry: retryCount=%d, clientId=%s, groupId=%s, memberId=%s\n", retryCount, clientId, groupId, memberId);
        
        long startTime = System.currentTimeMillis();
        // 📊 指标埋点: JoinGroup尝试
        Map<String, String> labels = new HashMap<>();
        labels.put("group_id", groupId);
        labels.put("retry_count", String.valueOf(retryCount));
        metricsCollector.incrementCounter("coordinator.join_group.attempt", labels);
        
        try {
            groupState = GroupState.REBALANCING;
            System.out.printf("[DEBUG] joinGroup: clientId=%s, groupId=%s, memberId=%s, topics=%s, retryCount=%d\n", clientId, groupId, memberId, subscribedTopics, retryCount);
            
            // 如果是重试，先等待一段时间让 Coordinator 处理完之前的请求
            if (retryCount > 0) {
                int waitTime = 2000; // 固定2秒
                System.out.printf("[DEBUG] Waiting %d ms before retry...\n", waitTime);
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during joinGroup retry", ie);
                }
            }
            
            ByteBuffer request = JoinGroupRequestBuilder.build(clientId, groupId, memberId, subscribedTopics);
            ByteBuffer response = coordinatorSocket.sendAndReceive(request);
            JoinGroupResponseParser.JoinGroupResult result = JoinGroupResponseParser.parse(response);
            
            System.out.printf("[DEBUG] JoinGroup response received: errorCode=%d, generationId=%d, leaderId=%s, memberId=%s, members=%s\n", 
                result.getErrorCode(), result.getGenerationId(), result.getLeaderId(), result.getMemberId(), result.getMembers());
            
            if (result.getErrorCode() != 0) {
                throw new RuntimeException("Failed to join group: error=" + result.getErrorCode());
            }
            
            // 保存新的memberId和generationId
            String newMemberId = result.getMemberId();
            int newGenerationId = result.getGenerationId();
            
            // 只有在memberId不为空时才更新
            if (newMemberId != null && !newMemberId.isEmpty()) {
                this.memberId = newMemberId;
            }
            this.generationId = newGenerationId;
            
            // 新增：Leader选举逻辑
            this.isLeader = this.memberId.equals(result.getLeaderId());
            
            // 解析所有成员信息（无论Leader还是非Leader都要完整填充）
            this.allMembers.clear();
            for (String memberId : result.getMembers()) {
                if (memberId != null && !memberId.isEmpty()) {
                    this.allMembers.add(new MemberInfo(memberId, subscribedTopics));
                }
            }
            // 确保自己一定在allMembers中
            if (!this.allMembers.stream().anyMatch(member -> member.getMemberId().equals(this.memberId))) {
                this.allMembers.add(new MemberInfo(this.memberId, subscribedTopics));
                System.out.printf("[DEBUG] Added self to allMembers: memberId=%s, allMembers=%s\n", this.memberId, this.allMembers);
            }
            
            System.out.printf("[ConsumerCoordinator] joinGroup success: generationId=%d, memberId=%s, isLeader=%s, members=%s\n", 
                this.generationId, this.memberId, this.isLeader, result.getMembers());
            System.out.printf("[DEBUG] joinGroup result: protocolName=%s, leaderId=%s, memberId=%s, members=%s, allMembers=%s\n", 
                result.getProtocolName(), result.getLeaderId(), result.getMemberId(), result.getMembers(), this.allMembers);
            
            // 📊 指标埋点: JoinGroup成功
            long endTime = System.currentTimeMillis();
            labels.put("is_leader", String.valueOf(isLeader));
            labels.put("member_count", String.valueOf(result.getMembers().size()));
            metricsCollector.incrementCounter("coordinator.join_group.success", labels);
            metricsCollector.recordLatency("coordinator.join_group.latency", endTime - startTime, labels);
            
            // 新增：调试信息
            System.out.printf("[DEBUG] joinGroup completed: clientId=%s, memberId=%s, isLeader=%s, allMembers.size=%d\n", 
                clientId, this.memberId, this.isLeader, this.allMembers.size());
            
            // 新增：检测ghost consumer问题（仅在多消费者场景下）
            if (this.isLeader && result.getMembers().size() == 1 && result.getMembers().get(0).equals(this.memberId)) {
                // 检查是否有其他消费者应该在这个组中
                // 这里我们通过检查是否有其他消费者在同一个组中运行来判断
                // 暂时禁用这个检测，因为单消费者场景下这是正常行为
                System.out.printf("[DEBUG] Leader only sees itself in group (this is normal for single consumer)\n");
                // 注释掉ghost consumer检测逻辑，因为单消费者场景下这是正常行为
                /*
                if (retryCount < 1) {
                    System.out.printf("[INFO] Attempting to trigger rebalance to clean up ghost consumers (retryCount=%d)...\n", retryCount);
                    // 等待一段时间让服务端清理ghost consumer
                    Thread.sleep(5000);
                    joinGroupWithRetry(retryCount + 1);
                    return;
                }
                */
            }
            
        } catch (Exception e) {
            groupState = GroupState.UNJOINED;
            System.err.printf("[ERROR] joinGroup failed: clientId=%s, groupId=%s, memberId=%s, topics=%s, error=%s\n", clientId, groupId, memberId, subscribedTopics, e.getMessage());
            
            // 📊 指标埋点: JoinGroup失败
            labels.put("error_type", e.getClass().getSimpleName());
            metricsCollector.incrementCounter("coordinator.join_group.error", labels);
            
            // 如果是超时错误且还有重试次数，则重试
            if ((e instanceof java.net.SocketTimeoutException || e.getCause() instanceof java.net.SocketTimeoutException) && retryCount < 3) {
                System.out.printf("[WARN] Socket timeout detected, retrying joinGroup (retryCount=%d)...\n", retryCount);
                
                // 📊 指标埋点: JoinGroup重试
                metricsCollector.incrementCounter("coordinator.join_group.retry", labels);
                
                joinGroupWithRetry(retryCount + 1);
                return;
            }
            
            throw new RuntimeException("Failed to join group", e);
        }
    }
    
    private void syncGroup() {
        syncGroupWithRetry(0);
    }
    
    private void syncGroupWithRetry(int retryCount) {
        System.out.printf("[DEBUG] syncGroupWithRetry: retryCount=%d, clientId=%s, groupId=%s, memberId=%s, isLeader=%s\n", 
            retryCount, clientId, groupId, memberId, isLeader);
        try {
            groupState = GroupState.REBALANCING;
            ByteBuffer request;
            int expectedMemberCount = allMembers.size(); // 默认
            if (isLeader) {
                // 修正：只要有成员（自己），就可以分配
                if (allMembers.isEmpty() && retryCount < 10) {
                    System.out.printf("[Leader] 等待至少有一个成员: allMembers.size=%d, retryCount=%d\n", allMembers.size(), retryCount);
                    Thread.sleep(1000 * (retryCount + 1));
                    syncGroupWithRetry(retryCount + 1);
                    return;
                }
                Map<String, List<PartitionAssignment>> assignments = calculatePartitionAssignments();
                request = SyncGroupRequestBuilder.buildWithAssignments(clientId, groupId, generationId, memberId, assignments);
                System.out.printf("[ConsumerCoordinator] Leader sending assignments: %s\n", assignments);
                System.out.printf("[DEBUG] Leader syncGroup - clientId=%s, allMembers=%s, assignments=%s\n", 
                    clientId, allMembers, assignments);
                // Leader分配后不再主动rejoinGroup，只需正常syncGroup
                try {
                    ByteBuffer heartbeatReq = HeartbeatRequestBuilder.build(clientId, groupId, generationId, memberId);
                    ByteBuffer heartbeatResp = coordinatorSocket.sendAndReceive(heartbeatReq);
                    short errorCode = HeartbeatResponseParser.parse(heartbeatResp);
                    System.out.printf("[Leader] 主动心跳: errorCode=%d\n", errorCode);
                } catch (Exception e) {
                    System.err.println("[Leader] 主动心跳异常: " + e);
                }
            } else {
                // 非Leader等待Leader分配完成
                if (allMembers.isEmpty() && retryCount < 10) {
                    System.out.printf("[Non-leader] 等待Leader分配完成: allMembers.size=%d, retryCount=%d\n", allMembers.size(), retryCount);
                    Thread.sleep(1000 * (retryCount + 1));
                    syncGroupWithRetry(retryCount + 1);
                    return;
                }
                System.out.printf("[DEBUG] Non-leader syncGroup: memberId=%s, allMembers=%s, retryCount=%d\n", memberId, allMembers, retryCount);
                // 非Leader发送空的分配信息，等待Coordinator返回Leader的分配结果
                request = SyncGroupRequestBuilder.buildEmptyAssignment(clientId, groupId, generationId, memberId);
                System.out.println("[ConsumerCoordinator] Non-leader sending empty assignment");
            }
            ByteBuffer response = coordinatorSocket.sendAndReceive(request);
            SyncGroupResponseParser.SyncGroupResult result = SyncGroupResponseParser.parse(response);
            
            System.out.printf("[DEBUG] SyncGroup response received: errorCode=%d, assignments.size=%d, assignments=%s\n", 
                result.getErrorCode(), result.getAssignments().size(), result.getAssignments());
            
            if (result.getErrorCode() != 0) {
                // 特殊处理 REBALANCE_IN_PROGRESS/ILLEGAL_GENERATION 错误
                if ((result.getErrorCode() == 27 || result.getErrorCode() == 25 || result.getErrorCode() == 22) && retryCount < 10) {
                    System.out.printf("[WARN] Rebalance in progress/illegal generation during syncGroup, retrying (retryCount=%d)...\n", retryCount);
                    Thread.sleep(1000 * (retryCount + 1));
                    syncGroupWithRetry(retryCount + 1);
                    return;
                }
                throw new RuntimeException("Failed to sync group: error=" + result.getErrorCode());
            }
            this.assignments = result.getAssignments();
            System.out.printf("[ConsumerCoordinator] syncGroup success: assignments=%s\n", this.assignments);
            System.out.printf("[DEBUG] syncGroup completed: clientId=%s, memberId=%s, isLeader=%s, assignments.size=%d\n", 
                clientId, memberId, isLeader, this.assignments.size());
            
            // 新增：如果非Leader收到空的分配信息，记录警告但继续执行
            if (!this.isLeader && this.assignments.isEmpty()) {
                System.out.printf("[WARN] Non-leader received empty assignments, but continuing with empty assignment\n");
            }
            
            groupState = GroupState.STABLE;
            System.out.printf("[DEBUG] Group state changed to STABLE: clientId=%s, groupId=%s, memberId=%s\n", 
                clientId, groupId, memberId);
            synchronized (assignmentLock) {
                assignmentLock.notifyAll();
            }
        } catch (Exception e) {
            groupState = GroupState.UNJOINED;
            System.err.printf("[ERROR] syncGroupWithRetry failed: clientId=%s, groupId=%s, error=%s\n", clientId, groupId, e.getMessage());
            if ((e instanceof java.net.SocketTimeoutException || e.getCause() instanceof java.net.SocketTimeoutException) && retryCount < 3) {
                System.out.printf("[WARN] Socket timeout during syncGroup, retrying (retryCount=%d)...\n", retryCount);
                try {
                    Thread.sleep(1000 * (retryCount + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during syncGroup retry", ie);
                }
                syncGroupWithRetry(retryCount + 1);
                return;
            }
            throw new RuntimeException("Failed to sync group", e);
        }
    }
    
    private void startHeartbeat() {
        System.out.printf("[DEBUG] startHeartbeat: clientId=%s, groupId=%s, memberId=%s, generationId=%d\n", 
            clientId, groupId, memberId, generationId);
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            long heartbeatStart = System.currentTimeMillis();
            
            // 📊 指标埋点: 心跳尝试
            metricsCollector.incrementCounter("coordinator.heartbeat.attempt");
            
            try {
                // 如果正在重新加入组，跳过本次心跳
                if (isRejoining) {
                    System.out.printf("[DEBUG] Skipping heartbeat due to rejoin in progress: clientId=%s, groupId=%s\n", clientId, groupId);
                    // 📊 指标埋点: 心跳跳过
                    metricsCollector.incrementCounter("coordinator.heartbeat.skipped");
                    return;
                }
                
                // 新增：检查当前状态
                System.out.printf("[DEBUG] Heartbeat check - clientId=%s, groupId=%s, memberId=%s, generationId=%d, groupState=%s, assignments.size=%d\n", 
                    clientId, groupId, memberId, generationId, groupState, assignments.size());
                
                ByteBuffer request = HeartbeatRequestBuilder.build(clientId, groupId, generationId, memberId);
                
                
                ByteBuffer response = coordinatorSocket.sendAndReceive(request);
                short errorCode = HeartbeatResponseParser.parse(response);
                System.out.printf("[HeartbeatResponse] errorCode=%d for clientId=%s, groupId=%s, memberId=%s\n", 
                    errorCode, clientId, groupId, memberId);
                
                if (errorCode == 0) {
                    System.out.printf("[ConsumerCoordinator] Heartbeat success for clientId=%s, groupId=%s\n", clientId, groupId);
                    
                    // 📊 指标埋点: 心跳成功
                    long heartbeatLatency = System.currentTimeMillis() - heartbeatStart;
                    metricsCollector.incrementCounter("coordinator.heartbeat.success");
                    metricsCollector.recordLatency("coordinator.heartbeat.latency", heartbeatLatency);
                    
                    // 新增：定期检测组成员变化
                    // heartbeatCounter++;
                    // if (heartbeatCounter >= MEMBERSHIP_CHECK_INTERVAL) {
                    //     System.out.printf("[DEBUG] Performing periodic membership check for clientId=%s (heartbeatCounter=%d)\n", clientId, heartbeatCounter);
                    //     checkGroupMembership();
                    //     heartbeatCounter = 0; // 重置计数器
                    // }
                } else if (errorCode == 25 || errorCode == 27) { // REBALANCE_IN_PROGRESS
                    System.out.printf("[ConsumerCoordinator] Rebalance in progress detected (errorCode=%d)! clientId=%s, groupId=%s, will rejoin group\n", errorCode, clientId, groupId);
                    
                    // 📊 指标埋点: 心跳触发重平衡
                    metricsCollector.incrementCounter("coordinator.heartbeat.rebalance_triggered");
                    
                    // 重新加入组
                    rejoinGroup();
                } else if (errorCode == 22) { // ILLEGAL_GENERATION
                    System.err.printf("[ConsumerCoordinator] Illegal generation detected! clientId=%s, groupId=%s, will rejoin group\n", clientId, groupId);
                    
                    // 📊 指标埋点: 心跳检测到非法世代
                    metricsCollector.incrementCounter("coordinator.heartbeat.illegal_generation");
                    
                    // 重新加入组
                    rejoinGroup();
                } else {
                    System.err.printf("[ConsumerCoordinator] Heartbeat failed with error: %d for clientId=%s, groupId=%s\n", errorCode, clientId, groupId);
                    
                    // 📊 指标埋点: 心跳失败
                    Map<String, String> errorLabels = new HashMap<>();
                    errorLabels.put("error_code", String.valueOf(errorCode));
                    metricsCollector.incrementCounter("coordinator.heartbeat.error", errorLabels);
                    
                    // 对于其他错误，也尝试重新加入组
                    rejoinGroup();
                }
                
            } catch (Exception e) {
                System.err.printf("[ConsumerCoordinator] Failed to send heartbeat for clientId=%s, groupId=%s: %s\n", clientId, groupId, e.getMessage());
                // 心跳异常时也重新加入组
                rejoinGroup();
            }
        }, 0, config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }
    
    // 新增：重新加入组的方法
    private synchronized void rejoinGroup() {
        System.out.printf("[DEBUG] rejoinGroup called: clientId=%s, groupId=%s, isRejoining=%s\n", clientId, groupId, isRejoining);
        if (isRejoining) {
            System.out.println("[ConsumerCoordinator] Already re-joining, skipping...");
            return;
        }
        try {
            isRejoining = true; // 设置标志
            System.out.printf("[DEBUG] rejoinGroup: clientId=%s, groupId=%s, memberId=%s, topics=%s\n", clientId, groupId, memberId, subscribedTopics);
            System.out.println("[ConsumerCoordinator] Rejoining group...");
            groupState = GroupState.REBALANCING;
            
            // 关闭旧的socket连接
            if (coordinatorSocket != null) {
                try {
                    System.out.println("[DEBUG] Closing old coordinatorSocket");
                    coordinatorSocket.close();
                } catch (Exception e) {
                    System.out.println("[DEBUG] coordinatorSocket already closed or error: " + e);
                }
            }
            
            // 🔧 关键修复：重新查找协调器（可能已经切换到其他broker）
            try {
                System.out.println("[ConsumerCoordinator] 重新查找协调器...");
                findCoordinator();
            } catch (Exception e) {
                System.err.printf("[ConsumerCoordinator] 重新查找协调器失败: %s\n", e.getMessage());
                throw e;
            }
            
            // 重新建立连接到新的协调器
            this.coordinatorSocket = new KafkaSingleSocketClient(coordinatorHost, coordinatorPort);
            System.out.printf("[DEBUG] coordinatorSocket status: %s\n", coordinatorSocket == null ? "null" : "open");
            
            // 重新加入组
            joinGroup();
            // 重新同步组
            syncGroup();
            
            System.out.println("[ConsumerCoordinator] Successfully rejoined group");
            synchronized (assignmentLock) {
                assignmentLock.notifyAll();
            }
        } catch (Exception e) {
            System.err.printf("[ERROR] rejoinGroup failed: clientId=%s, groupId=%s, memberId=%s, topics=%s, error=%s\n", clientId, groupId, memberId, subscribedTopics, e.getMessage());
            groupState = GroupState.UNJOINED;
        } finally {
            isRejoining = false; // 重置标志
            System.out.printf("[DEBUG] Rejoin completed, isRejoining=false: clientId=%s, groupId=%s\n", clientId, groupId);
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
    
    public String getClientId() {
        return clientId;
    }
    
    public boolean isStable() {
        return groupState == GroupState.STABLE;
    }
    
    public boolean isRejoining() {
        return isRejoining;
    }
    
    public void close() {
        try {
            System.out.printf("[DEBUG] close() called: clientId=%s, groupId=%s, memberId=%s, groupState=%s\n", 
                clientId, groupId, memberId, groupState);
            
            // 先停止心跳
            if (heartbeatExecutor != null) {
                heartbeatExecutor.shutdown();
                try {
                    heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            // 发送LeaveGroup请求，通知服务端离开组
            System.out.printf("[DEBUG] LeaveGroup conditions check: coordinatorSocket=%s, memberId.isEmpty()=%s, groupState!=UNJOINED=%s\n", 
                coordinatorSocket != null ? "not null" : "null", 
                memberId.isEmpty(), 
                groupState != GroupState.UNJOINED);
            
            if (coordinatorSocket != null && !memberId.isEmpty() && groupState != GroupState.UNJOINED) {
                try {
                    System.out.printf("[DEBUG] Sending LeaveGroup request: memberId=%s, groupId=%s\n", memberId, groupId);
                    ByteBuffer request = LeaveGroupRequestBuilder.build(clientId, groupId, memberId);
                    ByteBuffer response = coordinatorSocket.sendAndReceive(request);
                    short errorCode = LeaveGroupResponseParser.parse(response);
                    System.out.printf("[DEBUG] LeaveGroup response: errorCode=%d\n", errorCode);
                } catch (Exception e) {
                    System.err.printf("[WARN] Failed to send LeaveGroup request: %s\n", e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.out.printf("[DEBUG] Skip LeaveGroup request: coordinatorSocket=%s, memberId=%s, groupState=%s\n", 
                    coordinatorSocket != null ? "not null" : "null", memberId, groupState);
            }
            
            // 关闭socket连接
            if (coordinatorSocket != null) {
                coordinatorSocket.close();
            }
            
            groupState = GroupState.UNJOINED;
            System.out.printf("[DEBUG] ConsumerCoordinator closed: clientId=%s, groupId=%s, memberId=%s\n", clientId, groupId, memberId);
            
        } catch (Exception e) {
            System.err.printf("[ERROR] Error during close: %s\n", e.getMessage());
            e.printStackTrace();
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
        Map<String, List<PartitionAssignment>> assignments = new HashMap<>();
        
        // 过滤掉空字符串的memberId
        List<MemberInfo> validMembers = new ArrayList<>();
        for (MemberInfo member : allMembers) {
            if (member.getMemberId() != null && !member.getMemberId().isEmpty()) {
                validMembers.add(member);
            }
        }
        
        System.out.printf("[DEBUG] calculatePartitionAssignments: allMembers=%s, validMembers=%s\n", allMembers, validMembers);
        
        if (validMembers.isEmpty()) {
            System.out.println("[WARN] No valid members found, skipping partition assignment");
            return assignments;
        }
        
        // 获取topic分区信息
        Map<String, List<Integer>> topicPartitions = getTopicPartitions();
        System.out.printf("[DEBUG] calculatePartitionAssignments: topicPartitions=%s\n", topicPartitions);
        
        // 使用分区分配器
        assignments = partitionAssignor.assign(validMembers, topicPartitions);
        
        System.out.printf("[DEBUG] calculatePartitionAssignments: assignments=%s\n", assignments);
        return assignments;
    }
    
    // 新增：获取topic分区信息
    private Map<String, List<Integer>> getTopicPartitions() {
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        for (String topic : subscribedTopics) {
            List<Integer> partitions = new ArrayList<>();
            if (metadataManager != null) {
                // 获取topic分区信息 - 消费者上下文，正常情况
                metadataManager.refreshMetadata(topic, false, false);
                Map<Integer, String> leaders = metadataManager.getPartitionLeaders(topic);
                System.out.println("[DEBUG] topic=" + topic + " 分区leaders: " + leaders);
                if (leaders != null && !leaders.isEmpty()) {
                    partitions.addAll(leaders.keySet());
                }
            }
            if (partitions.isEmpty()) {
                partitions.add(0); // fallback
            }
            topicPartitions.put(topic, partitions);
        }
        System.out.printf("[DEBUG] getTopicPartitions: topicPartitions=%s\n", topicPartitions);
        return topicPartitions;
    }
    
    // 新增：设置socket更新回调
    public void setOnSocketUpdatedCallback(Runnable callback) {
        this.onSocketUpdatedCallback = callback;
    }

    // 新增：主动检测组成员变化的方法
    private void checkGroupMembership() {
        try {
            System.out.printf("[DEBUG] checkGroupMembership: clientId=%s, groupId=%s, memberId=%s, allMembers.size=%d\n", 
                clientId, groupId, memberId, allMembers.size());
            
            // 如果正在重新加入组，跳过检测
            if (isRejoining) {
                System.out.printf("[DEBUG] Skipping membership check due to rejoin in progress: clientId=%s\n", clientId);
                return;
            }
            
            // 构造一个简单的JoinGroup请求来获取当前组成员信息
            ByteBuffer request = JoinGroupRequestBuilder.build(clientId, groupId, memberId, subscribedTopics);
            ByteBuffer response = coordinatorSocket.sendAndReceive(request);
            JoinGroupResponseParser.JoinGroupResult result = JoinGroupResponseParser.parse(response);
            
            System.out.printf("[DEBUG] Membership check response: errorCode=%d, members=%s, current allMembers=%s\n", 
                result.getErrorCode(), result.getMembers(), allMembers);
            
            if (result.getErrorCode() == 0) {
                // 检查组成员是否有变化
                List<String> currentMembers = result.getMembers();
                boolean membershipChanged = false;
                
                // 检查成员数量变化
                if (currentMembers.size() != allMembers.size()) {
                    membershipChanged = true;
                    System.out.printf("[DEBUG] Member count changed: old=%d, new=%d\n", allMembers.size(), currentMembers.size());
                }
                
                // 检查具体成员变化
                for (String member : currentMembers) {
                    boolean found = allMembers.stream().anyMatch(m -> m.getMemberId().equals(member));
                    if (!found) {
                        membershipChanged = true;
                        System.out.printf("[DEBUG] New member detected: %s\n", member);
                    }
                }
                
                for (MemberInfo member : allMembers) {
                    boolean found = currentMembers.contains(member.getMemberId());
                    if (!found) {
                        membershipChanged = true;
                        System.out.printf("[DEBUG] Member left: %s\n", member.getMemberId());
                    }
                }
                
                if (membershipChanged) {
                    System.out.printf("[DEBUG] Group membership changed! Triggering rejoin for clientId=%s\n", clientId);
                    rejoinGroup();
                } else {
                    System.out.printf("[DEBUG] No membership change detected for clientId=%s\n", clientId);
                }
            } else if (result.getErrorCode() == 25) { // REBALANCE_IN_PROGRESS
                System.out.printf("[DEBUG] Rebalance in progress detected during membership check for clientId=%s\n", clientId);
                rejoinGroup();
            } else {
                System.out.printf("[DEBUG] Membership check failed with error: %d for clientId=%s\n", result.getErrorCode(), clientId);
            }
            
        } catch (Exception e) {
            System.err.printf("[DEBUG] Membership check failed for clientId=%s: %s\n", clientId, e.getMessage());
        }
    }
    
    // 新增：心跳计数器，用于定期检测组成员变化
    private int heartbeatCounter = 0;
    private static final int MEMBERSHIP_CHECK_INTERVAL = 5; // 每5次心跳检测一次组成员变化
} 