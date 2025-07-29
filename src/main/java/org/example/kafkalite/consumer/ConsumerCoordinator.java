package org.example.kafkalite.consumer;

import org.example.kafkalite.core.KafkaSocketClient;
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
    
    public ConsumerCoordinator(String clientId, String groupId, ConsumerConfig config) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.config = config;
        this.subscribedTopics = new ArrayList<>();
    }
    
    public void initializeGroup(List<String> topics) {
        this.subscribedTopics.clear();
        this.subscribedTopics.addAll(topics);
        
        findCoordinator();
        joinGroup();
        syncGroup();
        startHeartbeat();
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
            ByteBuffer request = JoinGroupRequestBuilder.build(clientId, groupId, memberId, subscribedTopics);
            ByteBuffer response = KafkaSocketClient.sendAndReceive(coordinatorHost, coordinatorPort, request);
            JoinGroupResponseParser.JoinGroupResult result = JoinGroupResponseParser.parse(response);
            
            if (result.getErrorCode() != 0) {
                throw new RuntimeException("Failed to join group: error=" + result.getErrorCode());
            }
            
            this.memberId = result.getMemberId();
            this.generationId = result.getGenerationId();
            // System.out.println("[ConsumerCoordinator] Joined group: " + result);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to join group", e);
        }
    }
    
    private void syncGroup() {
        try {
            ByteBuffer request = SyncGroupRequestBuilder.build(clientId, groupId, generationId, memberId, subscribedTopics);
            ByteBuffer response = KafkaSocketClient.sendAndReceive(coordinatorHost, coordinatorPort, request);
            SyncGroupResponseParser.SyncGroupResult result = SyncGroupResponseParser.parse(response);
            
            if (result.getErrorCode() != 0) {
                throw new RuntimeException("Failed to sync group: error=" + result.getErrorCode());
            }
            
            this.assignments = result.getAssignments();
            // System.out.println("[ConsumerCoordinator] Synced group, assignments: " + result.getAssignments());
            
        } catch (Exception e) {
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
                ByteBuffer request = HeartbeatRequestBuilder.build(clientId, groupId, generationId, memberId);
                ByteBuffer response = KafkaSocketClient.sendAndReceive(coordinatorHost, coordinatorPort, request);
                short errorCode = HeartbeatResponseParser.parse(response);
                
                if (errorCode != 0) {
                    System.err.println("[ConsumerCoordinator] Heartbeat failed with error: " + errorCode);
                }
                
            } catch (Exception e) {
                System.err.println("[ConsumerCoordinator] Failed to send heartbeat: " + e);
            }
        }, 0, config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
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
    
    public void close() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            try {
                heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
} 