package org.example.kafkalite.consumer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.core.KafkaSingleSocketClient;
import org.example.kafkalite.protocol.OffsetCommitRequestBuilder;
import org.example.kafkalite.protocol.OffsetCommitResponseParser;
import org.example.kafkalite.protocol.OffsetFetchRequestBuilder;
import org.example.kafkalite.protocol.OffsetFetchResponseParser;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetManager {
    private final String groupId;
    private final List<String> bootstrapServers;
    // topic -> partition -> offset
    private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();
    private ConsumerCoordinator coordinator;
    private KafkaSingleSocketClient coordinatorSocket;

    public OffsetManager(String groupId, List<String> bootstrapServers) {
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
    }

    public void setCoordinator(ConsumerCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void setCoordinatorSocket(KafkaSingleSocketClient socket) {
        this.coordinatorSocket = socket;
    }
    
    // 🔧 新增：更新bootstrap servers，用于集群切换
    public synchronized void updateBootstrapServers(List<String> newBootstrapServers) {
        System.out.printf("[OffsetManager] 更新bootstrap servers: %s -> %s\n", 
            this.bootstrapServers, newBootstrapServers);
        
        this.bootstrapServers.clear();
        this.bootstrapServers.addAll(newBootstrapServers);
        
        // 清空本地offset缓存，强制重新从新集群获取
        System.out.println("[OffsetManager] 清空本地offset缓存，准备从新集群重新获取");
        this.offsets.clear();
    }

    // 获取当前offset - 修复重复消费问题
    public synchronized long getOffset(String topic, int partition) {
        Long offset = offsets.getOrDefault(topic, Collections.emptyMap()).get(partition);
        
        // 1. 如果有有效的已提交offset，直接使用
        if (offset != null && offset >= 0) {
            System.out.printf("[OffsetManager] 使用已提交offset: topic=%s, partition=%d, offset=%d\n", 
                topic, partition, offset);
            return offset;
        }
        
        // 2. 如果offset为null或-1，表示该消费者组在此分区没有committed offset
        // 根据策略决定从哪里开始消费
        if (offset == null || offset == -1) {
            System.out.printf("[OffsetManager] 分区无已提交offset: topic=%s, partition=%d, offset=%s\n", 
                topic, partition, offset);
            
            // 这里可以配置策略：
            // - 如果想从最新开始（跳过历史消息）：可以调用ListOffsets获取latest
            // - 如果想从最早开始（消费所有历史消息）：返回0或调用ListOffsets获取earliest
            // - 当前简单策略：从0开始消费（适合大多数情况）
            
            long startOffset = 0L; // 简单策略：从头开始
            System.out.printf("[OffsetManager] 使用起始offset: topic=%s, partition=%d, offset=%d\n", 
                topic, partition, startOffset);
            
            // 更新到本地缓存
            updateOffset(topic, partition, startOffset);
            return startOffset;
        }
        
        // 3. 其他情况（不应该发生）
        System.out.printf("[OffsetManager] 未预期的offset值: topic=%s, partition=%d, offset=%d, 使用0\n", 
            topic, partition, offset);
        return 0L;
    }

    // 更新 offset
    public synchronized void updateOffset(String topic, int partition, long offset) {
        System.out.printf("[DEBUG] updateOffset: topic=%s, partition=%d, offset=%d\n", topic, partition, offset);
        if (!offsets.containsKey(topic)) {
            offsets.put(topic, new HashMap<>());
        }
        offsets.get(topic).put(partition, offset);
    }

    // 同步提交
    public synchronized void commitSync() {
        commitSync(-1, "");
    }
    // v2协议重载
    public synchronized void commitSync(int generationId, String memberId) {
        if (coordinator != null && !coordinator.isStable()) {
            System.err.println("[OffsetManager] Group is not stable (rebalance in progress), skip commit");
            return;
        }
        
        // 新增：检查是否正在重新加入组
        if (coordinator != null && coordinator.isRejoining()) {
            System.err.println("[OffsetManager] Group is rejoining, skip commit");
            return;
        }
        
        System.out.printf("[DEBUG] OffsetManager.commitSync called, thread=%s, generationId=%d, memberId=%s, offsets=%s\n", Thread.currentThread().getName(), generationId, memberId, offsets);
        if (generationId <= 0 || memberId == null || memberId.isEmpty()) {
            System.out.printf("[WARN] group未稳定，跳过本次offset提交: generationId=%d, memberId=%s\n", generationId, memberId);
            return;
        }
        try {
            System.out.printf("[DEBUG] commitSync: generationId=%d, memberId=%s, offsets=%s\n", generationId, memberId, offsets);
            System.out.println("[DEBUG] offsets to commit: " + offsets);
            // 1. 构造 OffsetCommitRequest v2
            // 修复：使用正确的clientId而不是硬编码的"kafka-ite"
            String realClientId = coordinator != null ? coordinator.getClientId() : "kafka-lite";
            
            ByteBuffer request = OffsetCommitRequestBuilder.build(
                    groupId, offsets, 1, realClientId, generationId, memberId, 86400000L  // 使用24小时作为保留时间
            );
            // 2. 发送并接收响应
            // 获取最新的coordinatorSocket引用
            KafkaSingleSocketClient currentSocket = coordinator != null ? coordinator.getCoordinatorSocket() : coordinatorSocket;
            ByteBuffer response = currentSocket != null ?
                currentSocket.sendAndReceive(request) :
                KafkaSocketClient.sendAndReceive(bootstrapServers.get(0).split(":")[0], Integer.parseInt(bootstrapServers.get(0).split(":")[1]), request);
            // 3. 解析响应
            byte[] respBytes = new byte[response.remaining()];
            response.mark();
            response.get(respBytes);
            response.reset();
            System.out.print("[OffsetCommitResponse] 响应字节流: ");
            for (byte b : respBytes) System.out.printf("%02x ", b);
            System.out.println();
            Map<String, Map<Integer, Short>> result = null;
            try {
                result = org.example.kafkalite.protocol.OffsetCommitResponseParser.parse(response);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
            for (Map.Entry<String, Map<Integer, Long>> topicEntry : offsets.entrySet()) {
                String topic = topicEntry.getKey();
                for (Map.Entry<Integer, Long> partEntry : topicEntry.getValue().entrySet()) {
                    int partition = partEntry.getKey();
                    long offset = partEntry.getValue();
                    short error = result.getOrDefault(topic, Collections.emptyMap()).getOrDefault(partition, (short) -1);
                    if (error == 0) {
                        System.out.printf("[Commit] topic=%s, partition=%d, offset=%d, SUCCESS\n", topic, partition, offset);
                    } else {
                        System.out.printf("[Commit] topic=%s, partition=%d, offset=%d, ERROR_CODE=%d\n", topic, partition, offset, error);
                        if (error == 16) {
                            System.err.println("[OffsetManager] Commit failed with ILLEGAL_GENERATION, skip this commit and wait for next poll/rebalance");
                            // 通知coordinator重新加入组
                            if (coordinator != null) {
                                System.out.println("[OffsetManager] Triggering rejoin group due to ILLEGAL_GENERATION");
                                coordinator.triggerRejoinGroup();
                            }
                            return;
                        }
                    }
                }
            }
            
            // 新增：commit成功后验证offset是否真的提交了
            // System.out.println("[OffsetManager] Commit成功，验证offset是否真的提交...");
            // verifyCommittedOffsets();
        } catch (Exception e) {
            System.err.println("[Commit] commitSync failed: " + e.getMessage());
        }
    }
    
    // 新增：验证已提交的offset
    private void verifyCommittedOffsets() {
        try {
            // 等待一段时间让offset真正写入
            System.out.println("[OffsetManager] 等待1秒让offset写入...");
            Thread.sleep(1000);
            
            // 构造查询请求
            java.util.Map<String, Integer[]> topicParts = new java.util.HashMap<>();
            for (Map.Entry<String, Map<Integer, Long>> entry : offsets.entrySet()) {
                String topic = entry.getKey();
                java.util.List<Integer> parts = new java.util.ArrayList<>(entry.getValue().keySet());
                topicParts.put(topic, parts.toArray(new Integer[0]));
            }
            
            // 修复：使用正确的clientId进行offset查询
            String realClientId = coordinator != null ? coordinator.getClientId() : "kafka-lite";
            java.nio.ByteBuffer request = org.example.kafkalite.protocol.OffsetFetchRequestBuilder.build(
                    groupId, topicParts, 1, realClientId
            );
            
            java.util.Map<String, java.util.Map<Integer, Long>> committed;
            
            // 使用coordinator而不是bootstrap server
            if (coordinator != null && coordinator.getCoordinatorSocket() != null) {
                System.out.printf("[OffsetManager] 验证时使用coordinator: %s:%d\n", 
                    coordinator.getCoordinatorSocket().getHost(), coordinator.getCoordinatorSocket().getPort());
                java.nio.ByteBuffer response = coordinator.getCoordinatorSocket().sendAndReceive(request);
                committed = org.example.kafkalite.protocol.OffsetFetchResponseParser.parse(response);
            } else {
                // 回退到使用bootstrap server
                String brokerAddress = bootstrapServers.get(0);
                String[] parts = brokerAddress.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.printf("[OffsetManager] 验证时使用bootstrap server: %s:%d\n", host, port);
                java.nio.ByteBuffer response = org.example.kafkalite.core.KafkaSocketClient.sendAndReceive(host, port, request);
                committed = org.example.kafkalite.protocol.OffsetFetchResponseParser.parse(response);
            }
            
            System.out.printf("[OffsetManager] 验证结果 - 期望的offset: %s\n", offsets);
            System.out.printf("[OffsetManager] 验证结果 - 实际提交的offset: %s\n", committed);
            
            // 比较期望和实际的offset
            for (Map.Entry<String, Map<Integer, Long>> entry : offsets.entrySet()) {
                String topic = entry.getKey();
                for (Map.Entry<Integer, Long> part : entry.getValue().entrySet()) {
                    int partition = part.getKey();
                    long expectedOffset = part.getValue();
                    long actualOffset = committed.getOrDefault(topic, new java.util.HashMap<>()).getOrDefault(partition, -1L);
                    if (actualOffset == expectedOffset) {
                        System.out.printf("[OffsetManager] 验证成功: topic=%s, partition=%d, offset=%d\n", topic, partition, expectedOffset);
                    } else {
                        System.err.printf("[OffsetManager] 验证失败: topic=%s, partition=%d, 期望=%d, 实际=%d\n", topic, partition, expectedOffset, actualOffset);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[OffsetManager] 验证offset失败: " + e.getMessage());
        }
    }

    // 异步提交
    public void commitAsync() {
        System.out.printf("[DEBUG] OffsetManager.commitAsync called, thread=%s\n", Thread.currentThread().getName());
        // 获取当前的generationId和memberId
        int generationId = coordinator != null ? coordinator.getGenerationId() : -1;
        String memberId = coordinator != null ? coordinator.getMemberId() : "";
        new Thread(() -> commitSync(generationId, memberId)).start();
    }

    // 新增：从 broker 查询 group offset
    public synchronized void fetchCommittedOffsets(java.util.List<String> topics, java.util.Map<String, java.util.List<Integer>> topicPartitions) {
        try {
            System.out.printf("[OffsetManager] 开始查询已提交的offset: groupId=%s, topics=%s\n", groupId, topics);
            // 构造请求参数
            Map<String, Integer[]> topicParts = new HashMap<>();
            for (String topic : topics) {
                List<Integer> parts = topicPartitions.get(topic);
                if (parts != null) {
                    topicParts.put(topic, parts.toArray(new Integer[0]));
                }
            }
            // 修复：使用正确的clientId进行offset查询
            String realClientId = coordinator != null ? coordinator.getClientId() : "kafka-lite";
            ByteBuffer request = OffsetFetchRequestBuilder.build(
                    groupId, topicParts, 1, realClientId
            );
            
            // 使用coordinator而不是bootstrap server
            if (coordinator != null && coordinator.getCoordinatorSocket() != null) {
                System.out.printf("[OffsetManager] 使用coordinator查询offset: %s:%d\n", 
                    coordinator.getCoordinatorSocket().getHost(), coordinator.getCoordinatorSocket().getPort());
                java.nio.ByteBuffer response = coordinator.getCoordinatorSocket().sendAndReceive(request);
                java.util.Map<String, java.util.Map<Integer, Long>> committed = org.example.kafkalite.protocol.OffsetFetchResponseParser.parse(response);
                System.out.printf("[OffsetManager] 查询到的已提交offset: %s\n", committed);
                for (java.util.Map.Entry<String, java.util.Map<Integer, Long>> entry : committed.entrySet()) {
                    String topic = entry.getKey();
                    for (java.util.Map.Entry<Integer, Long> part : entry.getValue().entrySet()) {
                        long fetchedOffset = part.getValue();
                        if (fetchedOffset >= 0) {
                            // 只有当offset>=0时才更新，offset=-1表示没有committed offset
                            updateOffset(topic, part.getKey(), fetchedOffset);
                            System.out.printf("[OffsetManager] 找到已提交offset: topic=%s, partition=%d, offset=%d\n", 
                                topic, part.getKey(), fetchedOffset);
                        } else {
                            System.out.printf("[OffsetManager] 分区无已提交offset: topic=%s, partition=%d, offset=%d\n", 
                                topic, part.getKey(), fetchedOffset);
                            // 不更新本地缓存，保持null状态，让getOffset方法处理
                        }
                    }
                }
            } else {
                // 回退到使用bootstrap server
            String brokerAddress = bootstrapServers.get(0);
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
                System.out.printf("[OffsetManager] 使用bootstrap server查询offset: %s:%d\n", host, port);
            java.nio.ByteBuffer response = org.example.kafkalite.core.KafkaSocketClient.sendAndReceive(host, port, request);
            java.util.Map<String, java.util.Map<Integer, Long>> committed = org.example.kafkalite.protocol.OffsetFetchResponseParser.parse(response);
                System.out.printf("[OffsetManager] 查询到的已提交offset: %s\n", committed);
            for (java.util.Map.Entry<String, java.util.Map<Integer, Long>> entry : committed.entrySet()) {
                String topic = entry.getKey();
                for (java.util.Map.Entry<Integer, Long> part : entry.getValue().entrySet()) {
                    long fetchedOffset = part.getValue();
                    if (fetchedOffset >= 0) {
                        // 只有当offset>=0时才更新，offset=-1表示没有committed offset
                        updateOffset(topic, part.getKey(), fetchedOffset);
                        System.out.printf("[OffsetManager] 找到已提交offset: topic=%s, partition=%d, offset=%d\n", 
                            topic, part.getKey(), fetchedOffset);
                    } else {
                        System.out.printf("[OffsetManager] 分区无已提交offset: topic=%s, partition=%d, offset=%d\n", 
                            topic, part.getKey(), fetchedOffset);
                        // 不更新本地缓存，保持null状态，让getOffset方法处理
                    }
                    }
                }
            }
            System.out.println("[OffsetManager] fetchCommittedOffsets 完成: " + offsets);
        } catch (Exception e) {
            System.err.println("[OffsetManager] fetchCommittedOffsets 失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 关闭socket
    public void close() {
        if (coordinatorSocket != null) {
            coordinatorSocket.close();
        }
    }
}
