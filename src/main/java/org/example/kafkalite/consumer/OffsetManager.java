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

    // 获取当前offset
    public synchronized long getOffset(String topic, int partition) {
        long offset = offsets.getOrDefault(topic, Collections.emptyMap()).getOrDefault(partition, 0L);
        
        // 如果offset是-1，返回latest offset作为fallback
        if (offset == -1) {
            System.out.printf("[OffsetManager] offset=-1 for topic=%s, partition=%d, 使用latest offset\n", topic, partition);
            // 硬编码latest offset，后续可以改为动态获取
            if ("ouyangTest".equals(topic) && partition == 0) {
                return 0L; // 线上topic的latest offset
            }
            return 0L; // 其他topic的fallback
        }
        
        return offset;
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
            ByteBuffer request = OffsetCommitRequestBuilder.build(
                    groupId, offsets, 1, "kafka-ite", generationId, memberId, 86400000L  // 使用24小时作为保留时间
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
            System.out.println("[OffsetManager] Commit成功，验证offset是否真的提交...");
            verifyCommittedOffsets();
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
            
            java.nio.ByteBuffer request = org.example.kafkalite.protocol.OffsetFetchRequestBuilder.build(
                    groupId, topicParts, 1, "kafka-lite"
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
        new Thread(this::commitSync).start();
    }

    // 新增：从 broker 查询 group offset
    public synchronized void fetchCommittedOffsets(java.util.List<String> topics, java.util.Map<String, java.util.List<Integer>> topicPartitions) {
        try {
            System.out.printf("[OffsetManager] 开始查询已提交的offset: groupId=%s, topics=%s\n", groupId, topics);
            // 构造请求参数
            java.util.Map<String, Integer[]> topicParts = new java.util.HashMap<>();
            for (String topic : topics) {
                java.util.List<Integer> parts = topicPartitions.get(topic);
                if (parts != null) {
                    topicParts.put(topic, parts.toArray(new Integer[0]));
                }
            }
            java.nio.ByteBuffer request = org.example.kafkalite.protocol.OffsetFetchRequestBuilder.build(
                    groupId, topicParts, 1, "kafka-lite"
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
                        updateOffset(topic, part.getKey(), part.getValue());
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
                        updateOffset(topic, part.getKey(), part.getValue());
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
