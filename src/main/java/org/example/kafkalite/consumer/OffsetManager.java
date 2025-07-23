package org.example.kafkalite.consumer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.protocol.OffsetCommitRequestBuilder;

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

    public OffsetManager(String groupId, List<String> bootstrapServers) {
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
    }

    // 获取当前offset
    public synchronized long getOffset(String topic, int partition) {
        return offsets.getOrDefault(topic, Collections.emptyMap()).getOrDefault(partition, 0L);
    }

    // 更新 offset
    public synchronized void updateOffset(String topic, int partition, long offset) {
        if (!offsets.containsKey(topic)) {
            offsets.put(topic, new HashMap<>());
        }
        offsets.get(topic).put(partition, offset);
    }

    // 同步提交
    public synchronized void commitSync() {
        try {
            // 1. 构造 OffsetCommitRequest
            ByteBuffer request = OffsetCommitRequestBuilder.build(
                    groupId, offsets, 1, "kafka-ite"
            );
            // 2. 选一个 broker 发送
            String brokerAddress = bootstrapServers.get(0);
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            // 3. 发送并接收响应
            ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, request);
            // 4. 检查 errorCode
            System.out.println("[OffsetManager] commitSync result: " + request);
        } catch (Exception e) {
            System.err.println("[OffsetManager] commitSync failed: " + e.getMessage());
        }
    }

    // 异步提交
    public void commitAsync() {
        new Thread(this::commitSync).start();
    }
}
