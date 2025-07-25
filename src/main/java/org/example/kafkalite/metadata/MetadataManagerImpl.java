package org.example.kafkalite.metadata;

import org.example.kafkalite.core.KafkaSocketClient;

import java.nio.ByteBuffer;
import java.util.*;

public class MetadataManagerImpl implements MetadataManager {
    private final List<String> bootstrapServers;

    // 缓存：topic -> partition -> leader broker地址
    private final Map<String, Map<Integer, String>> topicPartitionLeaders = new HashMap<>();

    // 缓存：nodeId -> brokerInfo
    private final Map<Integer, BrokerInfo> brokerMap = new HashMap<>();

    public MetadataManagerImpl(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void refreshMetadata(String topic) {
        try {
            // System.out.println("[MetadataManagerImpl] Refreshing metadata for topic: " + topic);
            
            // 1. 编码 MetadataRequest 请求体
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            ByteBuffer request = MetadataRequestEncoder.encodeMetadataRequest(topics, 1);

            // 2. 选一个broker发请求
            String brokerAddress = bootstrapServers.get(0);
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            // System.out.println("[MetadataManagerImpl] Sending request to " + host + ":" + port);

            // 3. 使用Socket 发送并接收Kafka响应
            ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, request);

            // 4. 解析响应
            Metadata metadata = MetadataResponseParser.parse(response);
            // System.out.println("[MetadataManagerImpl] Received metadata: " + metadata);

            // 5. 缓存更新
            brokerMap.clear();
            brokerMap.putAll(metadata.getBrokers());

            Map<Integer, String> leaders = metadata.getPartitionLeaders(topic);
            if (leaders != null) {
                topicPartitionLeaders.put(topic, leaders);
                // System.out.println("[MetadataManagerImpl] Updated partition leaders for topic " + topic + ": " + leaders);
            } else {
                // System.err.println("[MetadataManagerImpl] No partition leaders found for topic: " + topic);
            }

        } catch (Exception e) {
            // System.err.println("[MetadataManagerImpl] Failed to refresh metadata: " + e.getMessage());
            throw new RuntimeException("Failed to refresh metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<Integer, String> getPartitionLeaders(String topic) {
        return topicPartitionLeaders.getOrDefault(topic, Collections.emptyMap());
    }
}
