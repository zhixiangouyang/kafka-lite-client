package org.example.kafkalite.metadata;

import org.example.kafkalite.core.KafkaSocketClient;

import java.nio.ByteBuffer;
import java.util.*;

public class MetadataManagerImpl implements MetadataManager{
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
            // 1. 编码 MetadataRequest 请求体
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            ByteBuffer request = MetadataRequestEncoder.encodeMetadataRequest(topics, 1);

            // 2. 选一个broker发请求
            String brokerAddress = bootstrapServers.get(0);
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            // 3. 使用Socket 发送并接收Kafka响应
            ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, request);

            // 4. 解析响应
            Metadata metadata = MetadataResponseParser.parse(response);

            // 5. 缓存更新
            brokerMap.clear();
            brokerMap.putAll(metadata.getBrokers());

            topicPartitionLeaders.put(topic, buildPartitionLeaderMap(metadata, topic));

        } catch (Exception e) {
            throw new RuntimeException("Failed to refresh metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<Integer, String> getPartitionLeaders(String topic) {
        return topicPartitionLeaders.getOrDefault(topic, Collections.emptyMap());
    }

    @Override
    public int getPartitionCount(String topic) {
        Map<Integer, String> leaderMap = topicPartitionLeaders.get(topic);
        return leaderMap == null ? 0 : leaderMap.size();
    }

    private Map<Integer, String> buildPartitionLeaderMap(Metadata metadata, String topic) {
        Map<Integer, String> result = new HashMap<>();

        Map<Integer, BrokerInfo> brokers = metadata.getBrokers();
        Map<String, Map<Integer, Integer>> topicPartitions = metadata.getTopicPartitionLeaders();

        Map<Integer, Integer> partitionToLeaderId = topicPartitions.get(topic);
        if (partitionToLeaderId == null) {
            return result;
        }

        partitionToLeaderId.forEach((partition, leaderId) -> {
            BrokerInfo leader = brokers.get(leaderId);
            if (leader != null) {
                result.put(partition, leader.getHost() + ":" + leader.getPort());
            }
        });


        return result;
    }
}
