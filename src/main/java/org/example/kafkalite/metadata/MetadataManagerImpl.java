package org.example.kafkalite.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        // TODO: 编码MetadataRequest，建立TCP连接，发送并解析响应
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
}
