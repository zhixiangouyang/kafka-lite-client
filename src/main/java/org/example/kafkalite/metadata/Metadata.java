package org.example.kafkalite.metadata;

import java.util.Map;
import java.util.HashMap;

public class Metadata {
    private final Map<Integer, BrokerInfo> brokers;
    private final Map<String, Map<Integer, PartitionInfo>> topics;
    
    public Metadata(Map<Integer, BrokerInfo> brokers, Map<String, Map<Integer, PartitionInfo>> topics) {
        this.brokers = brokers;
        this.topics = topics;
    }

    public Map<Integer, BrokerInfo> getBrokers() {
        return brokers;
    }

    public Map<String, Map<Integer, PartitionInfo>> getTopics() {
        return topics;
    }
    
    public Map<Integer, String> getPartitionLeaders(String topic) {
        Map<Integer, PartitionInfo> partitions = topics.get(topic);
        if (partitions == null) {
            return null;
        }
        
        Map<Integer, String> leaders = new HashMap<>();
        for (Map.Entry<Integer, PartitionInfo> entry : partitions.entrySet()) {
            leaders.put(entry.getKey(), entry.getValue().getLeaderAddress());
        }
        return leaders;
    }
    
    @Override
    public String toString() {
        return "Metadata{" +
                "brokers=" + brokers +
                ", topics=" + topics +
                '}';
    }
}
