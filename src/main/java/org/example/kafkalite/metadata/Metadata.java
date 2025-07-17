package org.example.kafkalite.metadata;

import java.util.Map;

public class Metadata {
    private final Map<Integer, BrokerInfo> brokers;
    private final Map<String, Map<Integer, Integer>> topicPartitionLeaders;

    public Map<Integer, BrokerInfo> getBrokers() {
        return brokers;
    }

    public Map<String, Map<Integer, Integer>> getTopicPartitionLeaders() {
        return topicPartitionLeaders;
    }

    public Metadata(Map<Integer, BrokerInfo> brokers, Map<String, Map<Integer, Integer>> topicPartitionLeaders) {
        this.brokers = brokers;
        this.topicPartitionLeaders = topicPartitionLeaders;
    }
}
