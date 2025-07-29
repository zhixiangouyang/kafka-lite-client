package org.example.kafkalite.consumer;

public class PartitionAssignment {
    private final String topic;
    private final int partition;
    
    public PartitionAssignment(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public int getPartition() {
        return partition;
    }
    
    @Override
    public String toString() {
        return String.format("PartitionAssignment(topic=%s, partition=%d)", topic, partition);
    }
} 