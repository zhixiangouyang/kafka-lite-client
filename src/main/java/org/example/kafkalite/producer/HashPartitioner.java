package org.example.kafkalite.producer;

public class HashPartitioner implements Partitioner{
    @Override
    public int partition(String topic, String key, int numPartitions) {
        if (key == null) return 0;
        return Math.abs(key.hashCode()) % numPartitions;
    }
}
