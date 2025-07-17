package org.example.kafkalite.producer;

public interface Partitioner {
   int partition(String topic, String key, int numPartitions);
}
