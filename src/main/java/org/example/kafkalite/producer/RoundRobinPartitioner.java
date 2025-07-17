package org.example.kafkalite.producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartitioner implements Partitioner{
    private final ConcurrentHashMap<String, AtomicInteger> topicCounter = new ConcurrentHashMap<>();
    @Override
    public int partition(String topic, String key, int numPartitions) {
        topicCounter.putIfAbsent(topic, new AtomicInteger(0));
        int idx = topicCounter.get(topic).getAndIncrement();
        return idx % numPartitions;
    }
}
