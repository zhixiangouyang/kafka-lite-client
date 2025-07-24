package org.example.kafkalite.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {
    private static final int WINDOW_SIZE_MS = 60_000; // 1 minute window
    private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<Long>> latencies = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> windowStartTimes = new ConcurrentHashMap<>();

    public void incrementCounter(String metric) {
        counters.computeIfAbsent(metric, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void recordLatency(String metric, long latencyMs) {
        latencies.computeIfAbsent(metric, k -> Collections.synchronizedList(new ArrayList<>())).add(latencyMs);
    }

    public double getQPS(String metric) {
        long currentTime = System.currentTimeMillis();
        long windowStart = windowStartTimes.computeIfAbsent(metric, k -> currentTime - WINDOW_SIZE_MS);
        long count = counters.getOrDefault(metric, new AtomicLong(0)).get();
        
        // Calculate QPS for the current window
        double windowSizeSeconds = (currentTime - windowStart) / 1000.0;
        return count / windowSizeSeconds;
    }

    public double getP99Latency(String metric) {
        List<Long> metricLatencies = latencies.getOrDefault(metric, new ArrayList<>());
        if (metricLatencies.isEmpty()) {
            return 0.0;
        }

        synchronized (metricLatencies) {
            List<Long> sortedLatencies = new ArrayList<>(metricLatencies);
            Collections.sort(sortedLatencies);
            int index = (int) Math.ceil(sortedLatencies.size() * 0.99) - 1;
            return sortedLatencies.get(index);
        }
    }

    public void resetMetrics(String metric) {
        counters.remove(metric);
        latencies.remove(metric);
        windowStartTimes.put(metric, System.currentTimeMillis());
    }

    // Constants for metric names
    public static final String METRIC_PRODUCER_SEND = "producer.send";
    public static final String METRIC_CONSUMER_POLL = "consumer.poll";
    public static final String METRIC_CONSUMER_COMMIT = "consumer.commit";
} 