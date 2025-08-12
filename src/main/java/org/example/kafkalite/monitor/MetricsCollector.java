package org.example.kafkalite.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MetricsCollector {
    private static final int WINDOW_SIZE_MS = 60_000; // 1 minute window
    private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<Long>> latencies = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> windowStartTimes = new ConcurrentHashMap<>();
    
    // 新增：支持标签的指标存储
    private final ConcurrentMap<MetricKey, AtomicLong> labeledCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<MetricKey, List<Long>> labeledLatencies = new ConcurrentHashMap<>();
    private final ConcurrentMap<MetricKey, AtomicReference<Double>> gauges = new ConcurrentHashMap<>();
    
    // 客户端标识，用于区分不同的客户端实例
    private final String clientId;
    private final String instanceId;
    
    public MetricsCollector() {
        this("kafka-lite-client", generateInstanceId());
    }
    
    public MetricsCollector(String clientId, String instanceId) {
        this.clientId = clientId;
        this.instanceId = instanceId;
    }
    
    private static String generateInstanceId() {
        return "instance-" + System.currentTimeMillis() % 10000;
    }

    // 原有方法保持不变
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
    
    // 新增：支持标签的指标方法
    public void incrementCounter(String metric, Map<String, String> labels) {
        MetricKey key = new MetricKey(metric, labels);
        labeledCounters.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    public void recordLatency(String metric, long latencyMs, Map<String, String> labels) {
        MetricKey key = new MetricKey(metric, labels);
        labeledLatencies.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(latencyMs);
    }
    
    public void setGauge(String metric, double value) {
        setGauge(metric, value, Collections.emptyMap());
    }
    
    public void setGauge(String metric, double value, Map<String, String> labels) {
        MetricKey key = new MetricKey(metric, labels);
        gauges.computeIfAbsent(key, k -> new AtomicReference<>(0.0)).set(value);
    }
    
    // 新增：Prometheus格式输出方法
    public String toPrometheusFormat() {
        StringBuilder sb = new StringBuilder();
        
        // 输出Counter类型指标
        exportCounters(sb);
        
        // 输出Gauge类型指标
        exportGauges(sb);
        
        // 输出延迟指标（转换为Gauge）
        exportLatencies(sb);
        
        // 输出QPS指标（转换为Gauge）
        exportQPS(sb);
        
        return sb.toString();
    }
    
    private void exportCounters(StringBuilder sb) {
        // 导出无标签的计数器
        for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
            String metricName = sanitizeMetricName(entry.getKey());
            sb.append("# HELP ").append(metricName).append("_total Total count of ").append(entry.getKey()).append("\n");
            sb.append("# TYPE ").append(metricName).append("_total counter\n");
            sb.append(metricName).append("_total{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"} ");
            sb.append(entry.getValue().get()).append("\n\n");
        }
        
        // 导出有标签的计数器
        for (Map.Entry<MetricKey, AtomicLong> entry : labeledCounters.entrySet()) {
            MetricKey key = entry.getKey();
            String metricName = sanitizeMetricName(key.name);
            
            sb.append("# HELP ").append(metricName).append("_total Total count of ").append(key.name).append("\n");
            sb.append("# TYPE ").append(metricName).append("_total counter\n");
            sb.append(metricName).append("_total{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"");
            
            for (Map.Entry<String, String> label : key.labels.entrySet()) {
                sb.append(",").append(label.getKey()).append("=\"").append(label.getValue()).append("\"");
            }
            sb.append("} ").append(entry.getValue().get()).append("\n\n");
        }
    }
    
    private void exportGauges(StringBuilder sb) {
        for (Map.Entry<MetricKey, AtomicReference<Double>> entry : gauges.entrySet()) {
            MetricKey key = entry.getKey();
            String metricName = sanitizeMetricName(key.name);
            
            sb.append("# HELP ").append(metricName).append(" Current value of ").append(key.name).append("\n");
            sb.append("# TYPE ").append(metricName).append(" gauge\n");
            sb.append(metricName).append("{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"");
            
            for (Map.Entry<String, String> label : key.labels.entrySet()) {
                sb.append(",").append(label.getKey()).append("=\"").append(label.getValue()).append("\"");
            }
            sb.append("} ").append(entry.getValue().get()).append("\n\n");
        }
    }
    
    private void exportLatencies(StringBuilder sb) {
        // 导出无标签的延迟指标
        for (Map.Entry<String, List<Long>> entry : latencies.entrySet()) {
            String metricName = sanitizeMetricName(entry.getKey()) + "_latency";
            double p99 = getP99Latency(entry.getKey());
            double avg = getAverageLatency(entry.getKey());
            
            // P99延迟
            sb.append("# HELP ").append(metricName).append("_p99_milliseconds P99 latency for ").append(entry.getKey()).append("\n");
            sb.append("# TYPE ").append(metricName).append("_p99_milliseconds gauge\n");
            sb.append(metricName).append("_p99_milliseconds{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"} ");
            sb.append(p99).append("\n\n");
            
            // 平均延迟
            sb.append("# HELP ").append(metricName).append("_avg_milliseconds Average latency for ").append(entry.getKey()).append("\n");
            sb.append("# TYPE ").append(metricName).append("_avg_milliseconds gauge\n");
            sb.append(metricName).append("_avg_milliseconds{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"} ");
            sb.append(avg).append("\n\n");
        }
        
        // 导出有标签的延迟指标
        for (Map.Entry<MetricKey, List<Long>> entry : labeledLatencies.entrySet()) {
            MetricKey key = entry.getKey();
            String metricName = sanitizeMetricName(key.name) + "_latency";
            double p99 = getP99Latency(key);
            double avg = getAverageLatency(key);
            
            // P99延迟
            sb.append("# HELP ").append(metricName).append("_p99_milliseconds P99 latency for ").append(key.name).append("\n");
            sb.append("# TYPE ").append(metricName).append("_p99_milliseconds gauge\n");
            sb.append(metricName).append("_p99_milliseconds{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"");
            
            for (Map.Entry<String, String> label : key.labels.entrySet()) {
                sb.append(",").append(label.getKey()).append("=\"").append(label.getValue()).append("\"");
            }
            sb.append("} ").append(p99).append("\n\n");
            
            // 平均延迟
            sb.append("# HELP ").append(metricName).append("_avg_milliseconds Average latency for ").append(key.name).append("\n");
            sb.append("# TYPE ").append(metricName).append("_avg_milliseconds gauge\n");
            sb.append(metricName).append("_avg_milliseconds{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"");
            
            for (Map.Entry<String, String> label : key.labels.entrySet()) {
                sb.append(",").append(label.getKey()).append("=\"").append(label.getValue()).append("\"");
            }
            sb.append("} ").append(avg).append("\n\n");
        }
    }
    
    private void exportQPS(StringBuilder sb) {
        for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
            String metricName = sanitizeMetricName(entry.getKey()) + "_rate";
            double qps = getQPS(entry.getKey());
            
            sb.append("# HELP ").append(metricName).append("_per_second Rate per second for ").append(entry.getKey()).append("\n");
            sb.append("# TYPE ").append(metricName).append("_per_second gauge\n");
            sb.append(metricName).append("_per_second{client_id=\"").append(clientId).append("\",instance_id=\"").append(instanceId).append("\"} ");
            sb.append(qps).append("\n\n");
        }
    }
    
    // 辅助方法
    private double getAverageLatency(String metric) {
        List<Long> metricLatencies = latencies.getOrDefault(metric, new ArrayList<>());
        if (metricLatencies.isEmpty()) {
            return 0.0;
        }
        
        synchronized (metricLatencies) {
            return metricLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }
    }
    
    private double getP99Latency(MetricKey key) {
        List<Long> metricLatencies = labeledLatencies.getOrDefault(key, new ArrayList<>());
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
    
    private double getAverageLatency(MetricKey key) {
        List<Long> metricLatencies = labeledLatencies.getOrDefault(key, new ArrayList<>());
        if (metricLatencies.isEmpty()) {
            return 0.0;
        }
        
        synchronized (metricLatencies) {
            return metricLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }
    }
    
    private String sanitizeMetricName(String name) {
        // Prometheus指标名称只能包含字母、数字、下划线和冒号
        return name.replaceAll("[^a-zA-Z0-9:_]", "_").toLowerCase();
    }
    
    // 内部类：支持标签的指标键
    private static class MetricKey {
        final String name;
        final Map<String, String> labels;
        
        MetricKey(String name, Map<String, String> labels) {
            this.name = name;
            this.labels = labels != null ? new ConcurrentHashMap<>(labels) : new ConcurrentHashMap<>();
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            
            MetricKey metricKey = (MetricKey) o;
            
            if (!name.equals(metricKey.name)) return false;
            return labels.equals(metricKey.labels);
        }
        
        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + labels.hashCode();
            return result;
        }
        
        @Override
        public String toString() {
            return "MetricKey{name='" + name + "', labels=" + labels + "}";
        }
    }

    // Constants for metric names
    public static final String METRIC_PRODUCER_SEND = "producer.send";
    public static final String METRIC_CONSUMER_POLL = "consumer.poll";
    public static final String METRIC_CONSUMER_COMMIT = "consumer.commit";
    
    // 新增：更多指标常量
    public static final String METRIC_PRODUCER_SEND_SUCCESS = "producer.send.success";
    public static final String METRIC_PRODUCER_SEND_ERROR = "producer.send.error";
    public static final String METRIC_PRODUCER_BATCH_SIZE = "producer.batch.size";
    public static final String METRIC_CONSUMER_FETCH_SUCCESS = "consumer.fetch.success";
    public static final String METRIC_CONSUMER_FETCH_ERROR = "consumer.fetch.error";
    public static final String METRIC_CONSUMER_LAG = "consumer.lag";
    public static final String METRIC_CONNECTION_POOL_ACTIVE = "connection.pool.active";
    public static final String METRIC_CONNECTION_POOL_IDLE = "connection.pool.idle";
    public static final String METRIC_DNS_RESOLUTION = "dns.resolution";
    public static final String METRIC_DR_SWITCH = "dr.switch";
    public static final String METRIC_METADATA_REFRESH = "metadata.refresh";
} 