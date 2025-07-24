package org.example.kafkalite.consumer;

import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.monitor.MetricsCollector;
import org.example.kafkalite.protocol.FetchRequestBuilder;
import org.example.kafkalite.protocol.FetchResponseParser;
import org.example.kafkalite.core.KafkaSocketClient;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaLiteConsumerImpl implements KafkaLiteConsumer {
    private final List<String> bootstrapServers;
    private final String groupId;
    private final MetadataManager metadataManager;
    private final OffsetManager offsetManager;
    private final ConsumerConfig config;
    private final MetricsCollector metricsCollector;
    private List<String> subscribedTopics = new ArrayList<>();
    private final Map<String, Map<Integer, String>> topicPartitionLeaders = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;

    public KafkaLiteConsumerImpl(String groupId, List<String> bootstrapServers, ConsumerConfig config) {
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
        this.config = config;
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
        this.offsetManager = new OffsetManager(groupId, bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.scheduler = Executors.newScheduledThreadPool(1);

        // 如果启用了自动提交，启动定时任务
        if (config.isEnableAutoCommit()) {
            scheduler.scheduleAtFixedRate(
                this::commitAsync,
                config.getAutoCommitIntervalMs(),
                config.getAutoCommitIntervalMs(),
                TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    public void subscribe(List<String> topics) {
        this.subscribedTopics = topics;
        for (String topic : topics) {
            metadataManager.refreshMetadata(topic);
            topicPartitionLeaders.put(topic, metadataManager.getPartitionLeaders(topic));
        }
    }

    @Override
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }

        long startTime = System.currentTimeMillis();
        List<ConsumerRecord> allRecords = new ArrayList<>();
        
        try {
            for (String topic : subscribedTopics) {
                Map<Integer, String> partitionLeaders = topicPartitionLeaders.get(topic);
                if (partitionLeaders == null) continue;

                for (Map.Entry<Integer, String> entry : partitionLeaders.entrySet()) {
                    if (allRecords.size() >= config.getMaxPollRecords()) {
                        break;
                    }

                    int partition = entry.getKey();
                    String broker = entry.getValue();
                    String[] parts = broker.split(":");
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    long offset = offsetManager.getOffset(topic, partition);

                    int retryCount = 0;
                    while (retryCount < config.getMaxRetries()) {
                        try {
                            ByteBuffer fetchRequest = FetchRequestBuilder.build(
                                "kafka-lite",
                                topic,
                                partition,
                                offset,
                                config.getFetchMaxBytes(),
                                1
                            );

                            ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, fetchRequest);
                            List<ConsumerRecord> records = FetchResponseParser.parse(response);
                            allRecords.addAll(records);

                            // 更新offset
                            if (!records.isEmpty()) {
                                long lastOffset = records.get(records.size() - 1).getOffset();
                                offsetManager.updateOffset(topic, partition, lastOffset + 1);
                            }
                            break;
                        } catch (Exception e) {
                            retryCount++;
                            if (retryCount >= config.getMaxRetries()) {
                                System.err.println("Failed to fetch from topic=" + topic + ", partition=" + partition + " after " + config.getMaxRetries() + " retries");
                                // 处理leader切换
                                metadataManager.refreshMetadata(topic);
                                topicPartitionLeaders.put(topic, metadataManager.getPartitionLeaders(topic));
                            } else {
                                try {
                                    Thread.sleep(config.getRetryBackoffMs());
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException("Interrupted while retrying", ie);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            // 记录监控指标
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_POLL);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_POLL, endTime - startTime);
        }

        return allRecords;
    }

    @Override
    public void commitSync() {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }

        long startTime = System.currentTimeMillis();
        try {
            offsetManager.commitSync();
        } finally {
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_COMMIT);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_COMMIT, endTime - startTime);
        }
    }

    @Override
    public void commitAsync() {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }

        long startTime = System.currentTimeMillis();
        try {
            offsetManager.commitAsync();
        } finally {
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_COMMIT);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_COMMIT, endTime - startTime);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                // 停止自动提交调度器
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                }

                // 最后一次提交offset
                if (config.isEnableAutoCommit()) {
                    commitSync();
                }
            } finally {
                // 清理资源
                subscribedTopics.clear();
                topicPartitionLeaders.clear();
            }
        }
    }

    // 获取消费监控指标
    public double getConsumerQPS() {
        return metricsCollector.getQPS(MetricsCollector.METRIC_CONSUMER_POLL);
    }

    public double getConsumerP99Latency() {
        return metricsCollector.getP99Latency(MetricsCollector.METRIC_CONSUMER_POLL);
    }

    public double getCommitQPS() {
        return metricsCollector.getQPS(MetricsCollector.METRIC_CONSUMER_COMMIT);
    }

    public double getCommitP99Latency() {
        return metricsCollector.getP99Latency(MetricsCollector.METRIC_CONSUMER_COMMIT);
    }
} 