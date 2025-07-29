package org.example.kafkalite.consumer;

import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.monitor.MetricsCollector;
import org.example.kafkalite.protocol.FetchRequestBuilder;
import org.example.kafkalite.protocol.FetchResponseParser;
import org.example.kafkalite.protocol.SyncGroupResponseParser;
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
    private final String clientId;
    private final MetadataManager metadataManager;
    private final OffsetManager offsetManager;
    private final ConsumerConfig config;
    private final MetricsCollector metricsCollector;
    private final ConsumerCoordinator coordinator;
    private List<String> subscribedTopics = new ArrayList<>();
    private final Map<String, Map<Integer, String>> topicPartitionLeaders = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;

    public KafkaLiteConsumerImpl(String groupId, List<String> bootstrapServers, ConsumerConfig config) {
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
        this.config = config;
        this.clientId = "kafka-lite-" + UUID.randomUUID().toString().substring(0, 8);
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
        this.offsetManager = new OffsetManager(groupId, bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.coordinator = new ConsumerCoordinator(clientId, groupId, config);
        this.offsetManager.setCoordinator(this.coordinator);
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
        
        // 刷新元数据
        for (String topic : topics) {
            metadataManager.refreshMetadata(topic);
            topicPartitionLeaders.put(topic, metadataManager.getPartitionLeaders(topic));
        }
        
        // 初始化消费者组
        coordinator.initializeGroup(topics);
        // 新增：获取分区列表并拉取 group offset
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        for (String topic : topics) {
            Map<Integer, String> leaders = topicPartitionLeaders.get(topic);
            if (leaders != null) {
                topicPartitions.put(topic, new ArrayList<>(leaders.keySet()));
            }
        }
        offsetManager.fetchCommittedOffsets(topics, topicPartitions);
    }

    @Override
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }

        long startTime = System.currentTimeMillis();
        List<ConsumerRecord> allRecords = new ArrayList<>();
        System.out.println("[Poll] 开始拉取消息...");
        List<PartitionAssignment> assignments = coordinator.getAssignments();
        System.out.println("[Poll] 当前分区分配: " + assignments);
        try {
            if (assignments == null || assignments.isEmpty()) {
                System.out.println("[Poll] 当前无分区分配，返回空结果");
                return allRecords;
            }
            for (PartitionAssignment assignment : assignments) {
                if (allRecords.size() >= config.getMaxPollRecords()) {
                    break;
                }
                String topic = assignment.getTopic();
                int partition = assignment.getPartition();
                Map<Integer, String> partitionLeaders = topicPartitionLeaders.get(topic);
                if (partitionLeaders == null) continue;
                String broker = partitionLeaders.get(partition);
                if (broker == null) continue;
                String[] parts = broker.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                long offset = offsetManager.getOffset(topic, partition);
                int retryCount = 0;
                while (retryCount < config.getMaxRetries()) {
                    try {
                        System.out.printf("[Poll] 拉取参数: topic=%s, partition=%d, offset=%d, broker=%s:%d%n",
                            topic, partition, offset, host, port);

                        ByteBuffer fetchRequest = FetchRequestBuilder.build(
                            clientId,
                            topic,
                            partition,
                            offset,
                            config.getFetchMaxBytes(),
                            1
                        );
                        ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, fetchRequest);
                        List<ConsumerRecord> records = FetchResponseParser.parse(response);
                        if (!records.isEmpty()) {
                            long firstOffset = records.get(0).getOffset();
                            long lastOffset = records.get(records.size() - 1).getOffset();
                            System.out.printf("[Poll] 拉取到%d条消息, offset范围: [%d, %d]\n", records.size(), firstOffset, lastOffset);
                            offsetManager.updateOffset(topic, partition, lastOffset + 1);
                        } else {
                            System.out.printf("[Poll] topic=%s, partition=%d, fetched=0%n", topic, partition);
                        }
                        allRecords.addAll(records);
                        break;
                    } catch (Exception e) {
                        System.err.println("[Poll] 拉取异常: " + e.getMessage());
                        retryCount++;
                        if (retryCount >= config.getMaxRetries()) {
                            System.err.println("Failed to fetch from topic=" + topic + ", partition=" + partition + " after " + config.getMaxRetries() + " retries");
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
        } finally {
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_POLL);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_POLL, endTime - startTime);
            System.out.printf("[Poll] 本次总共拉取消息数: %d\n", allRecords.size());
            commitSync();
            System.out.println("[Poll] offset 提交完成");
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
            offsetManager.commitSync(coordinator.getGenerationId(), coordinator.getMemberId());
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
                
                // 关闭协调者
                coordinator.close();
            } finally {
                // 清理资源
                subscribedTopics = new ArrayList<>();
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