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
    // 移除 scheduler 相关自动提交线程实现
    // private final ScheduledExecutorService scheduler;

    public KafkaLiteConsumerImpl(String groupId, List<String> bootstrapServers, ConsumerConfig config) {
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
        this.config = config;
        this.clientId = "kafka-lite-" + UUID.randomUUID().toString().substring(0, 8);
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
        this.offsetManager = new OffsetManager(groupId, bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.coordinator = new ConsumerCoordinator(clientId, groupId, config, bootstrapServers);
        this.offsetManager.setCoordinator(this.coordinator);
        // 新增：注入coordinatorSocket
        this.offsetManager.setCoordinatorSocket(this.coordinator.coordinatorSocket);
        // 移除 scheduler 相关自动提交线程实现
        // this.scheduler = Executors.newScheduledThreadPool(1);
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
        // 移除 subscribe 中自动提交线程启动逻辑
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
                            System.out.printf("[DEBUG] poll调用updateOffset: topic=%s, partition=%d, offset=%d\n", topic, partition, lastOffset+1);
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
                            // 重试失败后抛出异常，而不是静默失败
                            throw new RuntimeException("Failed to fetch from topic=" + topic + ", partition=" + partition + " after " + config.getMaxRetries() + " retries", e);
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
        } catch (Exception e) {
            System.err.println("[Poll] 拉取过程中发生异常: " + e.getMessage());
            e.printStackTrace();
            // 不要重新抛出异常，而是返回空结果，让消费者继续运行
        } finally {
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_POLL);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_POLL, endTime - startTime);
            System.out.printf("[Poll] 本次总共拉取消息数: %d\n", allRecords.size());
            System.out.printf("[DEBUG] poll finally, thread=%s, enableAutoCommit=%s\n", Thread.currentThread().getName(), config.isEnableAutoCommit());
            if (config.isEnableAutoCommit()) {
                System.out.println("[DEBUG] poll finally自动提交offset");
                commitSync();
            } else {
                System.out.println("[DEBUG] poll finally不自动提交offset，需要手动调用commitSync");
            }
        }
        return allRecords;
    }

    @Override
    public void commitSync() {
        System.out.printf("[DEBUG] commitSync入口, thread=%s\n", Thread.currentThread().getName());
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
        System.out.printf("[DEBUG] commitAsync入口, thread=%s\n", Thread.currentThread().getName());
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
                // 移除 scheduler 相关自动提交线程关闭逻辑
                // 在 close 方法中无需 shutdown scheduler

                // 最后一次提交offset
                if (config.isEnableAutoCommit()) {
                    commitSync();
                }
                
                // 关闭协调者
                coordinator.close();
                offsetManager.close();
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