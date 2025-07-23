package org.example.kafkalite.producer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.monitor.MetricsCollector;
import org.example.kafkalite.protocol.KafkaRecordEncoder;
import org.example.kafkalite.protocol.ProduceRequestBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaLiteProducerImpl implements KafkaLiteProducer {
    private final Partitioner partitioner;
    private final MetadataManager metadataManager;
    private final MetricsCollector metricsCollector;
    private final BlockingQueue<ProducerRecord> recordQueue;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService senderThread;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int batchSize;
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;

    public KafkaLiteProducerImpl(List<String> bootstrapServers, Partitioner partitioner, ProducerConfig config) {
        this.partitioner = partitioner;
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.batchSize = config.getBatchSize();
        this.lingerMs = config.getLingerMs();
        this.maxRetries = config.getMaxRetries();
        this.retryBackoffMs = config.getRetryBackoffMs();
        this.recordQueue = new LinkedBlockingQueue<>(config.getMaxQueueSize());
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.senderThread = Executors.newSingleThreadExecutor();

        // 启动发送线程
        startSenderThread();
    }

    private void startSenderThread() {
        senderThread.submit(() -> {
            List<ProducerRecord> batch = new ArrayList<>();
            while (!closed.get() || !recordQueue.isEmpty()) {
                try {
                    ProducerRecord record = recordQueue.poll(lingerMs, TimeUnit.MILLISECONDS);
                    if (record != null) {
                        batch.add(record);
                    }

                    if (batch.size() >= batchSize || (record == null && !batch.isEmpty())) {
                        sendBatch(batch);
                        batch.clear();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    private void sendBatch(List<ProducerRecord> batch) {
        if (batch.isEmpty()) return;

        for (ProducerRecord record : batch) {
            long startTime = System.currentTimeMillis();
            try {
                sendWithRetry(record);
            } finally {
                long endTime = System.currentTimeMillis();
                metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND);
                metricsCollector.recordLatency(MetricsCollector.METRIC_PRODUCER_SEND, endTime - startTime);
            }
        }
    }

    private void sendWithRetry(ProducerRecord record) {
        int retries = 0;
        while (retries < maxRetries) {
            try {
                doSend(record);
                return;
            } catch (Exception e) {
                retries++;
                if (retries >= maxRetries) {
                    throw new RuntimeException("Failed to send record after " + maxRetries + " retries", e);
                }
                try {
                    Thread.sleep(retryBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying", ie);
                }
                // 刷新元数据，以防是由于leader变更导致的错误
                metadataManager.refreshMetadata(record.getTopic());
            }
        }
    }

    private void doSend(ProducerRecord record) throws Exception {
        String topic = record.getTopic();
        String key = record.getKey();
        String value = record.getValue();

        // 1. 刷新元数据（若已存在可内部跳过）
        metadataManager.refreshMetadata(topic);

        // 2. 获取partition -> broker 映射
        Map<Integer, String> partitionToBroker = metadataManager.getPartitionLeaders(topic);
        int partitionCount = partitionToBroker.size();

        if (partitionCount == 0) {
            throw new RuntimeException("No partition found for topic: " + topic);
        }

        // 3. 使用分区器决定发送到哪个分区
        int partition = partitioner.partition(topic, key, partitionCount);

        // 4. 获取 partition 对应的 broker
        String brokerAddress = partitionToBroker.get(partition);
        if (brokerAddress == null) {
            throw new RuntimeException("No leader broker found for topic = " + topic + ", partition = " + partition);
        }

        String[] parts = brokerAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        // 5. 构造 RecordBatch（Kafka协议标准格式）
        ByteBuffer recordBatch = KafkaRecordEncoder.encodeRecordBatch(key, value);

        // 6. 构造ProduceRequest（包含header + body）
        ByteBuffer request = ProduceRequestBuilder.build(
                "kafka-lite",
                topic,
                partition,
                recordBatch,
                (short) 1,  //acks
                3000,
                1
        );

        // 7. 通过socket发送
        KafkaSocketClient.sendAndReceive(host, port, request);
    }

    @Override
    public void send(ProducerRecord record) {
        if (closed.get()) {
            throw new IllegalStateException("Cannot send after the producer is closed");
        }

        try {
            if (!recordQueue.offer(record, lingerMs, TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("Send buffer is full");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while adding record to queue", e);
        }
    }

    @Override
    public void flush() {
        // 等待队列清空
        while (!recordQueue.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while flushing", e);
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                // 等待所有消息发送完成
                while (!recordQueue.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                // 关闭线程池
                scheduler.shutdown();
                senderThread.shutdown();

                try {
                    if (!scheduler.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        scheduler.shutdownNow();
                    }
                    if (!senderThread.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        senderThread.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    senderThread.shutdownNow();
                }
            } finally {
                recordQueue.clear();
            }
        }
    }

    // 获取生产者监控指标
    public double getProducerQPS() {
        return metricsCollector.getQPS(MetricsCollector.METRIC_PRODUCER_SEND);
    }

    public double getProducerP99Latency() {
        return metricsCollector.getP99Latency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
}
