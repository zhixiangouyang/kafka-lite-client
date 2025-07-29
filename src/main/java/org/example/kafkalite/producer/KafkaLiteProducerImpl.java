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
    private final ExecutorService senderThreadPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int batchSize;
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final ConcurrentMap<String, KafkaSocketClient.ConnectionPool> connectionPools = new ConcurrentHashMap<>();
    private final int senderThreads;

    public KafkaLiteProducerImpl(List<String> bootstrapServers, Partitioner partitioner, ProducerConfig config) {
        this.partitioner = partitioner;
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.batchSize = config.getBatchSize();
        this.lingerMs = config.getLingerMs();
        this.maxRetries = config.getMaxRetries();
        this.retryBackoffMs = config.getRetryBackoffMs();
        this.recordQueue = new LinkedBlockingQueue<>(config.getMaxQueueSize());
        
        // 使用多线程发送消息，提高并行度
        this.senderThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
        this.senderThreadPool = Executors.newFixedThreadPool(senderThreads);

        // 启动发送线程
        startSenderThreads();
    }

    private void startSenderThreads() {
        // 启动多个发送线程
        for (int i = 0; i < senderThreads; i++) {
            senderThreadPool.submit(() -> {
                List<ProducerRecord> batch = new ArrayList<>();
                while (!closed.get() || !recordQueue.isEmpty()) {
                    try {
                        // 尝试在lingerMs时间内收集尽可能多的消息
                        long batchStartTime = System.currentTimeMillis();
                        long remainingWaitTime = lingerMs;
                        
                        while (batch.size() < batchSize && remainingWaitTime > 0) {
                            ProducerRecord record = recordQueue.poll(remainingWaitTime, TimeUnit.MILLISECONDS);
                            if (record != null) {
                                batch.add(record);
                            } else {
                                break; // 没有更多消息了
                            }
                            
                            // 更新剩余等待时间
                            long now = System.currentTimeMillis();
                            remainingWaitTime = lingerMs - (now - batchStartTime);
                        }

                        if (!batch.isEmpty()) {
                            // 按照topic和partition分组，减少网络请求
                            Map<String, Map<Integer, List<ProducerRecord>>> topicPartitionBatches = new ConcurrentHashMap<>();
                            
                            for (ProducerRecord record : batch) {
                                String topic = record.getTopic();
                                // 确保元数据已刷新
                                if (!topicPartitionBatches.containsKey(topic)) {
                                    metadataManager.refreshMetadata(topic);
                                    topicPartitionBatches.put(topic, new ConcurrentHashMap<>());
                                }
                                
                                // 获取分区
                                Map<Integer, String> partitionToBroker = metadataManager.getPartitionLeaders(topic);
                                int partitionCount = partitionToBroker.size();
                                if (partitionCount == 0) {
                                    continue; // 跳过没有分区的主题
                                }
                                
                                int partition = partitioner.partition(topic, record.getKey(), partitionCount);
                                
                                // 按分区分组
                                topicPartitionBatches.get(topic)
                                    .computeIfAbsent(partition, k -> new ArrayList<>())
                                    .add(record);
                            }
                            
                            // 计算总批次数
                            int totalBatches = 0;
                            for (Map<Integer, List<ProducerRecord>> partitionMap : topicPartitionBatches.values()) {
                                totalBatches += partitionMap.size();
                            }
                            
                            // 如果没有有效批次，直接返回
                            if (totalBatches == 0) {
                                continue;
                            }
                            
                            // 并行发送每个分区的批次
                            CountDownLatch latch = new CountDownLatch(totalBatches);
                            for (Map.Entry<String, Map<Integer, List<ProducerRecord>>> topicEntry : topicPartitionBatches.entrySet()) {
                                String topic = topicEntry.getKey();
                                for (Map.Entry<Integer, List<ProducerRecord>> partitionEntry : topicEntry.getValue().entrySet()) {
                                    int partition = partitionEntry.getKey();
                                    List<ProducerRecord> partitionBatch = partitionEntry.getValue();
                                    
                                    // 提交到线程池中执行
                                    CompletableFuture.runAsync(() -> {
                                        try {
                                            sendPartitionBatch(topic, partition, partitionBatch);
                                        } catch (Exception e) {
                                            System.err.println("发送分区批次失败: " + e.getMessage());
                                        } finally {
                                            latch.countDown();
                                        }
                                    }, senderThreadPool);
                                }
                            }
                            
                            // 等待所有批次发送完成
                            latch.await(lingerMs * 3, TimeUnit.MILLISECONDS);
                            batch.clear();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }

    private void sendPartitionBatch(String topic, int partition, List<ProducerRecord> batch) {
        if (batch.isEmpty()) return;
        
        long startTime = System.currentTimeMillis();
        try {
            // 获取分区对应的broker
            Map<Integer, String> partitionToBroker = metadataManager.getPartitionLeaders(topic);
            String brokerAddress = partitionToBroker.get(partition);
            if (brokerAddress == null) {
                throw new RuntimeException("No leader broker found for topic = " + topic + ", partition = " + partition);
            }
            
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            // 获取或创建连接池
            KafkaSocketClient.ConnectionPool connectionPool = connectionPools.computeIfAbsent(
                brokerAddress, 
                k -> new KafkaSocketClient.ConnectionPool(host, port, 10) // 每个broker维护10个连接
            );
            
            // 构建批量消息
            ByteBuffer recordBatch = buildRecordBatch(batch);
            
            // 如果批量消息为空（可能因为大小限制被过滤），则直接返回
            if (recordBatch.remaining() == 0) {
                System.err.println("警告: 批量消息为空，可能是因为消息大小超过限制，跳过发送");
                return;
            }
            
            // 构造ProduceRequest
            ByteBuffer request = ProduceRequestBuilder.build(
                    "kafka-lite",
                    topic,
                    partition,
                    recordBatch,
                    (short) 1,  //acks
                    3000,
                    1
            );
            
            // 通过连接池发送
            ByteBuffer response = connectionPool.sendAndReceive(request);
            
            // 这里可以解析响应，处理错误等
            
        } catch (Exception e) {
            // 发生错误，尝试重试
            int retries = 0;
            boolean interrupted = false;
            while (retries < maxRetries && !interrupted) {
                try {
                    Thread.sleep(retryBackoffMs);
                    // 刷新元数据
                    metadataManager.refreshMetadata(topic);
                    // 重试发送
                    sendPartitionBatch(topic, partition, batch);
                    return; // 如果重试成功，直接返回
                } catch (InterruptedException ie) {
                    // 处理中断异常
                    Thread.currentThread().interrupt();
                    interrupted = true;
                } catch (Exception retryEx) {
                    retries++;
                    if (retries >= maxRetries) {
                        System.err.println("批量发送失败，已达到最大重试次数: " + retryEx.getMessage());
                    }
                }
            }
        } finally {
            long endTime = System.currentTimeMillis();
            // 记录每条消息的指标
            for (int i = 0; i < batch.size(); i++) {
                metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND);
                metricsCollector.recordLatency(MetricsCollector.METRIC_PRODUCER_SEND, (endTime - startTime) / batch.size());
            }
        }
    }
    
    private ByteBuffer buildRecordBatch(List<ProducerRecord> records) {
        // 使用批量编码功能
        return KafkaRecordEncoder.encodeBatchMessages(records);
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
        
        // 获取或创建连接池
        KafkaSocketClient.ConnectionPool connectionPool = connectionPools.computeIfAbsent(
            brokerAddress, 
            k -> new KafkaSocketClient.ConnectionPool(host, port, 10)
        );

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

        // 7. 通过连接池发送
        connectionPool.sendAndReceive(request);
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
                senderThreadPool.shutdown();

                try {
                    if (!senderThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        senderThreadPool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    senderThreadPool.shutdownNow();
                }
                
                // 关闭所有连接池
                for (KafkaSocketClient.ConnectionPool pool : connectionPools.values()) {
                    pool.close();
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
    
    // 获取当前队列大小
    public int getQueueSize() {
        return recordQueue.size();
    }
}
