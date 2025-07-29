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
import java.util.concurrent.atomic.AtomicLong;

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
        
        // 使用更多线程发送消息，提高并行度
        this.senderThreads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        this.senderThreadPool = Executors.newFixedThreadPool(senderThreads, 
            new ThreadFactory() {
                private final AtomicLong threadCounter = new AtomicLong(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "kafka-sender-" + threadCounter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });

        System.out.printf("初始化生产者: 发送线程数=%d, 批次大小=%d, 等待时间=%dms%n", 
            senderThreads, batchSize, lingerMs);
        
        // 预先初始化连接和元数据
        for (String broker : bootstrapServers) {
            try {
                String[] parts = broker.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                // 预先创建连接池
                KafkaSocketClient.ConnectionPool connectionPool = new KafkaSocketClient.ConnectionPool(host, port, 10);
                connectionPools.put(broker, connectionPool);
                
                System.out.printf("预先创建连接池: %s%n", broker);
            } catch (Exception e) {
                System.err.printf("预先创建连接池失败: %s, 错误: %s%n", broker, e.getMessage());
            }
        }
            
        // 启动发送线程
        startSenderThreads();
    }

    private void startSenderThreads() {
        // 启动多个发送线程
        for (int i = 0; i < senderThreads; i++) {
            final int threadId = i;
            senderThreadPool.submit(() -> {
                System.out.printf("发送线程 %d 已启动%n", threadId);
                List<ProducerRecord> batch = new ArrayList<>();
                while (!closed.get() || !recordQueue.isEmpty()) {
                    try {
                        // 尝试在lingerMs时间内收集尽可能多的消息
                        long batchStartTime = System.currentTimeMillis();
                        long remainingWaitTime = lingerMs;
                        
                        // 降低批处理阈值，更快触发发送
                        int currentBatchSize = Math.max(10, batchSize / 4);
                        
                        while (batch.size() < currentBatchSize && remainingWaitTime > 0) {
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
                                    System.err.printf("警告: topic=%s 没有可用分区%n", topic);
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
                            List<Future<?>> futures = new ArrayList<>(totalBatches);
                            
                            for (Map.Entry<String, Map<Integer, List<ProducerRecord>>> topicEntry : topicPartitionBatches.entrySet()) {
                                String topic = topicEntry.getKey();
                                for (Map.Entry<Integer, List<ProducerRecord>> partitionEntry : topicEntry.getValue().entrySet()) {
                                    int partition = partitionEntry.getKey();
                                    List<ProducerRecord> partitionBatch = partitionEntry.getValue();
                                    
                                    // 直接发送，不使用CompletableFuture，避免线程池饥饿
                                    try {
                                        sendPartitionBatch(topic, partition, partitionBatch);
                                    } catch (Exception e) {
                                        System.err.println("发送分区批次失败: " + e.getMessage());
                                    } finally {
                                        latch.countDown();
                                    }
                                }
                            }
                            
                            // 等待所有批次发送完成，或者超时
                            boolean completed = latch.await(lingerMs * 5, TimeUnit.MILLISECONDS);
                            if (!completed) {
                                System.err.println("警告: 部分批次发送超时，取消剩余任务");
                                // 取消未完成的任务
                                for (Future<?> future : futures) {
                                    if (!future.isDone()) {
                                        future.cancel(true);
                                    }
                                }
                            }
                            
                            batch.clear();
                        } else {
                            // 没有消息可发送，短暂等待
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.printf("发送线程 %d 被中断%n", threadId);
                        break;
                    } catch (Exception e) {
                        System.err.printf("发送线程 %d 发生错误: %s%n", threadId, e.getMessage());
                        // 继续运行，不要因为一个错误就退出线程
                    }
                }
                System.out.printf("发送线程 %d 已退出%n", threadId);
            });
        }
    }

    private void sendPartitionBatch(String topic, int partition, List<ProducerRecord> batch) {
        if (batch.isEmpty()) return;
        
        // 移除小批量优化，确保所有消息都能及时发送
        
        long startTime = System.currentTimeMillis();
        try {
            // 获取分区对应的broker
            Map<Integer, String> partitionToBroker = metadataManager.getPartitionLeaders(topic);
            String brokerAddress = partitionToBroker.get(partition);
            if (brokerAddress == null) {
                System.err.printf("错误: 找不到topic=%s, partition=%d的leader broker%n", topic, partition);
                throw new RuntimeException("No leader broker found for topic = " + topic + ", partition = " + partition);
            }
            
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            // 获取或创建连接池
            KafkaSocketClient.ConnectionPool connectionPool;
            try {
                connectionPool = connectionPools.computeIfAbsent(
                    brokerAddress, 
                    k -> new KafkaSocketClient.ConnectionPool(host, port, 10) // 每个broker维护10个连接
                );
            } catch (Exception e) {
                System.err.printf("错误: 创建连接池失败: %s:%d, 错误: %s%n", host, port, e.getMessage());
                throw e;
            }
            
            // 构建批量消息
            ByteBuffer recordBatch;
            try {
                recordBatch = buildRecordBatch(batch);
            } catch (Exception e) {
                System.err.printf("错误: 构建批量消息失败: %s%n", e.getMessage());
                throw e;
            }
            
            // 如果批量消息为空（可能因为大小限制被过滤），则直接返回
            if (recordBatch.remaining() == 0) {
                System.err.println("警告: 批量消息为空，可能是因为消息大小超过限制，跳过发送");
                return;
            }
            
            // 构造ProduceRequest
            ByteBuffer request;
            try {
                request = ProduceRequestBuilder.build(
                        "kafka-lite",
                        topic,
                        partition,
                        recordBatch,
                        (short) 1,  //acks
                        3000,
                        1
                );
            } catch (Exception e) {
                System.err.printf("错误: 构建ProduceRequest失败: %s%n", e.getMessage());
                throw e;
            }
            
            // 通过连接池发送
            ByteBuffer response;
            try {
                response = connectionPool.sendAndReceive(request);
                System.out.printf("成功发送 %d 条消息到 topic=%s, partition=%d%n", 
                    batch.size(), topic, partition);
            } catch (Exception e) {
                System.err.printf("错误: 发送消息失败: topic=%s, partition=%d, 错误: %s%n", 
                    topic, partition, e.getMessage());
                throw e;
            }
            
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
                    System.out.printf("重试发送消息 (第%d次): topic=%s, partition=%d%n", 
                        retries + 1, topic, partition);
                    // 重试发送
                    sendPartitionBatch(topic, partition, batch);
                    return; // 如果重试成功，直接返回
                } catch (InterruptedException ie) {
                    // 处理中断异常
                    Thread.currentThread().interrupt();
                    interrupted = true;
                    System.err.println("发送线程被中断");
                } catch (Exception retryEx) {
                    retries++;
                    System.err.printf("重试失败 (第%d次): topic=%s, partition=%d, 错误: %s%n", 
                        retries, topic, partition, retryEx.getMessage());
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
            // 添加背压机制：如果队列已满超过90%，等待一段时间
            while (recordQueue.size() > recordQueue.remainingCapacity() * 9) {
                Thread.sleep(1);
            }
            
            // 使用超时版本的offer，避免无限等待
            if (!recordQueue.offer(record, lingerMs, TimeUnit.MILLISECONDS)) {
                System.err.println("警告: 发送缓冲区已满，消息被丢弃");
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
