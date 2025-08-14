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
    private final String compressionType;
    private final int poolSize;
    
    // 新增：分区级缓存机制
    private final ConcurrentMap<String, PartitionBatchCache> partitionCaches = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cacheFlushExecutor;
    
    // 分区批次缓存
    private static class PartitionBatchCache {
        private final String topicPartitionKey;
        private final List<ProducerRecord> cachedRecords = new ArrayList<>();
        private volatile long firstMessageTime = 0;
        private final Object lock = new Object();
        
        public PartitionBatchCache(String topicPartitionKey) {
            this.topicPartitionKey = topicPartitionKey;
        }
        
        // 添加消息到缓存，返回是否达到发送条件
        public List<ProducerRecord> addAndCheckFlush(List<ProducerRecord> newRecords, int batchSize, long lingerMs) {
            synchronized (lock) {
                if (cachedRecords.isEmpty()) {
                    firstMessageTime = System.currentTimeMillis();
                }
                
                cachedRecords.addAll(newRecords);
                
                // 检查是否需要立即发送
                boolean shouldFlush = cachedRecords.size() >= batchSize || 
                                    (System.currentTimeMillis() - firstMessageTime >= lingerMs);
                
                if (shouldFlush) {
                    List<ProducerRecord> toSend = new ArrayList<>(cachedRecords);
                    cachedRecords.clear();
                    firstMessageTime = 0;
                    return toSend;
                }
                
                return null; // 不需要发送
            }
        }
        
        // 强制刷新缓存
        public List<ProducerRecord> forceFlush() {
            synchronized (lock) {
                if (cachedRecords.isEmpty()) {
                    return null;
                }
                
                List<ProducerRecord> toSend = new ArrayList<>(cachedRecords);
                cachedRecords.clear();
                firstMessageTime = 0;
                return toSend;
            }
        }
        
        // 检查是否超时需要刷新
        public boolean shouldTimeoutFlush(long lingerMs) {
            synchronized (lock) {
                return !cachedRecords.isEmpty() && 
                       (System.currentTimeMillis() - firstMessageTime >= lingerMs);
            }
        }
    }

    public KafkaLiteProducerImpl(List<String> bootstrapServers, Partitioner partitioner, ProducerConfig config) {
        this.partitioner = partitioner;
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.batchSize = config.getBatchSize();
        this.lingerMs = config.getLingerMs();
        this.maxRetries = config.getMaxRetries();
        this.retryBackoffMs = config.getRetryBackoffMs();
        this.recordQueue = new LinkedBlockingQueue<>(config.getMaxQueueSize());
        this.compressionType = config.getCompressionType();
        this.poolSize = config.getConnectionPoolSize();
        
        // 使用更多线程发送消息，提高并行度
//        this.senderThreads = 18;
        this.senderThreads = Math.max(50, Runtime.getRuntime().availableProcessors() * 4);
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

        // 初始化缓存刷新线程池
        this.cacheFlushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "partition-cache-flush");
            t.setDaemon(true);
            return t;
        });
        
        // 启动定期刷新缓存的任务
        startCacheFlushTask();
        
        System.out.printf("初始化生产者: 发送线程数=%d, 批次大小=%d, 等待时间=%dms%n", 
            senderThreads, batchSize, lingerMs);
        
        // 预先初始化连接和元数据
        for (String broker : bootstrapServers) {
            try {
                String[] parts = broker.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                // 预先创建连接池
                KafkaSocketClient.ConnectionPool connectionPool = new KafkaSocketClient.ConnectionPool(host, port, this.poolSize);
                connectionPools.put(broker, connectionPool);
                
                System.out.printf("预先创建连接池: %s%n", broker);
            } catch (Exception e) {
                System.err.printf("预先创建连接池失败: %s, 错误: %s%n", broker, e.getMessage());
            }
        }
            
        // 启动发送线程
        startSenderThreads();
    }
    
    // 启动定期刷新缓存的任务
    private void startCacheFlushTask() {
        cacheFlushExecutor.scheduleWithFixedDelay(() -> {
            try {
                // 定期检查所有分区缓存，刷新超时的缓存
                for (Map.Entry<String, PartitionBatchCache> entry : partitionCaches.entrySet()) {
                    String topicPartitionKey = entry.getKey();
                    PartitionBatchCache cache = entry.getValue();
                    
                    if (cache.shouldTimeoutFlush(lingerMs)) {
                        List<ProducerRecord> toFlush = cache.forceFlush();
                        if (toFlush != null && !toFlush.isEmpty()) {
                            // 解析topic和partition
                            String[] parts = topicPartitionKey.split("-");
                            if (parts.length == 2) {
                                String topic = parts[0];
                                int partition = Integer.parseInt(parts[1]);
                                
                                // 异步发送，避免阻塞缓存刷新线程
                                senderThreadPool.submit(() -> {
                                    try {
                                        doSendPartitionBatch(topic, partition, toFlush);
                                    } catch (Exception e) {
                                        System.err.printf("缓存刷新发送失败: topic=%s, partition=%d, 错误=%s%n", 
                                            topic, partition, e.getMessage());
                                    }
                                });
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("缓存刷新任务异常: " + e.getMessage());
            }
        }, lingerMs, lingerMs / 2, TimeUnit.MILLISECONDS); // 每半个linger时间检查一次
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
                        int currentBatchSize = Math.max(10, batchSize);
                        
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
                                // 📊 指标埋点: 记录批次大小
                                metricsCollector.setGauge(MetricsCollector.METRIC_PRODUCER_BATCH_SIZE, batch.size());
                                
                            // 按照topic和partition分组，减少网络请求
                            Map<String, Map<Integer, List<ProducerRecord>>> topicPartitionBatches = new ConcurrentHashMap<>();
                            
                            for (ProducerRecord record : batch) {
                                String topic = record.getTopic();
                                // 确保元数据已刷新 - 生产者上下文
                                if (!topicPartitionBatches.containsKey(topic)) {
                                    metadataManager.refreshMetadata(topic, false, true);
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
        
        // 新增：使用分区级缓存优化批量发送
        String topicPartitionKey = topic + "-" + partition;
        PartitionBatchCache cache = partitionCaches.computeIfAbsent(topicPartitionKey, 
            k -> new PartitionBatchCache(k));
        
        // 尝试将消息添加到缓存，检查是否需要立即发送
        List<ProducerRecord> toSend = cache.addAndCheckFlush(batch, batchSize, lingerMs);
        
        if (toSend != null) {
            // 达到发送条件，立即发送
            doSendPartitionBatch(topic, partition, toSend);
        }
        // 如果toSend为null，说明消息已缓存，等待后续触发或超时刷新
    }
    
    // 实际执行分区批次发送的方法
    private void doSendPartitionBatch(String topic, int partition, List<ProducerRecord> batch) {
        if (batch.isEmpty()) return;
        
        long startTime = System.currentTimeMillis();
        // 📊 指标埋点: 记录分区批次发送尝试
        Map<String, String> labels = new java.util.HashMap<>();
        labels.put("topic", topic);
        labels.put("partition", String.valueOf(partition));
        metricsCollector.incrementCounter("producer.batch.send.attempt", labels);
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
                    k -> new KafkaSocketClient.ConnectionPool(host, port, this.poolSize)
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
                
                // 📊 指标埋点: 批次发送成功
                metricsCollector.incrementCounter("producer.batch.send.success", labels);
                for (int i = 0; i < batch.size(); i++) {
                    metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND_SUCCESS);
                }
                
            } catch (Exception e) {
                System.err.printf("错误: 发送消息失败: topic=%s, partition=%d, 错误: %s%n", 
                    topic, partition, e.getMessage());
                
                // 📊 指标埋点: 批次发送失败
                metricsCollector.incrementCounter("producer.batch.send.error", labels);
                for (int i = 0; i < batch.size(); i++) {
                    metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND_ERROR);
                }
                
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
                    // 刷新元数据 - 生产者重试，错误触发
                    metadataManager.refreshMetadata(topic, true, true);
                    System.out.printf("重试发送消息 (第%d次): topic=%s, partition=%d%n", 
                        retries + 1, topic, partition);
                    // 重试时直接发送，跳过缓存机制
                    doSendPartitionBatch(topic, partition, batch);
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
            long totalLatency = endTime - startTime;
            
            // 📊 指标埋点: 记录批次发送延迟
            metricsCollector.recordLatency("producer.batch.send.latency", totalLatency, labels);
            
            // 记录每条消息的平均延迟
            long avgLatency = batch.isEmpty() ? 0 : totalLatency / batch.size();
            for (int i = 0; i < batch.size(); i++) {
                metricsCollector.recordLatency(MetricsCollector.METRIC_PRODUCER_SEND, avgLatency);
            }
        }
    }
    
    private ByteBuffer buildRecordBatch(List<ProducerRecord> records) {
        // 使用优化版批量编码功能，适合处理1KB大小的消息
        return KafkaRecordEncoder.encodeBatchMessagesOptimized(records, compressionType);
    }

    private void doSend(ProducerRecord record) throws Exception {
        String topic = record.getTopic();
        String key = record.getKey();
        String value = record.getValue();

        // 1. 刷新元数据（若已存在可内部跳过）- 生产者上下文
        metadataManager.refreshMetadata(topic, false, true);

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
            k -> new KafkaSocketClient.ConnectionPool(host, port, this.poolSize)
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

        long startTime = System.currentTimeMillis();
        try {
            // 添加背压机制：如果队列已满超过90%，等待一段时间
            while (recordQueue.size() > recordQueue.remainingCapacity() * 9) {
                Thread.sleep(1);
            }
            
            // 使用超时版本的offer，避免无限等待
            if (!recordQueue.offer(record, lingerMs, TimeUnit.MILLISECONDS)) {
                System.err.println("警告: 发送缓冲区已满，消息被丢弃");
                // 📊 指标埋点: 队列满错误
                metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND_ERROR);
                throw new RuntimeException("Send buffer is full");
            }
            
            // 📊 指标埋点: 异步发送成功入队
            metricsCollector.incrementCounter("producer.send.queued");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 📊 指标埋点: 中断错误
            metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND_ERROR);
            throw new RuntimeException("Interrupted while adding record to queue", e);
        } finally {
            // 📊 指标埋点: 记录入队延迟
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordLatency("producer.send.queue_latency", latency);
        }
    }

    @Override
    public void sendSync(ProducerRecord record) throws Exception {
        if (closed.get()) {
            throw new IllegalStateException("Cannot send after the producer is closed");
        }

        long startTime = System.currentTimeMillis();
        try {
        // 直接调用现有的doSend方法进行同步发送
        doSend(record);
            
            // 📊 指标埋点: 同步发送成功
            metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND_SUCCESS);
            
        } catch (Exception e) {
            // 📊 指标埋点: 同步发送失败
            metricsCollector.incrementCounter(MetricsCollector.METRIC_PRODUCER_SEND_ERROR);
            throw e;
        } finally {
            // 📊 指标埋点: 记录同步发送总延迟
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordLatency(MetricsCollector.METRIC_PRODUCER_SEND, latency);
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
                // 新增：刷新所有分区缓存中的消息
                System.out.println("正在刷新分区缓存...");
                for (Map.Entry<String, PartitionBatchCache> entry : partitionCaches.entrySet()) {
                    String topicPartitionKey = entry.getKey();
                    PartitionBatchCache cache = entry.getValue();
                    
                    List<ProducerRecord> toFlush = cache.forceFlush();
                    if (toFlush != null && !toFlush.isEmpty()) {
                        // 解析topic和partition
                        String[] parts = topicPartitionKey.split("-");
                        if (parts.length == 2) {
                            String topic = parts[0];
                            int partition = Integer.parseInt(parts[1]);
                            
                            System.out.printf("刷新缓存: topic=%s, partition=%d, 消息数=%d%n", 
                                topic, partition, toFlush.size());
                            
                            try {
                                doSendPartitionBatch(topic, partition, toFlush);
                            } catch (Exception e) {
                                System.err.printf("刷新缓存失败: topic=%s, partition=%d, 错误=%s%n", 
                                    topic, partition, e.getMessage());
                            }
                        }
                    }
                }
                partitionCaches.clear();
                
                // 等待所有消息发送完成
                while (!recordQueue.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                // 关闭缓存刷新线程池
                if (cacheFlushExecutor != null) {
                    cacheFlushExecutor.shutdown();
                    try {
                        if (!cacheFlushExecutor.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                            cacheFlushExecutor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        cacheFlushExecutor.shutdownNow();
                    }
                }

                // 关闭发送线程池
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
    
    // 新增：扩展延迟指标
    public double getProducerP50Latency() {
        return metricsCollector.getP50Latency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
    
    public double getProducerP95Latency() {
        return metricsCollector.getP95Latency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
    
    public double getProducerP999Latency() {
        return metricsCollector.getP999Latency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
    
    public double getProducerAvgLatency() {
        return metricsCollector.getAverageLatency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
    
    public double getProducerMaxLatency() {
        return metricsCollector.getMaxLatency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
    
    public double getProducerMinLatency() {
        return metricsCollector.getMinLatency(MetricsCollector.METRIC_PRODUCER_SEND);
    }
    
    // 获取当前队列大小
    public int getQueueSize() {
        return recordQueue.size();
    }
}
