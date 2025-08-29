package org.example.kafkalite.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 官方Kafka客户端生产者测试类 - 用于与自研客户端对比性能
 * 配置参数与KafkaProducerMonitorTest保持一致
 */
public class OfficialKafkaProducerMonitorTest {
    
    // 生成指定大小的随机消息
    private static String generateMessage(int sizeInBytes) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(sizeInBytes);
        
        // 生成随机字符，直到达到指定大小
        while (sb.length() < sizeInBytes) {
            // 使用数字和字母，避免特殊字符可能导致的问题
            char c = (char) (random.nextInt(26) + 'a');
            sb.append(c);
        }
        
        return sb.toString();
    }
    
    // 预生成一些消息模板，避免每次都生成新的随机消息
    private static String[] generateMessageTemplates(int count, int sizeInBytes) {
        String[] templates = new String[count];
        for (int i = 0; i < count; i++) {
            templates[i] = generateMessage(sizeInBytes);
        }
        return templates;
    }
    
    public static void main(String[] args) {
        // 1. 配置 broker 地址
        String broker = "10.251.176.5:19092"; // 默认使用您指定的broker
        
        if (args.length > 0) {
            broker = args[0];
        }

        // 2. 创建官方Kafka生产者配置 (映射自研客户端的配置)
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 对应自研客户端的配置
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 10);  // batchSize: 10KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);           // lingerMs: 1ms
        props.put(ProducerConfig.RETRIES_CONFIG, 3);             // maxRetries: 3
        props.put(ProducerConfig.ACKS_CONFIG, "all");            // acks: -1 (all)
        
        // 队列相关配置 (映射maxQueueSize)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 500000 * 1024); // 大约500MB缓冲区
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);             // 最多阻塞5秒
        
        // 性能优化配置
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");       // 无压缩，与自研一致
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        // 3. 创建官方Kafka生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 测试持续时间（毫秒）
        final long testDurationMs;
        if (args.length > 1) {
            testDurationMs = Long.parseLong(args[1]);
        } else {
            testDurationMs = 30 * 60000; // 默认3分钟
        }
        
        // 消息大小（字节）
        final int messageSizeBytes = 1024; // 1KB
        System.out.printf("消息大小: %d 字节%n", messageSizeBytes);
        
        // 预生成10个消息模板，减少CPU开销
        final String[] messageTemplates = generateMessageTemplates(10, messageSizeBytes);
        System.out.println("已生成消息模板");

        // 用于计算实时QPS的变量
        AtomicLong messageCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        AtomicLong bytesSent = new AtomicLong(0);
        AtomicLong successCount = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        
        // 限流相关参数
        final int maxQps = 200000; // 提高最大QPS限制
        final AtomicLong lastRateCheckTime = new AtomicLong(startTime);
        final AtomicLong messagesSinceLastCheck = new AtomicLong(0);

        try {
            // 创建QPS监控线程
            Thread monitorThread = new Thread(() -> {
                try {
                    long lastCount = 0;
                    long lastTime = System.currentTimeMillis();
                    
                    while (System.currentTimeMillis() - startTime < testDurationMs) {
                        Thread.sleep(5000); // 每5秒报告一次
                        long now = System.currentTimeMillis();
                        long count = messageCount.get();
                        long errors = errorCount.get();
                        long success = successCount.get();
                        long bytes = bytesSent.get();
                        double elapsedSeconds = (now - startTime) / 1000.0;
                        double totalQps = count / elapsedSeconds;
                        double mbps = (bytes / (1024.0 * 1024.0)) / elapsedSeconds; // MB/s
                        
                        // 计算最近5秒的QPS
                        double recentQps = (count - lastCount) / ((now - lastTime) / 1000.0);
                        lastCount = count;
                        lastTime = now;
                        
                        System.out.printf("[官方客户端 %.1fs] 时间: %.2f秒, 已发送: %d条消息(%.2fMB), 成功: %d条, 错误: %d条, 平均QPS: %.2f, 最近QPS: %.2f, 吞吐量: %.2fMB/s%n", 
                            elapsedSeconds,
                            elapsedSeconds, 
                            count,
                            bytes / (1024.0 * 1024.0),
                            success,
                            errors,
                            totalQps,
                            recentQps,
                            mbps);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            monitorThread.setDaemon(true);
            monitorThread.start();

            System.out.println("开始官方Kafka客户端性能测试...");
            System.out.printf("📍 目标Broker: %s\n", broker);
            System.out.printf("⏱️ 测试时长: %.1f分钟\n", testDurationMs / 60000.0);
            System.out.println("🔄 对比自研客户端，观察QPS变化趋势");
            System.out.println("================================================================================");

            // 4. 持续发送消息 (与KafkaProducerMonitorTest相同的逻辑)
            
            // 创建多个发送线程，提高生产速度
            int producerThreads = 1; // 使用1个线程并行生产消息
            Thread[] producerThreadsArray = new Thread[producerThreads];
            
            for (int t = 0; t < producerThreads; t++) {
                final int threadId = t;
                producerThreadsArray[t] = new Thread(() -> {
                    int localIndex = threadId * 1000000; // 每个线程使用不同的起始索引
                    Random random = new Random();
                    
                    try {
                        while (System.currentTimeMillis() - startTime < testDurationMs) {
                            // 实现限流: 检查发送速率是否超过限制
                            long now = System.currentTimeMillis();
                            long lastCheck = lastRateCheckTime.get();
                            long timeSinceLastCheck = now - lastCheck;
                            
                            if (timeSinceLastCheck >= 100) { // 每100ms检查一次
                                double currentRate = (messagesSinceLastCheck.get() * 1000.0) / timeSinceLastCheck;
                                if (currentRate > maxQps) {
                                    // 发送速率过高，等待一小段时间
                                    Thread.sleep(1);
                                }
                                
                                // 重置计数器
                                if (lastRateCheckTime.compareAndSet(lastCheck, now)) {
                                    messagesSinceLastCheck.set(0);
                                }
                            }
                            
                            // 从模板中随机选择一个消息，并添加唯一标识符
                            String messageTemplate = messageTemplates[random.nextInt(messageTemplates.length)];
                            String messageValue = String.format("%d:%s", localIndex, messageTemplate);
                            
                            ProducerRecord<String, String> record = new ProducerRecord<>(
                                "performance-test-topic-3", // 使用您指定的topic
                                "key" + localIndex,
                                messageValue
                            );
                            
                            try {
                                final int currentIndex = localIndex; // 创建final副本供lambda使用
                                // 异步发送，带回调
                                producer.send(record, new Callback() {
                                    @Override
                                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                                        if (exception == null) {
                                            // 发送成功
                                            successCount.incrementAndGet();
                                        } else {
                                            // 发送失败
                                            errorCount.incrementAndGet();
                                            System.err.printf("发送消息失败 [%d]: %s%n", currentIndex, exception.getMessage());
                                        }
                                    }
                                });
                                
                                messageCount.incrementAndGet();
                                messagesSinceLastCheck.incrementAndGet();
                                bytesSent.addAndGet(messageValue.length());
                                localIndex++;
                                
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                System.err.printf("发送消息异常 [%d]: %s%n", localIndex, e.getMessage());
                                
                                // 如果是缓冲区满了，等待一小段时间
                                if (e.getMessage().contains("buffer") || e.getMessage().contains("memory")) {
                                    Thread.sleep(50); // 等待更长时间
                                } else {
                                    Thread.sleep(10);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                producerThreadsArray[t].start();
            }
            
            // 等待所有生产者线程完成
            for (Thread t : producerThreadsArray) {
                t.join();
            }
            
            // 等待消息发送完成
            System.out.println("测试时间结束，等待缓冲区中的消息发送完成...");
            
            // 刷新并等待所有消息发送完成
            producer.flush();
            
            // 最终统计
            long finalTime = System.currentTimeMillis();
            double totalSeconds = (finalTime - startTime) / 1000.0;
            long finalCount = messageCount.get();
            long finalSuccess = successCount.get();
            long finalErrors = errorCount.get();
            long finalBytes = bytesSent.get();
            double finalQps = finalCount / totalSeconds;
            double finalMbps = (finalBytes / (1024.0 * 1024.0)) / totalSeconds;

            System.out.println("================================================================================");
            System.out.println("🏁 官方Kafka客户端测试完成!");
            System.out.printf("📊 总计发送: %d 条消息\n", finalCount);
            System.out.printf("✅ 成功发送: %d 条消息\n", finalSuccess);
            System.out.printf("❌ 失败数量: %d 条消息\n", finalErrors);
            System.out.printf("📈 成功率: %.2f%%\n", finalSuccess * 100.0 / Math.max(finalCount, 1));
            System.out.printf("⏱️ 总耗时: %.2f 秒\n", totalSeconds);
            System.out.printf("🚀 平均QPS: %.2f\n", finalQps);
            System.out.printf("📦 数据量: %.2f MB (%.2f MB/s)\n", finalBytes / (1024.0 * 1024.0), finalMbps);
            System.out.println("================================================================================");
            System.out.println("💡 对比要点:");
            System.out.println("1. 观察QPS是否保持稳定，有无下降趋势");
            System.out.println("2. 比较成功率和错误率");
            System.out.println("3. 观察内存使用情况和GC频率");
            System.out.println("4. 检查CPU使用率");
            System.out.println("================================================================================");

        } catch (Exception e) {
            System.err.println("测试过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭生产者
            producer.close();
            System.out.println("官方Kafka生产者已关闭");
        }
    }
}

/**
 * 运行说明:
 * 
 * 1. 需要添加官方Kafka客户端依赖到pom.xml:
 *    <dependency>
 *        <groupId>org.apache.kafka</groupId>
 *        <artifactId>kafka-clients</artifactId>
 *        <version>3.5.0</version>
 *    </dependency>
 * 
 * 2. 编译运行:
 *    mvn compile
 *    java -cp target/classes:$HOME/.m2/repository/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar:... \
 *         org.example.kafkalite.client.OfficialKafkaProducerMonitorTest
 * 
 * 3. 指定参数:
 *    java ... OfficialKafkaProducerMonitorTest "10.251.176.5:19092" 180000
 * 
 * 4. 对比观察:
 *    - 同时运行两个测试，观察QPS变化趋势
 *    - 自研客户端QPS下降，官方客户端保持稳定 → 说明自研客户端有问题
 *    - 两者都下降 → 可能是Broker或网络问题
 *    - 观察内存使用和GC情况
 */ 