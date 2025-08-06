package org.example.kafkalite.client;

import org.example.kafkalite.producer.HashPartitioner;
import org.example.kafkalite.producer.KafkaLiteProducer;
import org.example.kafkalite.producer.KafkaLiteProducerImpl;
import org.example.kafkalite.producer.ProducerRecord;
import org.example.kafkalite.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerTest {
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
        String broker = "10.251.183.199:27462";
//        String broker = "localhost:9092";
        if (args.length > 0) {
            broker = args[0];
        }

        // 2. 创建生产者配置
        ProducerConfig config = new ProducerConfig.Builder()
            .batchSize(3800)  // 增大批次大小到64KB，适应1KB消息
            .lingerMs(3)       // 1ms等待时间，提高吞吐量
            .maxRetries(3)
            .compressionType("gzip")
            .maxQueueSize(500000) // 增大队列大小
            .build();

        // 3. 创建生产者实例，选择分区策略
        KafkaLiteProducerImpl producer = new KafkaLiteProducerImpl(
                Arrays.asList(broker),
                new HashPartitioner(),
                config
        );

        // 测试持续时间（毫秒）
        final long testDurationMs;
        if (args.length > 1) {
            testDurationMs = Long.parseLong(args[1]);
        } else {
            testDurationMs = 180000; // 默认3分钟
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
                        long bytes = bytesSent.get();
                        double elapsedSeconds = (now - startTime) / 1000.0;
                        double totalQps = count / elapsedSeconds;
                        double mbps = (bytes / (1024.0 * 1024.0)) / elapsedSeconds; // MB/s
                        
                        // 计算最近5秒的QPS
                        double recentQps = (count - lastCount) / ((now - lastTime) / 1000.0);
                        lastCount = count;
                        lastTime = now;
                        
                        System.out.printf("时间: %.2f秒, 已发送: %d条消息(%.2fMB), 错误: %d条, 平均QPS: %.2f, 最近QPS: %.2f, 吞吐量: %.2fMB/s, 队列大小: %d, 生产者QPS: %.2f, P99延迟: %.2f ms%n", 
                            elapsedSeconds, 
                            count,
                            bytes / (1024.0 * 1024.0),
                            errors,
                            totalQps,
                            recentQps,
                            mbps,
                            producer.getQueueSize(),
                            producer.getProducerQPS(),
                            producer.getProducerP99Latency());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            monitorThread.setDaemon(true);
            monitorThread.start();

            // 4. 持续发送消息
            
            // 创建多个发送线程，提高生产速度
            int producerThreads = 1; // 使用4个线程并行生产消息
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
                            
                            // 动态控制发送速率，避免队列溢出
                            if (producer.getQueueSize() < config.getMaxQueueSize() * 0.8) {
                                // 从模板中随机选择一个消息，并添加唯一标识符
                                String messageTemplate = messageTemplates[random.nextInt(messageTemplates.length)];
                                String messageValue = String.format("%d:%s", localIndex, messageTemplate);
                                
                                ProducerRecord record = new ProducerRecord(
                                    "ouyangTest6",
                                    "key" + localIndex, 
                                    messageValue
                                );
                                
                                try {
                                    producer.send(record);
                                    messageCount.incrementAndGet();
                                    messagesSinceLastCheck.incrementAndGet();
                                    bytesSent.addAndGet(messageValue.length());
                                    localIndex++;
                                } catch (Exception e) {
                                    errorCount.incrementAndGet();
                                    System.err.printf("发送消息失败 [%d]: %s%n", localIndex, e.getMessage());
                                    // 如果是队列满了，等待一小段时间
                                    if (e.getMessage().contains("buffer is full")) {
                                        Thread.sleep(50); // 等待更长时间
                                    } else {
                                        Thread.sleep(10);
                                    }
                                }
                            } else {
                                // 队列接近满，等待一小段时间
                                Thread.sleep(5);
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
            System.out.println("测试时间结束，等待队列中的消息发送完成...");
            producer.flush();
            
            // 打印最终指标
            long endTime = System.currentTimeMillis();
            double totalSeconds = (endTime - startTime) / 1000.0;
            long totalMessages = messageCount.get();
            long totalErrors = errorCount.get();
            long totalBytes = bytesSent.get();
            double mbps = (totalBytes / (1024.0 * 1024.0)) / totalSeconds;
            
            System.out.println("\n测试结束:");
            System.out.printf("总时间: %.2f秒%n", totalSeconds);
            System.out.printf("总消息数: %d%n", totalMessages);
            System.out.printf("总数据量: %.2f MB%n", totalBytes / (1024.0 * 1024.0));
            System.out.printf("错误数: %d%n", totalErrors);
            System.out.printf("平均QPS: %.2f%n", totalMessages / totalSeconds);
            System.out.printf("平均吞吐量: %.2f MB/s%n", mbps);
            System.out.printf("生产者QPS: %.2f%n", producer.getProducerQPS());
            System.out.printf("生产者P99延迟: %.2f ms%n", producer.getProducerP99Latency());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("测试被中断: " + e.getMessage());
        } finally {
            // 5. 关闭生产者
            System.out.println("关闭生产者...");
            producer.close();
            System.out.println("生产者已关闭");
        }
    }
}
