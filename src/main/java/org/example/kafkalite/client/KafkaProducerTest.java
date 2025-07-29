package org.example.kafkalite.client;

import org.example.kafkalite.producer.HashPartitioner;
import org.example.kafkalite.producer.KafkaLiteProducer;
import org.example.kafkalite.producer.KafkaLiteProducerImpl;
import org.example.kafkalite.producer.ProducerRecord;
import org.example.kafkalite.producer.ProducerConfig;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerTest {
    public static void main(String[] args) {
        // 1. 配置 broker 地址
        String broker = "10.251.183.199:27462";
        if (args.length > 0) {
            broker = args[0];
        }

        // 2. 创建生产者配置
        ProducerConfig config = new ProducerConfig.Builder()
            .batchSize(16384)
            .lingerMs(1) // 减少等待时间，提高吞吐量
            .maxRetries(3)
            .maxQueueSize(100000) // 增大队列大小
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
            testDurationMs = 120000; // 默认2分钟
        }

        // 用于计算实时QPS的变量
        AtomicLong messageCount = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        long lastReportTime = startTime;

        try {
            // 创建QPS监控线程
            Thread monitorThread = new Thread(() -> {
                try {
                    while (System.currentTimeMillis() - startTime < testDurationMs) {
                        Thread.sleep(5000); // 每5秒报告一次
                        long now = System.currentTimeMillis();
                        long count = messageCount.get();
                        double elapsedSeconds = (now - startTime) / 1000.0;
                        double qps = count / elapsedSeconds;
                        
                        System.out.printf("时间: %.2f秒, 已发送: %d条消息, 平均QPS: %.2f, 当前QPS: %.2f, 生产者QPS: %.2f, P99延迟: %.2f ms%n", 
                            elapsedSeconds, 
                            count,
                            qps,
                            producer.getProducerQPS(),
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
            int messageIndex = 0;
            while (System.currentTimeMillis() - startTime < testDurationMs) {
                // 动态控制发送速率，避免队列溢出
                if (producer.getQueueSize() < config.getMaxQueueSize() * 0.9) {
                    ProducerRecord record = new ProducerRecord(
                        "ouyangTest",
                        "key" + messageIndex, 
                        "hello kafka-lite " + messageIndex
                    );
                    
                    try {
                        producer.send(record);
                        messageCount.incrementAndGet();
                        messageIndex++;
                    } catch (Exception e) {
                        System.err.println("发送消息失败: " + e.getMessage());
                        // 如果是队列满了，等待一小段时间
                        if (e.getMessage().contains("buffer is full")) {
                            Thread.sleep(10);
                        }
                    }
                } else {
                    // 队列接近满，等待一小段时间
                    Thread.sleep(1);
                }
            }
            
            // 等待消息发送完成
            producer.flush();
            
            // 打印最终指标
            long endTime = System.currentTimeMillis();
            double totalSeconds = (endTime - startTime) / 1000.0;
            long totalMessages = messageCount.get();
            
            System.out.println("\n测试结束:");
            System.out.printf("总时间: %.2f秒%n", totalSeconds);
            System.out.printf("总消息数: %d%n", totalMessages);
            System.out.printf("平均QPS: %.2f%n", totalMessages / totalSeconds);
            System.out.printf("生产者QPS: %.2f%n", producer.getProducerQPS());
            System.out.printf("生产者P99延迟: %.2f ms%n", producer.getProducerP99Latency());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("测试被中断: " + e.getMessage());
        } finally {
            // 5. 关闭生产者
            producer.close();
        }
    }
}
