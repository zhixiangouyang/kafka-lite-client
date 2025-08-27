package org.example.kafkalite.client;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 官方Kafka客户端性能测试 - 精简版，移除内部监控
 */
public class KafkaProducerAutoCompressMultiThread {

    // 预生成消息模板
    private static String[] generateMessageTemplates(int count, int sizeBytes) {
        String[] templates = new String[count];
        Random random = new Random(); // 添加随机数生成器

        for (int i = 0; i < count; i++) {
            StringBuilder sb = new StringBuilder(sizeBytes);

            // 改为和第一个方法一样的随机生成逻辑
            while (sb.length() < sizeBytes) {
                // 使用数字和字母，避免特殊字符可能导致的问题
                char c = (char) (random.nextInt(26) + 'a');
                sb.append(c);
            }

            templates[i] = sb.toString();
        }
        return templates;
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "cluster-test-topic";
//        String topic = "java-test-topic";
//        String bootstrapServers = "10.251.183.199:27462";
        String bootstrapServers = "10.251.176.5:19092";
        int threadCount = 1;
        long testDurationMs = 1000 * 600; // 2分钟
        int messageSizeBytes = 1024;
        int maxQps = 200000;

        System.out.printf("测试配置: 线程数=%d, 测试时长=%ds, 消息大小=%d字节, 最大QPS=%d\n",
                threadCount, testDurationMs/1000, messageSizeBytes, maxQps);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 修正配置：移除幂等性，使用acks=1保持一致性
//        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.ACKS_CONFIG, "1");                          // 与自研客户端一致
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);             // 900KB
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);                       // 3ms
//        props.put(ProducerConfig.RETRIES_CONFIG, 3);
//        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);  // 串行发送

        // 不启用幂等性，避免acks冲突
        // props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);        // 注释掉

//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 268435456);           // 256MB
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        final String[] messageTemplates = generateMessageTemplates(10, messageSizeBytes);
        System.out.println("已生成消息模板");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        // 简化统计变量 - 只保留核心指标
        AtomicLong messageCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + testDurationMs;

        // 限流变量
        final AtomicLong lastRateCheckTime = new AtomicLong(startTime);
        final AtomicLong messagesSinceLastCheck = new AtomicLong(0);

        System.out.println("开始发送消息...");

        // 生产者线程
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    int localIndex = threadId * 1000000;
                    Random random = new Random();

                    while (System.currentTimeMillis() < endTime) {
                        // 限流逻辑
                        long now = System.currentTimeMillis();
                        long lastCheck = lastRateCheckTime.get();
                        long timeSinceLastCheck = now - lastCheck;

                        if (timeSinceLastCheck >= 100) {
                            double currentRate = (messagesSinceLastCheck.get() * 1000.0) / timeSinceLastCheck;
                            if (currentRate > maxQps) {
                                if (lastRateCheckTime.compareAndSet(lastCheck, now)) {
                                    messagesSinceLastCheck.set(0);
                                }
                                Thread.sleep(1);
                                continue;
                            }

                            if (lastRateCheckTime.compareAndSet(lastCheck, now)) {
                                messagesSinceLastCheck.set(0);
                            }
                        }

                        // 消息生成
                        String key = "key-" + threadId + "-" + localIndex;
                        String template = messageTemplates[random.nextInt(messageTemplates.length)];
                        String value = template.substring(0, Math.min(template.length(), messageSizeBytes));

                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                        // 发送消息
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                errorCount.incrementAndGet();
                            } else {
                                messageCount.incrementAndGet();
                                messagesSinceLastCheck.incrementAndGet();
                            }
                        });

                        localIndex++;

                        if ((localIndex % 1000) == 0 && System.currentTimeMillis() >= endTime) {
                            break;
                        }
                    }

                    System.out.println("线程 " + threadId + " 完成");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // 等待完成
        finishLatch.await();
        producer.flush();
        producer.close();
        executor.shutdown();

        // 最终统计
        long finalTime = System.currentTimeMillis();
        double totalSeconds = (finalTime - startTime) / 1000.0;
        long totalMessages = messageCount.get();
        long totalErrors = errorCount.get();

        System.out.println("\n===== 官方Kafka客户端测试结果 =====");
        System.out.printf("总时间: %.2f秒%n", totalSeconds);
        System.out.printf("总消息数: %d%n", totalMessages);
        System.out.printf("错误数: %d%n", totalErrors);
        System.out.printf("平均QPS: %.2f%n", totalMessages / totalSeconds);
        System.out.println("=====================================");
    }
}