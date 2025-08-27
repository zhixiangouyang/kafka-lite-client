package org.example.kafkalite.client;

import org.example.kafkalite.monitor.MetricsCollector;
import org.example.kafkalite.monitor.PrometheusMetricsServer;
import org.example.kafkalite.producer.HashPartitioner;
import org.example.kafkalite.producer.KafkaLiteProducerImpl;
import org.example.kafkalite.producer.ProducerRecord;
import org.example.kafkalite.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带Prometheus监控的Kafka生产者测试类
 * 基于KafkaProducerTest，增加了可视化监控功能
 */
public class KafkaProducerMonitorTest {
    private static MetricsCollector metricsCollector;
    private static PrometheusMetricsServer metricsServer;
    
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

        // 初始化监控
        initializeMonitoring();

        // 2. 创建生产者配置 (与原KafkaProducerTest相同)
        ProducerConfig config = new ProducerConfig.Builder()
            .batchSize(1024 * 10)  // 增大批次大小到64KB，适应1KB消息
            .lingerMs(1)       // 1ms等待时间，提高吞吐量
            .maxRetries(3)
            .acks((short) -1)
//            .compressionType("gzip")
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
            testDurationMs = 600000; // 默认3分钟
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
            // 创建QPS监控线程 (增强版，包含Prometheus指标)
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
                        
                        System.out.printf("[%.1fs] 时间: %.2f秒, 已发送: %d条消息(%.2fMB), 错误: %d条, 平均QPS: %.2f, 最近QPS: %.2f, 吞吐量: %.2fMB/s, 队列大小: %d%n", 
                            elapsedSeconds,
                            elapsedSeconds, 
                            count,
                            bytes / (1024.0 * 1024.0),
                            errors,
                            totalQps,
                            recentQps,
                            mbps,
                            producer.getQueueSize());
                        
                        // 扩展延迟分布监控
                        System.out.printf("    📈 延迟分布: P50=%.1fms | P95=%.1fms | P99=%.1fms | P99.9=%.1fms | 平均=%.1fms | 最大=%.1fms%n",
                            producer.getProducerP50Latency(),
                            producer.getProducerP95Latency(), 
                            producer.getProducerP99Latency(),
                            producer.getProducerP999Latency(),
                            producer.getProducerAvgLatency(),
                            producer.getProducerMaxLatency());
                        
                        // 更新Prometheus指标 (包含扩展延迟指标)
                        updatePrometheusMetrics(count, errors, bytes, totalQps, recentQps, mbps, producer);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            monitorThread.setDaemon(true);
            monitorThread.start();

            System.out.println("开始生产者性能测试...");
            System.out.printf("📍 目标Broker: %s\n", broker);
            System.out.printf(" 测试时长: %.1f分钟\n", testDurationMs / 60000.0);
            System.out.printf("监控端点: http://localhost:8084/metrics\n");
            System.out.printf("💚 健康检查: http://localhost:8084/health\n");
            System.out.println("================================================================================");

            // 4. 持续发送消息 (与原KafkaProducerTest完全相同的逻辑)
            
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
                            
                            // 动态控制发送速率，避免队列溢出
                            if (producer.getQueueSize() < config.getMaxQueueSize() * 0.8) {
                                // 从模板中随机选择一个消息，并添加唯一标识符
                                String messageTemplate = messageTemplates[random.nextInt(messageTemplates.length)];
                                String messageValue = String.format("%d:%s", localIndex, messageTemplate);
                                
                                ProducerRecord record = new ProducerRecord(
                                    "performance-test-topic-3", // 使用您指定的topic
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
            
            // 等待队列清空或最多等待30秒
            long waitStart = System.currentTimeMillis();
            while (producer.getQueueSize() > 0 && (System.currentTimeMillis() - waitStart) < 30000) {
                Thread.sleep(1000);
                System.out.printf("等待队列清空... 剩余: %d 条消息%n", producer.getQueueSize());
            }
            
            // 最终统计
            long finalTime = System.currentTimeMillis();
            double totalSeconds = (finalTime - startTime) / 1000.0;
            long finalCount = messageCount.get();
            long finalErrors = errorCount.get();
            long finalBytes = bytesSent.get();
            double finalQps = finalCount / totalSeconds;
            double finalMbps = (finalBytes / (1024.0 * 1024.0)) / totalSeconds;
            double errorRate = finalCount > 0 ? (finalErrors * 100.0) / finalCount : 0;
            
            System.out.println("\n================================================================================");
            System.out.println("🏁 最终测试结果:");
            System.out.println("================================================================================");
            System.out.printf(" 总测试时间: %.2f秒\n", totalSeconds);
            System.out.printf("📈 消息统计: 总发送=%d, 错误=%d, 成功率=%.3f%%\n", finalCount, finalErrors, 100.0 - errorRate);
            System.out.printf("平均QPS: %.2f msg/s\n", finalQps);
            System.out.printf("📦 平均吞吐量: %.2f MB/s (总计: %.2f MB)\n", finalMbps, finalBytes / (1024.0 * 1024.0));
            System.out.printf(" 错误率: %.3f%%\n", errorRate);
            System.out.printf(" 最终P99延迟: %.2f ms\n", producer.getProducerP99Latency());
            
            // 更新最终指标
            updateFinalPrometheusMetrics(finalCount, finalErrors, finalBytes, finalQps, finalMbps, errorRate);
            
            System.out.println("================================================================================");
            System.out.println("监控数据已保存到Prometheus，可通过以下方式查看:");
            System.out.println("  • 指标端点: http://localhost:8084/metrics");
            System.out.println("  • 建议配置Grafana进行可视化展示");
            System.out.println("================================================================================");

        } catch (Exception e) {
            System.err.printf("测试过程中发生错误: %s%n", e.getMessage());
            e.printStackTrace();
        } finally {
            // 5. 关闭生产者
            producer.close();
            System.out.println("生产者已关闭");
            
            // 保持监控服务器运行一段时间，方便查看最终数据
            System.out.println("监控服务器将继续运行60秒，方便查看最终数据...");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // 关闭监控服务器
            if (metricsServer != null && metricsServer.isRunning()) {
                metricsServer.stop();
                System.out.println("监控服务器已关闭");
            }
        }
    }
    
    /**
     * 初始化监控系统
     */
    private static void initializeMonitoring() {
        try {
            // 创建指标收集器
            metricsCollector = new MetricsCollector("kafka-producer-test", "test-instance-001");
            
            // 创建并启动Prometheus服务器 (使用8084端口避免冲突)
            metricsServer = PrometheusMetricsServer.create(metricsCollector, 8084);
            metricsServer.start();
            
            System.out.println("Prometheus监控服务器已启动: http://localhost:8084/metrics");
            
        } catch (Exception e) {
            System.err.printf("监控系统初始化失败: %s\n", e.getMessage());
            System.err.println("测试将继续，但没有监控功能");
        }
    }
    
    /**
     * 更新Prometheus指标 (扩展版)
     */
    private static void updatePrometheusMetrics(long messageCount, long errorCount, long bytesSent,
                                              double totalQps, double recentQps, double mbps,
                                              KafkaLiteProducerImpl producer) {
        if (metricsCollector == null) return;
        
        try {
            // 更新计数器指标
            metricsCollector.setGauge("test.messages.sent.total", messageCount);
            metricsCollector.setGauge("test.messages.error.total", errorCount);
            metricsCollector.setGauge("test.bytes.sent.total", bytesSent);
            
            // 更新性能指标
            metricsCollector.setGauge("test.qps.average", totalQps);
            metricsCollector.setGauge("test.qps.recent", recentQps);
            metricsCollector.setGauge("test.throughput.mbps", mbps);
            metricsCollector.setGauge("test.queue.size", producer.getQueueSize());
            metricsCollector.setGauge("test.producer.qps", producer.getProducerQPS());
            
            // 扩展延迟指标
            metricsCollector.setGauge("test.producer.p50.latency", producer.getProducerP50Latency());
            metricsCollector.setGauge("test.producer.p95.latency", producer.getProducerP95Latency());
            metricsCollector.setGauge("test.producer.p99.latency", producer.getProducerP99Latency());
            metricsCollector.setGauge("test.producer.p999.latency", producer.getProducerP999Latency());
            metricsCollector.setGauge("test.producer.avg.latency", producer.getProducerAvgLatency());
            metricsCollector.setGauge("test.producer.max.latency", producer.getProducerMaxLatency());
            metricsCollector.setGauge("test.producer.min.latency", producer.getProducerMinLatency());
            
            // 计算派生指标
            double errorRate = messageCount > 0 ? (errorCount * 100.0) / messageCount : 0;
            double successRate = 100.0 - errorRate;
            metricsCollector.setGauge("test.error.rate.percent", errorRate);
            metricsCollector.setGauge("test.success.rate.percent", successRate);
            
        } catch (Exception e) {
            System.err.printf("更新Prometheus指标失败: %s\n", e.getMessage());
        }
    }
    
    /**
     * 更新最终Prometheus指标
     */
    private static void updateFinalPrometheusMetrics(long finalCount, long finalErrors, long finalBytes,
                                                   double finalQps, double finalMbps, double errorRate) {
        if (metricsCollector == null) return;
        
        try {
            // 最终统计指标
            metricsCollector.setGauge("test.final.messages.total", finalCount);
            metricsCollector.setGauge("test.final.errors.total", finalErrors);
            metricsCollector.setGauge("test.final.bytes.total", finalBytes);
            metricsCollector.setGauge("test.final.qps", finalQps);
            metricsCollector.setGauge("test.final.throughput.mbps", finalMbps);
            metricsCollector.setGauge("test.final.error.rate.percent", errorRate);
            metricsCollector.setGauge("test.final.success.rate.percent", 100.0 - errorRate);
            
            // 测试完成标记
            metricsCollector.setGauge("test.completed", 1.0);
            
        } catch (Exception e) {
            System.err.printf("更新最终Prometheus指标失败: %s\n", e.getMessage());
        }
    }
} 