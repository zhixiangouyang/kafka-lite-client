package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;
import org.example.kafkalite.monitor.MetricsCollector;
import org.example.kafkalite.monitor.PrometheusMetricsServer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带Prometheus监控的Kafka消费者测试类
 * 基于KafkaConsumerTest，增加了可视化监控功能
 */
public class KafkaConsumerMonitorTest {
    private static volatile KafkaLiteConsumer consumer;
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static MetricsCollector metricsCollector;
    private static PrometheusMetricsServer metricsServer;
    
    // 统计变量
    private static final AtomicLong messageCount = new AtomicLong(0);
    private static final AtomicLong totalBytesConsumed = new AtomicLong(0);
    private static final AtomicLong commitCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);
    
    public static void main(String[] args) {
        // 初始化监控系统
        initializeMonitoring();
        
        // 添加信号处理器，确保Ctrl+C时能正确关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n收到关闭信号，正在关闭消费者...");
            running.set(false);
            if (consumer != null) {
                try {
                    consumer.close();
                    System.out.println("消费者已关闭");
                } catch (Exception e) {
                    System.err.println("关闭消费者时出错: " + e.getMessage());
                }
            }
            
            // 关闭监控服务器
            if (metricsServer != null && metricsServer.isRunning()) {
                metricsServer.stop();
                System.out.println("监控服务器已关闭");
            }
        }));

        // 1. 配置 broker 地址
        String broker = "10.251.183.199:27462";

        // 2. 创建消费者配置
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(true);           // 启用自动提交
        config.setAutoCommitIntervalMs(5000);       // 自动提交间隔5秒
        config.setFetchMaxBytes(1024 * 1024);       // 单次最多拉取1MB数据
        config.setMaxRetries(3);                    // 最大重试次数
        config.setRetryBackoffMs(1000);             // 重试间隔1秒
        config.setHeartbeatIntervalMs(1000);        // 心跳间隔1秒

        // 3. 创建消费者实例
        consumer = new KafkaLiteConsumerImpl(
            "consumer-monitor-test-group-1",           // 消费者组ID
            Arrays.asList(broker),                   // Kafka集群地址
            config                                   // 配置
        );

        // 启动监控线程
        Thread monitorThread = createMonitoringThread();
        monitorThread.setDaemon(true);
        monitorThread.start();

        try {
            // 4. 订阅主题
            consumer.subscribe(Arrays.asList("produce-consume-test-2"));

            System.out.println("🚀 开始消费消息...");
            System.out.printf("📍 目标Broker: %s\n", broker);
            System.out.printf("📊 监控端点: http://localhost:8084/metrics\n");
            System.out.printf("💚 健康检查: http://localhost:8084/health\n");
            System.out.println("按 Ctrl+C 停止消费");
            System.out.println("================================================================================");

            long startTime = System.currentTimeMillis();

            // 5. 循环消费消息
            while (running.get()) {
                long pollStartTime = System.currentTimeMillis();
                
                try {
                    // 拉取消息，超时时间1秒
                    List<ConsumerRecord> records = consumer.poll(1000);
                    
                    long pollLatency = System.currentTimeMillis() - pollStartTime;
                    
                    // 记录监控指标
                    if (metricsCollector != null) {
                        metricsCollector.recordLatency("consumer_poll_latency_ms", pollLatency);
                        metricsCollector.incrementCounter("consumer_poll_attempts");
                        
                        if (!records.isEmpty()) {
                            metricsCollector.incrementCounter("consumer_poll_success");
                            metricsCollector.setGauge("consumer_batch_size", records.size());
                        }
                    }
                    
                    // 处理消费到的消息
                    if (!records.isEmpty()) {
                        long messageBytes = 0;
                        for (ConsumerRecord record : records) {
                            messageCount.incrementAndGet();
                            
                            // 计算消息大小
                            if (record.getValue() != null) {
                                messageBytes += record.getValue().length();
                            }
                            if (record.getKey() != null) {
                                messageBytes += record.getKey().length();
                            }
                        }
                        
                        totalBytesConsumed.addAndGet(messageBytes);
                        
                        // 记录监控指标
                        if (metricsCollector != null) {
                            metricsCollector.setGauge("consumer_messages_received", records.size());
                            metricsCollector.setGauge("consumer_bytes_received", messageBytes);
                        }
                    }

                    // 如果是手动提交，在这里提交
                    if (!config.isEnableAutoCommit() && !records.isEmpty()) {
                        long commitStartTime = System.currentTimeMillis();
                        try {
                            consumer.commitSync();
                            commitCount.incrementAndGet();
                            
                            long commitLatency = System.currentTimeMillis() - commitStartTime;
                            if (metricsCollector != null) {
                                metricsCollector.recordLatency("consumer_commit_latency_ms", commitLatency);
                                metricsCollector.incrementCounter("consumer_commit_success");
                            }
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            if (metricsCollector != null) {
                                metricsCollector.incrementCounter("consumer_commit_errors");
                            }
                            System.err.printf("提交失败: %s\n", e.getMessage());
                        }
                    }

                    // 稍微休息一下，避免打印太快
                    if (records.isEmpty()) {
                        Thread.sleep(100);
                    }
                    
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    if (metricsCollector != null) {
                        metricsCollector.incrementCounter("consumer_poll_errors");
                    }
                    System.err.printf("消费异常: %s\n", e.getMessage());
                    Thread.sleep(1000); // 发生错误时等待更长时间
                }
            }
        } catch (InterruptedException e) {
            System.out.println("消费者被中断");
        } catch (Exception e) {
            System.err.println("消费异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. 关闭消费者
            System.out.println("关闭消费者...");
            if (consumer != null) {
                consumer.close();
            }
            
            // 打印最终统计
            printFinalStats();
            
            // 保持监控服务器运行一段时间，方便查看最终数据
            System.out.println("监控服务器将继续运行60秒，方便查看最终数据...");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * 初始化监控系统
     */
    private static void initializeMonitoring() {
        try {
            // 创建指标收集器
            metricsCollector = new MetricsCollector("kafka-consumer-test", "consumer-instance-001");
            
            // 创建并启动Prometheus服务器 (使用8084端口避免冲突)
            metricsServer = PrometheusMetricsServer.create(metricsCollector, 8084);
            metricsServer.start();
            
            System.out.println("✅ Prometheus监控服务器已启动: http://localhost:8084/metrics");
            
        } catch (Exception e) {
            System.err.printf("⚠️ 监控系统初始化失败: %s\n", e.getMessage());
            System.err.println("⚠️ 测试将继续，但没有监控功能");
        }
    }
    
    /**
     * 创建监控线程
     */
    private static Thread createMonitoringThread() {
        return new Thread(() -> {
            try {
                long lastCount = 0;
                long lastBytes = 0;
                long lastCommits = 0;
                long lastTime = System.currentTimeMillis();
                long startTime = lastTime;
                
                while (running.get()) {
                    Thread.sleep(5000); // 每5秒报告一次
                    
                    long now = System.currentTimeMillis();
                    long count = messageCount.get();
                    long bytes = totalBytesConsumed.get();
                    long commits = commitCount.get();
                    long errors = errorCount.get();
                    
                    double elapsedSeconds = (now - startTime) / 1000.0;
                    double totalMPS = count / elapsedSeconds; // Messages Per Second
                    double mbps = (bytes / (1024.0 * 1024.0)) / elapsedSeconds; // MB/s
                    
                    // 计算最近5秒的速率
                    double recentMPS = (count - lastCount) / ((now - lastTime) / 1000.0);
                    double recentMBPS = ((bytes - lastBytes) / (1024.0 * 1024.0)) / ((now - lastTime) / 1000.0);
                    
                    System.out.printf("📊 [%.1fs] 消息: %d条(%.2fMB), 提交: %d次, 错误: %d, 平均MPS: %.2f, 最近MPS: %.2f, 吞吐量: %.2fMB/s%n", 
                        elapsedSeconds,
                        count,
                        bytes / (1024.0 * 1024.0),
                        commits,
                        errors,
                        totalMPS,
                        recentMPS,
                        mbps);
                    
                    // 更新Prometheus指标
                    updatePrometheusMetrics(count, bytes, commits, errors, totalMPS, recentMPS, mbps);
                    
                    lastCount = count;
                    lastBytes = bytes;
                    lastCommits = commits;
                    lastTime = now;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * 更新Prometheus指标
     */
    private static void updatePrometheusMetrics(long count, long bytes, long commits, long errors, 
                                               double totalMPS, double recentMPS, double mbps) {
        if (metricsCollector == null) return;
        
        try {
            // 基础统计指标
            metricsCollector.setGauge("consumer_total_messages", count);
            metricsCollector.setGauge("consumer_total_bytes", bytes);
            metricsCollector.setGauge("consumer_total_commits", commits);
            metricsCollector.setGauge("consumer_total_errors", errors);
            
            // 性能指标
            metricsCollector.setGauge("consumer_messages_per_second", totalMPS);
            metricsCollector.setGauge("consumer_recent_messages_per_second", recentMPS);
            metricsCollector.setGauge("consumer_throughput_mbps", mbps);
            
            // 延迟指标
            double avgPollLatency = metricsCollector.getAverageLatency("consumer_poll_latency_ms");
            double p99PollLatency = metricsCollector.getP99Latency("consumer_poll_latency_ms");
            
            metricsCollector.setGauge("consumer_avg_poll_latency_ms", avgPollLatency);
            metricsCollector.setGauge("consumer_p99_poll_latency_ms", p99PollLatency);
            
            if (metricsCollector.getAverageLatency("consumer_commit_latency_ms") > 0) {
                double avgCommitLatency = metricsCollector.getAverageLatency("consumer_commit_latency_ms");
                double p99CommitLatency = metricsCollector.getP99Latency("consumer_commit_latency_ms");
                
                metricsCollector.setGauge("consumer_avg_commit_latency_ms", avgCommitLatency);
                metricsCollector.setGauge("consumer_p99_commit_latency_ms", p99CommitLatency);
            }
            
            // 错误率
            double errorRate = count > 0 ? (errors * 100.0) / count : 0;
            metricsCollector.setGauge("consumer_error_rate_percent", errorRate);
            
        } catch (Exception e) {
            System.err.printf("更新Prometheus指标失败: %s\n", e.getMessage());
        }
    }
    
    /**
     * 打印最终统计信息
     */
    private static void printFinalStats() {
        long finalCount = messageCount.get();
        long finalBytes = totalBytesConsumed.get();
        long finalCommits = commitCount.get();
        long finalErrors = errorCount.get();
        
        System.out.println("\n================================================================================");
        System.out.println("🏁 最终消费统计:");
        System.out.println("================================================================================");
        System.out.printf("📈 消息统计: 总消费=%d条, 总字节=%.2fMB, 提交=%d次, 错误=%d次\n", 
            finalCount, 
            finalBytes / (1024.0 * 1024.0), 
            finalCommits, 
            finalErrors);
        
        if (metricsCollector != null) {
            double avgPollLatency = metricsCollector.getAverageLatency("consumer_poll_latency_ms");
            double p99PollLatency = metricsCollector.getP99Latency("consumer_poll_latency_ms");
            
            System.out.printf("⏱️  Poll延迟: 平均=%.2fms, P99=%.2fms\n", avgPollLatency, p99PollLatency);
            
            if (metricsCollector.getAverageLatency("consumer_commit_latency_ms") > 0) {
                double avgCommitLatency = metricsCollector.getAverageLatency("consumer_commit_latency_ms");
                double p99CommitLatency = metricsCollector.getP99Latency("consumer_commit_latency_ms");
                System.out.printf("⏱️  Commit延迟: 平均=%.2fms, P99=%.2fms\n", avgCommitLatency, p99CommitLatency);
            }
        }
        
        double errorRate = finalCount > 0 ? (finalErrors * 100.0) / finalCount : 0;
        System.out.printf("⚠️  错误率: %.3f%%\n", errorRate);
        
        System.out.println("================================================================================");
        System.out.println("📊 监控数据已保存到Prometheus，可通过以下方式查看:");
        System.out.println("  • 指标端点: http://localhost:8084/metrics");
        System.out.println("  • 建议配置Grafana进行可视化展示");
        System.out.println("================================================================================");
    }
} 