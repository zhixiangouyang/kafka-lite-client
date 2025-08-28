package org.example.kafkalite.client;

import org.example.kafkalite.producer.HashPartitioner;
import org.example.kafkalite.producer.KafkaLiteProducer;
import org.example.kafkalite.producer.KafkaLiteProducerImpl;
import org.example.kafkalite.producer.ProducerRecord;
import org.example.kafkalite.producer.ProducerConfig;
import org.example.kafkalite.metadata.MetadataManagerImpl;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;

/**
 * 域名支持生产者测试
 * 演示新的智能DR切换：域名解析 + 自动重解析
 * 支持域名指向的集群变化后自动切换
 */
public class DomainSupportProducerTest {
    private static volatile KafkaLiteProducerImpl producer;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        System.out.println("=== 域名支持生产者DR切换测试 ===");
        System.out.println("新功能演示:");
        System.out.println("  1. 支持直接传入域名:端口");
        System.out.println("  2. 自动解析域名为多个IP");
        System.out.println("  3. 失败时自动重新解析DNS");
        System.out.println("  4. 透明切换到新的IP");
        System.out.println("  5. 持续生产消息，演示自动切换");
        System.out.println("按 Ctrl+C 停止测试\n");

        // 添加信号处理器
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n收到关闭信号，正在关闭生产者...");
            running.set(false);
            if (producer != null) {
                try {
                    producer.close();
                    System.out.println("生产者已关闭");
                } catch (Exception e) {
                    System.err.println("关闭生产者时出错: " + e.getMessage());
                }
            }
        }));

        // 测试新的域名支持
        testDomainSupport(args);
    }

    /**
     * 测试域名支持功能
     */
    private static void testDomainSupport(String[] args) {
        // 可以改为您的实际域名
        String kafkaDomain = "kafka.kafka_dr_1_noacl_test.test.mq.shopee.io:19092";

        // 如果命令行传入了域名参数，使用命令行参数
        if (args.length > 0) {
            kafkaDomain = args[0];
        }

        ProducerConfig config = createTestConfig();

        try {
            System.out.println("=== 创建支持域名的生产者 ===");
            System.out.printf("使用域名: %s%n", kafkaDomain);

            // 新功能：解析域名为IP列表，创建支持动态DNS的MetadataManager
            List<String> bootstrapServers = resolveDomainToIPs(kafkaDomain);
            System.out.printf("域名解析结果: %s%n", bootstrapServers);

            // 创建支持动态DNS的MetadataManager
            MetadataManagerImpl metadataManager = new MetadataManagerImpl(bootstrapServers, config.getConnectionPoolSize(), kafkaDomain);

            // 设置bootstrap servers变化回调，处理DNS重解析后的连接更新
            metadataManager.setBootstrapServersChangedCallback(() -> {
                handleBootstrapServersChanged(metadataManager);
            });

            // 创建生产者实例
            producer = new KafkaLiteProducerImpl(bootstrapServers, new HashPartitioner(), config);

            // 通过反射设置MetadataManager，使其支持动态DNS
            setMetadataManager(producer, metadataManager);

            System.out.println("\n=== 开始持续生产消息（演示自动DR切换） ===");
            System.out.println("新功能: 双重DNS检查机制");
            System.out.println("  1. 主动检查: 每次metadata refresh都检查DNS变化");
            System.out.println("  2. 被动检查: 所有broker失败时重新解析DNS");
            System.out.println();
            System.out.println("测试方法:");
            System.out.println("  方法1: 修改域名指向，无需停止原broker");
            System.out.println("  方法2: 停止所有当前IP的broker，启动新IP的broker");
            System.out.println();

            // 持续生产消息循环
            startMessageProduction();

        } catch (Exception e) {
            System.err.println("生产者创建异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭生产者
            System.out.println("关闭生产者...");
            if (producer != null) {
                producer.close();
            }
            System.out.println("测试结束");
        }
    }

    /**
     * 启动消息生产
     */
    private static void startMessageProduction() {
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
        final int maxQps = 10000; // 限制最大QPS
        final AtomicLong lastRateCheckTime = new AtomicLong(startTime);
        final AtomicLong messagesSinceLastCheck = new AtomicLong(0);

        // 获取配置
        ProducerConfig config = createTestConfig();

        try {
            // 创建QPS监控线程
            Thread monitorThread = new Thread(() -> {
                try {
                    long lastCount = 0;
                    long lastTime = System.currentTimeMillis();

                    while (running.get()) {
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

            // 创建多个发送线程，提高生产速度
            int producerThreads = 2; // 使用2个线程并行生产消息
            Thread[] producerThreadsArray = new Thread[producerThreads];

            for (int t = 0; t < producerThreads; t++) {
                final int threadId = t;
                producerThreadsArray[t] = new Thread(() -> {
                    int localIndex = threadId * 1000000; // 每个线程使用不同的起始索引
                    Random random = new Random();

                    try {
                        while (running.get()) {
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
                                            "cluster-test-topic-11",
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

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("消息生产被中断: " + e.getMessage());
        }
    }

    /**
     * 解析域名为IP地址列表
     */
    private static List<String> resolveDomainToIPs(String domainWithPort) {
        List<String> ips = new ArrayList<>();

        String[] parts = domainWithPort.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("域名格式错误，应为 domain:port，实际: " + domainWithPort);
        }

        String domain = parts[0];
        String port = parts[1];

        // 如果已经是IP地址，直接返回
        if (isValidIP(domain)) {
            ips.add(domainWithPort);
            return ips;
        }

        try {
            System.out.printf("正在解析域名: %s%n", domain);
            InetAddress[] addresses = InetAddress.getAllByName(domain);

            for (InetAddress address : addresses) {
                String ip = address.getHostAddress();
                String ipWithPort = ip + ":" + port;
                ips.add(ipWithPort);
                System.out.printf("解析到IP: %s%n", ipWithPort);
            }

            if (ips.isEmpty()) {
                throw new RuntimeException("域名解析结果为空: " + domain);
            }

        } catch (Exception e) {
            System.err.printf("域名解析失败: %s, 错误: %s%n", domain, e.getMessage());
            throw new RuntimeException("Failed to resolve domain: " + domain, e);
        }

        return ips;
    }

    /**
     * 检查是否为有效的IP地址
     */
    private static boolean isValidIP(String ip) {
        try {
            String[] parts = ip.split("\\.");
            if (parts.length != 4) return false;

            for (String part : parts) {
                int num = Integer.parseInt(part);
                if (num < 0 || num > 255) return false;
            }
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 处理bootstrap servers变化
     */
    private static void handleBootstrapServersChanged(MetadataManagerImpl metadataManager) {
        System.out.println("\n[生产者] 开始处理bootstrap servers变化...");

        try {
            // 获取新的bootstrap servers
            List<String> newBootstrapServers = metadataManager.getBootstrapServers();
            System.out.printf("[生产者] 新的bootstrap servers: %s%n", newBootstrapServers);

            // 这里可以添加更多的处理逻辑，比如：
            // 1. 更新连接池
            // 2. 清理旧的元数据缓存
            // 3. 重新初始化某些组件

            System.out.println("[生产者] bootstrap servers变化处理完成");

        } catch (Exception e) {
            System.err.printf("[生产者] 处理bootstrap servers变化时出错: %s%n", e.getMessage());
        }
    }

    /**
     * 通过反射设置MetadataManager
     */
    private static void setMetadataManager(KafkaLiteProducerImpl producer, MetadataManagerImpl metadataManager) {
        try {
            java.lang.reflect.Field field = KafkaLiteProducerImpl.class.getDeclaredField("metadataManager");
            field.setAccessible(true);
            field.set(producer, metadataManager);
            System.out.println("成功设置支持动态DNS的MetadataManager");
        } catch (Exception e) {
            System.err.printf("设置MetadataManager失败: %s%n", e.getMessage());
            System.err.println(" 生产者将使用默认的MetadataManager，不支持动态DNS切换");
        }
    }

    /**
     * 创建测试配置
     */
    private static ProducerConfig createTestConfig() {
        return new ProducerConfig.Builder()
                .batchSize(1024 * 10)  // 10KB批次大小
                .lingerMs(1)           // 1ms等待时间，提高吞吐量
                .maxRetries(3)
                .acks((short) 1)       // 使用acks=1，平衡性能和可靠性
                .maxQueueSize(100000)  // 队列大小
                .connectionPoolSize(20) // 增加连接池大小，避免耗尽
                .build();
    }

    /**
     * 生成指定大小的随机消息
     */
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

    /**
     * 预生成一些消息模板，避免每次都生成新的随机消息
     */
    private static String[] generateMessageTemplates(int count, int sizeInBytes) {
        String[] templates = new String[count];
        for (int i = 0; i < count; i++) {
            templates[i] = generateMessage(sizeInBytes);
        }
        return templates;
    }
}

/**
 * 测试说明：
 *
 * 1. 启动方式：
 *    java -cp target/classes org.example.kafkalite.client.DomainSupportProducerTest
 *    或者指定域名：
 *    java -cp target/classes org.example.kafkalite.client.DomainSupportProducerTest "kafka.domain.com:9092"
 *
 * 2. 观察要点：
 *    a) 启动时的域名解析日志
 *    b) 正常生产消息的过程
 *    c) 当所有broker失败时的DNS重解析日志
 *    d) 重解析后的重试连接过程
 *
 * 3. 测试DR切换：
 *    a) 停止当前所有broker (如 localhost:9092)
 *    b) 修改DNS，让域名指向新的IP
 *    c) 启动新IP的broker
 *    d) 观察客户端自动重解析并连接新broker
 *
 * 4. 关键日志：
 *    [DomainSupportProducerTest] 域名 localhost:9092 解析到 X 个IP: [...]
 *
 *    主动检查（新功能）:
 *    [MetadataManagerImpl] 主动发现DNS变化:
 *    [MetadataManagerImpl]   当前IP列表: [old_ips]
 *    [MetadataManagerImpl]   新解析IP列表: [new_ips]
 *    [MetadataManagerImpl] 主动切换完成: [new_ips]
 *
 *    被动检查（兜底）:
 *    [MetadataManagerImpl] 所有broker都不可用，尝试重新解析DNS...
 *    [MetadataManagerImpl] DNS重解析获得新IP: 旧=[...], 新=[...]
 *
 *    通用处理:
 *    [MetadataManagerImpl] 通知组件bootstrap servers已更新: [...]
 *    [DomainSupportProducerTest] 开始处理bootstrap servers变化...
 *    [DomainSupportProducerTest] bootstrap servers变化处理完成
 *
 * 5. 与传统方案对比：
 *    传统: new KafkaLiteProducerImpl(Arrays.asList("ip1:9092", "ip2:9092"), partitioner, config)
 *    新方案: 通过反射设置支持动态DNS的MetadataManager
 *
 *    优势：
 *    - 配置简化：只需一个域名
 *    - 自动发现：DNS变化时自动切换
 *    - 透明处理：业务代码无需修改
 *    - 持续生产：在切换过程中继续生产消息
 *
 * 6. 注意事项：
 *    - 使用反射设置MetadataManager，确保生产者支持动态DNS
 *    - 监控队列大小，避免在切换过程中队列溢出
 *    - 观察错误率，确保切换过程中的消息可靠性
 */ 