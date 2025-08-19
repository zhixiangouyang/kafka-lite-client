package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者Broker切换测试
 * 基于KafkaConsumerTest，支持多broker配置和故障切换
 */
public class ConsumerBrokerSwitchTest {
    private static volatile KafkaLiteConsumer consumer;
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) {
        System.out.println("=== 消费者Broker切换测试 ===");
        System.out.println("配置多个broker支持故障切换");
        System.out.println("在消费过程中可以手动停止broker观察切换效果");
        System.out.println("按 Ctrl+C 停止消费\n");
        
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
        }));

        // 1. 配置多个broker地址 - 支持故障转移
        // 根据实际情况选择配置：
        // 选项1：纯本地配置（如果本地有完整的Kafka集群）
        // 选项2：混合配置（本地+远程）
        // 选项3：纯远程配置（基于日志中看到的实际leader地址）
        
        List<String> brokers = Arrays.asList(
            // 本地broker1
//            "localhost:9093",
            // 本地broker2
//            "localhost:9094"

            // 线上broker
            "10.251.176.5:19092",
            "10.251.135.195:19092",
            "10.251.184.8:19092"
        );
        System.out.println("配置的Broker列表: " + brokers);
        System.out.println("使用本地Kafka集群进行broker切换测试");

        // 2. 创建消费者配置
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(true);           // 启用自动提交
        config.setAutoCommitIntervalMs(5000);       // 自动提交间隔5秒
        config.setFetchMaxBytes(1024 * 1024);       // 单次最多拉取1MB数据
        config.setMaxRetries(3);                    // 最大重试次数
        config.setRetryBackoffMs(1000);             // 重试间隔1秒
        config.setHeartbeatIntervalMs(3000);        // 心跳间隔3秒
        // 优化：根据环境调整元数据刷新间隔
        // 测试环境：30秒（快速检测broker切换）
        // 生产环境：建议5分钟（300000ms）
        // 配置较短的刷新间隔用于测试智能刷新策略
        config.setMetadataRefreshIntervalMs(10000); // 10秒，用于测试正常情况下的刷新频率

        // 3. 创建消费者实例
        consumer = new KafkaLiteConsumerImpl(
            "broker-switch-test-group",          // 消费者组ID
            brokers,                             // 多个Kafka broker地址
            config                               // 配置
        );

        try {
            // 4. 订阅主题
//            consumer.subscribe(Arrays.asList("cluster1-test-topic"));
            consumer.subscribe(Arrays.asList("broker-test-topic-1"));

            System.out.println("开始消费消息...");
            System.out.println("现在可以手动停止broker来测试切换:");
            System.out.println("  - 停止9093: 在kafka目录执行停止命令");
            System.out.println("  - 停止9094: 同样停止对应的broker");
            System.out.println("观察日志中的broker切换信息\n");

            long messageCount = 0;
            long lastStatusTime = System.currentTimeMillis();

            // 5. 循环消费消息
            while (running.get()) {
                try {
                    // 拉取消息，超时时间1秒
                    List<ConsumerRecord> records = consumer.poll(1000);
                    messageCount += records.size();
                    
                    // 打印消费到的消息
                    for (ConsumerRecord record : records) {
                        System.out.printf("收到消息: topic=%s, partition=%d, offset=%d",
                            record.getTopic(),
                            record.getPartition(),
                            record.getOffset()
                        );
                    }

                    // 每10秒输出一次状态和提醒
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastStatusTime >= 10000) {
                        System.out.printf("\n[状态] 已消费消息数: %d\n", messageCount);
                        System.out.println("提醒：现在可以停止一个broker来测试故障切换！");
                        System.out.println("   观察 [MetadataManagerImpl] 的切换日志\n");
                        lastStatusTime = currentTime;
                    }

                    // 如果是手动提交，在这里提交
                    if (!config.isEnableAutoCommit()) {
                        consumer.commitSync();
                        System.out.println("手动提交完成");
                    }

                    // 稍微休息一下，避免打印太快
                    if (records.isEmpty()) {
                        Thread.sleep(1000);
                    }
                    
                } catch (Exception e) {
                    System.err.printf("⚠️  消费过程中发生异常: %s\n", e.getMessage());
                    System.err.println("这可能是broker切换引起的，客户端会自动重试...");
                    
                    // 短暂休息后继续
                    Thread.sleep(2000);
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
            System.out.println("测试结束");
        }
    }
} 