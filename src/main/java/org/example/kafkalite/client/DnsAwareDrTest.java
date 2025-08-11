package org.example.kafkalite.client;

import org.example.kafkalite.consumer.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DNS感知DR切换测试
 * 借鉴ConsumerBrokerSwitchTest的持续消费模式
 * 让消费者持续拉取消息，然后在运行过程中触发DR切换
 */
public class DnsAwareDrTest {
    private static volatile DnsAwareDrConsumer consumer;
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) {
        System.out.println("=== DNS感知DR切换测试 ===");
        System.out.println("持续消费模式，支持运行时DR切换");
        System.out.println("在消费过程中输入命令触发DR切换:");
        System.out.println("  - 输入 'switch' 触发DR切换");
        System.out.println("  - 输入 'dns' 手动触发DNS检查");  
        System.out.println("  - 输入 'status' 查看当前状态");
        System.out.println("  - 按 Ctrl+C 停止消费\n");
        
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

        // 配置DNS感知DR消费者
        String kafkaDomain = "kafka.kafka_dr_1_noacl_test.test.mq.shopee.io:19092"; // 可以改为您的实际域名
        String groupId = "cluster-switch-test-group";
        ConsumerConfig config = createTestConfig();
        
        try {
            // 创建DNS感知的DR消费者
            consumer = new DnsAwareDrConsumer(kafkaDomain, groupId, config);
            
            // 订阅主题
            consumer.subscribe(Arrays.asList("cluster-test-topic"));
            
            System.out.println("=== 初始状态 ===");
            System.out.println(consumer.getStatus());
            System.out.println();

            // 启动命令输入线程
            startCommandInputThread();

            System.out.println("开始持续消费消息...");
            System.out.println("提醒：现在可以输入命令来测试DR切换！\n");

            long messageCount = 0;
            long lastStatusTime = System.currentTimeMillis();

            // 持续消费循环
            while (running.get()) {
                try {
                    // 拉取消息，超时时间1秒
                    List<ConsumerRecord> records = consumer.poll(1000);
                    messageCount += records.size();
                    
                    // 打印消费到的消息
                    for (ConsumerRecord record : records) {
                        System.out.printf("收到消息: topic=%s, partition=%d, offset=%d, value=%s\n",
                            record.getTopic(),
                            record.getPartition(),
                            record.getOffset(),
                            record.getValue() != null ? new String(record.getValue()) : "null"
                        );
                    }

                    // 每10秒输出一次状态和提醒
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastStatusTime >= 10000) {
                        System.out.printf("\n[状态] 已消费消息数: %d\n", messageCount);
                        System.out.println("提醒：输入 'switch' 触发DR切换，'dns' 检查DNS变化！");
                        System.out.println(" 观察 [DnsAwareDrConsumer] 的切换日志\n");
                        lastStatusTime = currentTime;
                    }

                    // 手动提交
                    if (!config.isEnableAutoCommit() && !records.isEmpty()) {
                        consumer.commitSync();
                    }

                    // 稍微休息一下，避免打印太快
                    if (records.isEmpty()) {
                        Thread.sleep(1000);
                    }
                    
                } catch (Exception e) {
                    System.err.printf("⚠️  消费过程中发生异常: %s\n", e.getMessage());
                    System.err.println("这可能是DR切换引起的，客户端会自动重试...");
                    
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
            // 关闭消费者
            System.out.println("关闭消费者...");
            if (consumer != null) {
                consumer.close();
            }
            System.out.println("测试结束");
        }
    }
    
    /**
     * 启动命令输入线程
     */
    private static void startCommandInputThread() {
        Thread commandThread = new Thread(() -> {
            try (java.util.Scanner scanner = new java.util.Scanner(System.in)) {
                while (running.get()) {
                    String command = scanner.nextLine().trim().toLowerCase();
                    
                    switch (command) {
                        case "switch":
                            System.out.println("\n🔀 [命令] 手动触发DR切换...");
                            if (consumer != null) {
                                consumer.triggerFailover();
                            }
                            break;
                            
                        case "dns":
                            System.out.println("\n🔍 [命令] 手动触发DNS检查...");
                            if (consumer != null) {
                                consumer.triggerDnsCheck();
                            }
                            break;
                            
                        case "status":
                            System.out.println("\n📊 [命令] 当前状态:");
                            if (consumer != null) {
                                System.out.println(consumer.getStatus());
                            }
                            break;
                            
                        case "quit":
                        case "exit":
                            System.out.println("\n👋 [命令] 退出程序...");
                            running.set(false);
                            break;
                            
                        case "help":
                            System.out.println("\n📖 [命令] 可用命令:");
                            System.out.println("  switch - 触发DR切换");
                            System.out.println("  dns    - 检查DNS变化");
                            System.out.println("  status - 查看当前状态");
                            System.out.println("  quit   - 退出程序");
                            break;
                            
                        default:
                            if (!command.isEmpty()) {
                                System.out.printf("❓ 未知命令: %s (输入 'help' 查看帮助)\n", command);
                            }
                            break;
                    }
                }
            } catch (Exception e) {
                System.err.println("命令输入线程异常: " + e.getMessage());
            }
        });
        
        commandThread.setDaemon(true);
        commandThread.setName("Command-Input");
        commandThread.start();
    }
    
    /**
     * 创建测试配置
     */
    private static ConsumerConfig createTestConfig() {
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(false);  // 手动提交，便于观察
        config.setMaxPollRecords(10);
        config.setHeartbeatIntervalMs(3000);
        config.setMetadataRefreshIntervalMs(30000);
        
        // DNS相关配置 - 更短的间隔便于测试
        config.setDnsTtlMs(15000L);         // DNS TTL: 15秒
        config.setHealthCheckIntervalMs(5000L); // 健康检查: 5秒
        config.setMaxRetryCount(3);
        
        return config;
    }
}

/**
 * 测试使用说明:
 * 
 * 1. 启动程序：
 *    java -cp target/classes org.example.kafkalite.client.DnsAwareDrTest
 * 
 * 2. 程序运行后会持续消费消息，同时可以输入命令：
 *    - switch: 手动触发DR切换
 *    - dns: 手动触发DNS检查（模拟DNS变化检测）
 *    - status: 查看当前消费者状态
 *    - quit: 退出程序
 * 
 * 3. 测试DR切换的几种方式：
 *    a) 手动命令: 输入 'switch' 触发切换
 *    b) DNS模拟: 输入 'dns' 模拟DNS变化检测
 *    c) 真实DNS: 如果您能修改DNS记录，程序会自动检测
 * 
 * 4. 观察日志输出：
 *    - [DnsAwareDrConsumer] 开头的是DNS监控日志
 *    - [DrAwareConsumerDecorator] 开头的是DR切换日志
 *    - [ClusterDiscovery] 开头的是集群发现日志
 * 
 * 5. 预期的运行效果：
 *    - 正常消费：持续拉取消息并显示
 *    - DR切换：当触发切换时，会看到重建消费者的过程
 *    - 异常恢复：切换过程中的异常会自动重试
 */ 