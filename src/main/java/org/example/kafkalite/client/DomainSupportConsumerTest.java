package org.example.kafkalite.client;

import org.example.kafkalite.consumer.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 域名支持测试
 * 演示新的智能DR切换：域名解析 + 自动重解析
 */
public class DomainSupportConsumerTest {
    private static volatile KafkaLiteConsumerImpl consumer;
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) {
        System.out.println("=== 域名支持DR切换测试 ===");
        System.out.println("新功能演示:");
        System.out.println("  1. 支持直接传入域名:端口");
        System.out.println("  2. 自动解析域名为多个IP");
        System.out.println("  3. 失败时自动重新解析DNS");
        System.out.println("  4. 透明切换到新的IP");
        System.out.println("按 Ctrl+C 停止测试\n");
        
        // 添加信号处理器
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

        // 测试新的域名支持
        testDomainSupport();
    }
    
    /**
     * 测试域名支持功能
     */
    private static void testDomainSupport() {
        String kafkaDomain = "kafka.kafka_dr_1_noacl_test.test.mq.shopee.io:19092";  // 可以改为您的实际域名
        String groupId = "domain-support-test-group-2"; // 使用动态group ID避免历史offset影响
        ConsumerConfig config = createTestConfig();
        
        try {
            System.out.println("=== 创建支持域名的消费者 ===");
            
            // 新功能：直接传入域名，自动解析为IP
            consumer = new KafkaLiteConsumerImpl(groupId, kafkaDomain, config);
            
            // 订阅主题
            consumer.subscribe(Arrays.asList("cluster-test-topic-11"));
            
            System.out.println("\n=== 开始持续消费（演示自动DR切换） ===");
            System.out.println("新功能: 双重DNS检查机制");
            System.out.println("  1. 主动检查: 每次metadata refresh都检查DNS变化");
            System.out.println("  2. 被动检查: 所有broker失败时重新解析DNS");
            System.out.println();
            System.out.println("测试方法:");
            System.out.println("  方法1: 修改域名指向，无需停止原broker");
            System.out.println("  方法2: 停止所有当前IP的broker，启动新IP的broker");
            System.out.println();

            long messageCount = 0;
            long lastStatusTime = System.currentTimeMillis();
            int pollFailureCount = 0;

            // 持续消费循环
            while (running.get()) {
                try {
                    // 拉取消息
                    List<ConsumerRecord> records = consumer.poll(2000);
                    messageCount += records.size();
                    pollFailureCount = 0; // 重置失败计数
                    
                    // 打印消费到的消息
//                    for (ConsumerRecord record : records) {
//                        System.out.printf(" 收到消息: topic=%s, partition=%d\n",
//                            record.getTopic(),
//                            record.getPartition()
//                        );
//                    }

                    // 每15秒输出一次状态
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastStatusTime >= 15000) {
                        System.out.printf("\n[状态] 已消费消息数: %d\n", messageCount);
                        System.out.println("提醒：测试DR切换的两种方法");
                        System.out.println("   方法1: 修改DNS指向新IP (推荐，原broker可继续运行)");
                        System.out.println("   方法2: 停止所有broker测试故障恢复");
                        System.out.println("   观察 [MetadataManagerImpl] 的主动/被动DNS检查日志\n");
                        lastStatusTime = currentTime;
                    }

                    // 手动提交
                    if (!config.isEnableAutoCommit() && !records.isEmpty()) {
                        consumer.commitSync();
                    }

                    // 稍微休息
                    if (records.isEmpty()) {
                        Thread.sleep(1000);
                    }
                    
                } catch (Exception e) {
                    pollFailureCount++;
                    System.err.printf(" 消费异常 (第%d次): %s\n", pollFailureCount, e.getMessage());
                    
                    if (pollFailureCount <= 3) {
                        System.err.println("这可能触发了DNS重解析，观察上方日志");
                        System.err.println("   如果是网络问题，客户端会自动重试和重解析");
                    }
                    
                    // 短暂休息后继续
                    Thread.sleep(3000);
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
     * 创建测试配置
     */
    private static ConsumerConfig createTestConfig() {
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(true);  // 手动提交，便于观察
        config.setHeartbeatIntervalMs(3000);
        config.setMetadataRefreshIntervalMs(60000 * 10); // 改为5分钟，减少刷新频率
        config.setFetchMaxWaitMs(3000); // fetch超时3秒，域名切换测试需要快速响应
        
        // 连接池配置
        config.setMetadataConnectionPoolSize(3);  // 较小的连接池便于测试
        config.setMaxRetryCount(2);
        
        return config;
    }
}

/**
 * 测试说明：
 * 
 * 1. 启动方式：
 *    java -cp target/classes org.example.kafkalite.client.DomainSupportTest
 * 
 * 2. 观察要点：
 *    a) 启动时的域名解析日志
 *    b) 正常消费的过程
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
 *    [KafkaLiteConsumerImpl] 域名 localhost:9092 解析到 X 个IP: [...]
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
 *    [KafkaLiteConsumerImpl] 开始处理bootstrap servers变化...
 *    [KafkaLiteConsumerImpl] 已清空partition leader缓存
 *    [KafkaLiteConsumerImpl] 已更新topic XXX 的partition leaders: {...}
 * 
 * 5. 与传统方案对比：
 *    传统: new KafkaLiteConsumerImpl(groupId, Arrays.asList("ip1:9092", "ip2:9092"), config)
 *    新方案: new KafkaLiteConsumerImpl(groupId, "kafka.domain.com:9092", config)
 *    
 *    优势：
 *    - 配置简化：只需一个域名
 *    - 自动发现：DNS变化时自动切换
 *    - 透明处理：业务代码无需修改
 */ 