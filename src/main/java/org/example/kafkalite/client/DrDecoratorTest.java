package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.DrAwareConsumerDecorator;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DrAwareConsumerDecorator测试类
 * 演示装饰器模式的DR切换功能
 */
public class DrDecoratorTest {
    
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) {
        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n收到关闭信号，正在关闭消费者...");
            running.set(false);
        }));
        
        System.out.println("=== DrAwareConsumerDecorator装饰器模式测试 ===\n");
        
        // 测试场景1: 普通消费者（无DR功能）
        testBasicConsumer();
        
        System.out.println("\n==================================================\n");
        
        // 测试场景2: DR装饰器消费者
        testDrDecoratorConsumer();
    }
    
    /**
     * 测试场景1: 普通消费者
     */
    private static void testBasicConsumer() {
        System.out.println("📍 测试场景1: 普通消费者（无DR功能）");
        
        ConsumerConfig config = createBasicConfig();
        
        // 直接使用基础实现
        KafkaLiteConsumer consumer = new KafkaLiteConsumerImpl(
            "basic-test-group",
            Arrays.asList("localhost:9092", "localhost:9093"),
            config
        );
        
        testConsumerBehavior(consumer, "普通消费者");
    }
    
    /**
     * 测试场景2: DR装饰器消费者
     */
    private static void testDrDecoratorConsumer() {
        System.out.println("📍 测试场景2: DR装饰器消费者");
        
        ConsumerConfig config = createDrConfig();
        
        // 主集群和备集群
        List<String> primaryServers = Arrays.asList("localhost:9092", "localhost:9093");
        List<String> secondaryServers = Arrays.asList("localhost:9094", "localhost:9095");
        
        // 使用装饰器模式
        DrAwareConsumerDecorator drConsumer = new DrAwareConsumerDecorator(
            primaryServers, secondaryServers, "dr-test-group", config
        );
        
        testDrConsumerBehavior(drConsumer);
    }
    
    /**
     * 测试普通消费者行为
     */
    private static void testConsumerBehavior(KafkaLiteConsumer consumer, String consumerType) {
        try {
            // 订阅主题
            consumer.subscribe(Arrays.asList("test-topic"));
            
            System.out.printf("开始消费消息（%s）...\n", consumerType);
            
            // 消费一段时间
            int messageCount = 0;
            long startTime = System.currentTimeMillis();
            
            while (running.get() && messageCount < 5 && 
                   (System.currentTimeMillis() - startTime) < 15000) {
                
                List<ConsumerRecord> records = consumer.poll(1000);
                
                for (ConsumerRecord record : records) {
                    System.out.printf("[%s] 消费消息: topic=%s, partition=%d, offset=%d, value=%s\n",
                        consumerType, record.getTopic(), record.getPartition(), record.getOffset(), 
                        new String(record.getValue()));
                    messageCount++;
                }
                
                if (messageCount > 0 && messageCount % 3 == 0) {
                    consumer.commitSync();
                    System.out.printf("[%s] 提交偏移量\n", consumerType);
                }
            }
            
            System.out.printf("[%s] 消费完成，共消费 %d 条消息\n", consumerType, messageCount);
            
        } catch (Exception e) {
            System.err.printf("[%s] 消费异常: %s\n", consumerType, e.getMessage());
        } finally {
            consumer.close();
        }
    }
    
    /**
     * 测试DR消费者行为
     */
    private static void testDrConsumerBehavior(DrAwareConsumerDecorator drConsumer) {
        try {
            // 订阅主题
            drConsumer.subscribe(Arrays.asList("dr-test-topic"));
            
            System.out.println("开始消费消息（DR装饰器）...");
            System.out.println("初始状态: " + drConsumer.getStatus());
            
            // 消费一段时间
            int messageCount = 0;
            long startTime = System.currentTimeMillis();
            
            while (running.get() && messageCount < 10 && 
                   (System.currentTimeMillis() - startTime) < 30000) {
                
                List<ConsumerRecord> records = drConsumer.poll(1000);
                
                for (ConsumerRecord record : records) {
                    System.out.printf("[DR消费者] 消费消息: topic=%s, partition=%d, offset=%d, value=%s\n",
                        record.getTopic(), record.getPartition(), record.getOffset(), 
                        new String(record.getValue()));
                    messageCount++;
                }
                
                // 定期提交偏移量和显示状态
                if (messageCount > 0 && messageCount % 3 == 0) {
                    drConsumer.commitSync();
                    System.out.println("状态: " + drConsumer.getStatus());
                }
                
                // 模拟故障转移（测试DR功能）
                if (messageCount == 5) {
                    System.out.println("\n模拟故障，触发DR切换...");
                    drConsumer.triggerFailover();
                    Thread.sleep(2000); // 等待切换完成
                    System.out.println("切换后状态: " + drConsumer.getStatus());
                    System.out.println();
                }
            }
            
            System.out.printf("[DR消费者] 消费完成，共消费 %d 条消息\n", messageCount);
            System.out.println("最终状态: " + drConsumer.getStatus());
            
            // 显示DNS缓存状态
            drConsumer.printDnsCache();
            
        } catch (Exception e) {
            System.err.printf("[DR消费者] 消费异常: %s\n", e.getMessage());
        } finally {
            drConsumer.close();
        }
    }
    
    /**
     * 创建基础配置
     */
    private static ConsumerConfig createBasicConfig() {
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(false);
        config.setMaxPollRecords(10);
        config.setHeartbeatIntervalMs(5000);
        config.setMetadataRefreshIntervalMs(30000);
        return config;
    }
    
    /**
     * 创建DR配置
     */
    private static ConsumerConfig createDrConfig() {
        ConsumerConfig config = createBasicConfig();
        
        // DR相关配置
        config.setDnsTtlMs(60000L);        // DNS TTL: 1分钟
        config.setHealthCheckIntervalMs(10000L); // 健康检查: 10秒
        config.setMaxRetryCount(3);        // 最大重试: 3次
        
        return config;
    }
} 