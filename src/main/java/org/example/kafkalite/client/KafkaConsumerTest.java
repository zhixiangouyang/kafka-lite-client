package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.HashMap;

public class KafkaConsumerTest {
    private static volatile KafkaLiteConsumer consumer;
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) {
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

        // 1. 配置 broker 地址
        // 消费者组测试
         String broker = "10.251.176.5:19092";

        // broker测试
//        String broker = "10.251.176.5:19092";
        // 本地测试

//        String broker = "localhost:9092";

        // 2. 创建消费者配置
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(true);           // 启用自动提交
        config.setAutoCommitIntervalMs(5000);       // 自动提交间隔5秒
        config.setFetchMaxBytes(1024 * 1024);       // 单次最多拉取1MB数据
        config.setFetchMaxWaitMs(3000);             // fetch超时2秒，基础消费测试
        config.setMaxRetries(3);                    // 最大重试次数
        config.setRetryBackoffMs(1000);             // 重试间隔1秒
        config.setHeartbeatIntervalMs(1000);        // 心跳间隔1秒

        // 3. 创建消费者实例
        consumer = new KafkaLiteConsumerImpl(
            "rebalance-test-group-3",                    // 消费者组ID
            Arrays.asList(broker),           // Kafka集群地址
            config                           // 配置
        );

        try {
            // 4. 订阅主题
            consumer.subscribe(Arrays.asList("broker-test-topic-1"));
//            consumer.subscribe(Arrays.asList("rebalance-test-topic-6"));
//            consumer.subscribe(Arrays.asList("rebalance-test-topic"));

            System.out.println("开始消费消息...");
            System.out.println("按 Ctrl+C 停止消费");

            // 5. 循环消费消息
            while (running.get()) {
                // 拉取消息，超时时间1秒
                List<ConsumerRecord> records = consumer.poll(1000);
                
                // 打印消费到的消息
//                for (ConsumerRecord record : records) {
//                    System.out.printf("收到消息: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
//                        record.getTopic(),
//                        record.getPartition(),
//                        record.getOffset(),
//                        record.getKey(),
//                        record.getValue()
//                    );
//                }
                
                // 新增：统计各分区消费情况
                if (!records.isEmpty()) {
                    Map<Integer, Integer> partitionCounts = new HashMap<>();
                    for (ConsumerRecord record : records) {
                        partitionCounts.put(record.getPartition(), 
                            partitionCounts.getOrDefault(record.getPartition(), 0) + 1);
                    }
                    System.out.printf("本次poll各分区消息统计: %s%n", partitionCounts);
                }

                // 如果是手动提交，在这里提交
                if (!config.isEnableAutoCommit()) {
                    consumer.commitSync();
                    System.out.println("手动提交完成");
                }

                // 稍微休息一下，避免打印太快
                if (records.isEmpty()) {
                    Thread.sleep(100);
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
        }
    }
} 