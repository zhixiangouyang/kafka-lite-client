package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;

import java.util.Arrays;
import java.util.List;

public class KafkaConsumerTest {
    public static void main(String[] args) {
//        1
        // 1. 配置 broker 地址
//        String broker = "localhost:9092";
        String broker = "10.251.183.199:27462";

        // 2. 创建消费者配置
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(true);           // 启用自动提交
        config.setAutoCommitIntervalMs(5000);       // 自动提交间隔5秒
        config.setFetchMaxBytes(1024 * 1024);       // 单次最多拉取1MB数据
        config.setMaxRetries(3);                    // 最大重试次数
        config.setRetryBackoffMs(1000);             // 重试间隔1秒
        config.setHeartbeatIntervalMs(3000);        // 心跳间隔3秒

        // 3. 创建消费者实例
        KafkaLiteConsumer consumer = new KafkaLiteConsumerImpl(
            "test-group-1",                    // 消费者组ID
            Arrays.asList(broker),           // Kafka集群地址
            config                           // 配置
        );

        try {
            // 4. 订阅主题
            consumer.subscribe(Arrays.asList("ouyangTest3"));

            System.out.println("开始消费消息...");
            System.out.println("按 Ctrl+C 停止消费");

            // 5. 循环消费消息
            while (true) {
                // 拉取消息，超时时间1秒
                List<ConsumerRecord> records = consumer.poll(1000);
                
                // 打印消费到的消息
                for (ConsumerRecord record : records) {
                    // System.out.printf("收到消息: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                    //     record.getTopic(),
                    //     record.getPartition(),
                    //     record.getOffset(),
                    //     record.getKey(),
                    //     record.getValue()
                    // );
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
            }
        } catch (InterruptedException e) {
            System.out.println("消费者被中断");
        } catch (Exception e) {
            System.err.println("消费异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. 关闭消费者
            System.out.println("关闭消费者...");
            consumer.close();
        }
    }
} 