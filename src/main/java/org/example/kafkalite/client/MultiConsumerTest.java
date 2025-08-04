package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MultiConsumerTest {
    public static void main(String[] args) {
        List<String> bootstrapServers = Arrays.asList("10.251.136.98:27462");
        List<String> topics = Arrays.asList("ouyangTest3");
        String groupId = "test-group-multi-" + System.currentTimeMillis();
        
        System.out.println("=== 启动多消费者测试 ===");
        System.out.println("Group ID: " + groupId);
        System.out.println("Topics: " + topics);
        
        CountDownLatch latch = new CountDownLatch(2);
        
        // 启动第一个消费者
        Thread consumer1 = new Thread(() -> {
            try {
                System.out.println("\n=== 启动消费者 1 ===");
                ConsumerConfig config = new ConsumerConfig();
                config.setEnableAutoCommit(true);
                config.setAutoCommitIntervalMs(1000);
                
                KafkaLiteConsumer consumer = new KafkaLiteConsumerImpl(groupId, bootstrapServers, config);
                consumer.subscribe(topics);
                
                System.out.println("消费者 1 开始消费...");
                while (!Thread.currentThread().isInterrupted()) {
                    List<ConsumerRecord> records = consumer.poll(1000);
                    for (ConsumerRecord record : records) {
                        System.out.printf("[Consumer1] Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
                                record.getTopic(), record.getPartition(), record.getOffset(), record.getKey(), record.getValue());
                    }
                }
                
                // 等待关闭信号
                latch.countDown();
                latch.await(30, TimeUnit.SECONDS);
                
                System.out.println("消费者 1 正在关闭...");
                consumer.close();
                System.out.println("消费者 1 已关闭");
                
            } catch (Exception e) {
                System.err.println("消费者 1 异常: " + e.getMessage());
                e.printStackTrace();
                latch.countDown();
            }
        });
        
        // 启动第二个消费者
        Thread consumer2 = new Thread(() -> {
            try {
                // 等待一段时间再启动第二个消费者
                Thread.sleep(2000);
                
                System.out.println("\n=== 启动消费者 2 ===");
                ConsumerConfig config = new ConsumerConfig();
                config.setEnableAutoCommit(true);
                config.setAutoCommitIntervalMs(1000);
                
                KafkaLiteConsumer consumer = new KafkaLiteConsumerImpl(groupId, bootstrapServers, config);
                consumer.subscribe(topics);
                
                System.out.println("消费者 2 开始消费...");
                while (!Thread.currentThread().isInterrupted()) {
                    List<ConsumerRecord> records = consumer.poll(1000);
                    for (ConsumerRecord record : records) {
                        System.out.printf("[Consumer2] Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
                                record.getTopic(), record.getPartition(), record.getOffset(), record.getKey(), record.getValue());
                    }
                }
                
                // 等待关闭信号
                latch.countDown();
                latch.await(30, TimeUnit.SECONDS);
                
                System.out.println("消费者 2 正在关闭...");
                consumer.close();
                System.out.println("消费者 2 已关闭");
                
            } catch (Exception e) {
                System.err.println("消费者 2 异常: " + e.getMessage());
                e.printStackTrace();
                latch.countDown();
            }
        });
        
        // 启动消费者线程
        consumer1.start();
        consumer2.start();
        
        // 等待一段时间后关闭
        try {
            Thread.sleep(30000); // 运行30秒
            System.out.println("\n=== 测试完成，正在关闭消费者 ===");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
} 