package org.example.kafkalite.client;

import org.example.kafkalite.producer.HashPartitioner;
import org.example.kafkalite.producer.KafkaLiteProducer;
import org.example.kafkalite.producer.KafkaLiteProducerImpl;
import org.example.kafkalite.producer.ProducerRecord;
import org.example.kafkalite.producer.ProducerConfig;

import java.util.Arrays;

public class KafkaProducerTest {
    public static void main(String[] args) {
        // 1. 配置 broker 地址
        String broker = "localhost:9092";

        // 2. 创建生产者配置
        ProducerConfig config = new ProducerConfig.Builder()
            .batchSize(16384)
            .lingerMs(5)
            .maxRetries(3)
            .maxQueueSize(10000)
            .build();

        // 3. 创建生产者实例，选择分区策略
        KafkaLiteProducerImpl producer = new KafkaLiteProducerImpl(
                Arrays.asList(broker),
                new HashPartitioner(),
                config
        );

        try {
            // 4. 发送多条消息以便观察指标
            for (int i = 0; i < 10; i++) {
                ProducerRecord record = new ProducerRecord(
                    "kafka-lite-topic",
                    "key" + i, 
                    "hello kafka-lite " + i
                );
                producer.send(record);
                
                // 每发送一条消息就打印一次指标
                System.out.printf("After sending message %d:%n", i + 1);
                System.out.printf("Producer QPS: %.2f%n", producer.getProducerQPS());
                System.out.printf("Producer P99 Latency: %.2f ms%n%n", producer.getProducerP99Latency());
                
                // 稍微等待一下，便于观察
                Thread.sleep(1000);
            }
            
            // 等待消息发送完成
            producer.flush();
            
            // 打印最终指标
            System.out.println("Final metrics:");
            System.out.printf("Producer QPS: %.2f%n", producer.getProducerQPS());
            System.out.printf("Producer P99 Latency: %.2f ms%n", producer.getProducerP99Latency());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Test interrupted: " + e.getMessage());
        } finally {
            // 5. 关闭生产者
            producer.close();
        }
    }
}
