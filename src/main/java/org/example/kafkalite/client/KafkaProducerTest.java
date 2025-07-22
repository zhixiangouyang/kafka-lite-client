package org.example.kafkalite.client;

import org.example.kafkalite.producer.HashPartitioner;
import org.example.kafkalite.producer.KafkaLiteProducer;
import org.example.kafkalite.producer.KafkaLiteProducerImpl;

import java.util.Arrays;

public class KafkaProducerTest {
    public static void main(String[] args) {
        // 1. 配置 broker 地址
        String broker = "localhost:9092";

        // 2. 创建生产者实例，选择分区策略
        KafkaLiteProducer producer = new KafkaLiteProducerImpl(
                Arrays.asList(broker),
                new HashPartitioner(),
                1
        );

        // 3. 发送消息
        producer.send("kafka-lite-topic", "myKey", "Hello KafkaLite!");
    }
}
