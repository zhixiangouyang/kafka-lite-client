package org.example.kafkalite.producer;

public interface KafkaLiteProducer {
    void send(ProducerRecord record);
    void flush();
    void close();
}
