package org.example.kafkalite.producer;

public interface KafkaLiteProducer {
    void send(ProducerRecord record);
    
    // 新增：同步发送方法
    void sendSync(ProducerRecord record) throws Exception;
    
    void flush();
    void close();
}
