package org.example.kafkalite.producer;

/**
 * 对外暴露的接口
 * send():发送消息核心方法
 * flush():确保缓冲区中消息都被发送
 * close():资源释放 关闭网络连接和线程池，防止内存泄漏
 */
public interface KafkaLiteProducer {
    void send(String topic, String key, String value);
    void flush();
    void close();
}
