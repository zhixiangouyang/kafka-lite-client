package org.example.kafkalite.consumer;

import java.util.List;

public interface KafkaLiteConsumer {

    // 订阅topic
    void subscribe(List<String> topics);

    // 拉取消息
    List<ConsumerRecord> poll(long timeoutMs);

    // 同步提交offset
    void commitSync();

    // 异步提交offset
    void commitAsync();

    // 关闭
    void close();
}
