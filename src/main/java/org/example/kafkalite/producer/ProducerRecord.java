package org.example.kafkalite.producer;

public class ProducerRecord {
    private final String topic;
    private final String key;
    private final String value;
    private final long sendTimestamp; // 消息发送时间戳

    public ProducerRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.sendTimestamp = System.currentTimeMillis(); // 记录发送时间
    }
    
    // 用于测试或特殊情况的构造函数
    public ProducerRecord(String topic, String key, String value, long sendTimestamp) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.sendTimestamp = sendTimestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
    
    public long getSendTimestamp() {
        return sendTimestamp;
    }
} 