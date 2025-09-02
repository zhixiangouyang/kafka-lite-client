package org.example.kafkalite.producer;

public class ProducerRecord {
    private final String topic;
    private final String key;
    private final String value;
    
    // 时间戳字段重构：分离不同阶段的时间
    private final long createTimestamp;      // 对象创建时间
    private volatile long queueTimestamp;    // 入队时间（异步发送时设置）
    private volatile long sendStartTimestamp; // 实际开始发送时间
    
    public ProducerRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.createTimestamp = System.currentTimeMillis();
        this.queueTimestamp = 0;
        this.sendStartTimestamp = 0;
    }
    
    // 用于测试或特殊情况的构造函数
    public ProducerRecord(String topic, String key, String value, long createTimestamp) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.createTimestamp = createTimestamp;
        this.queueTimestamp = 0;
        this.sendStartTimestamp = 0;
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
    
    // 时间戳相关方法
    public long getCreateTimestamp() {
        return createTimestamp;
    }
    
    public long getQueueTimestamp() {
        return queueTimestamp;
    }
    
    public void setQueueTimestamp(long queueTimestamp) {
        this.queueTimestamp = queueTimestamp;
    }
    
    public long getSendStartTimestamp() {
        return sendStartTimestamp;
    }
    
    public void setSendStartTimestamp(long sendStartTimestamp) {
        this.sendStartTimestamp = sendStartTimestamp;
    }
    
    // 兼容性方法，保持向后兼容
    @Deprecated
    public long getSendTimestamp() {
        return createTimestamp;
    }
    
    @Override
    public String toString() {
        return String.format("ProducerRecord{topic='%s', key='%s', value='%s', createTime=%d, queueTime=%d, sendStartTime=%d}", 
            topic, key, value, createTimestamp, queueTimestamp, sendStartTimestamp);
    }
} 