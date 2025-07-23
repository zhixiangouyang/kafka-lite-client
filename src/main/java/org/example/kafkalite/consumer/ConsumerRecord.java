package org.example.kafkalite.consumer;

public class ConsumerRecord {
    private final String topic;      // 消息所属的主题
    private final int partition;     // 消息所属的分区
    private final long offset;       // 消息所在分区的偏移量
    private final String key;        // 消息的key
    private final String value;      // 消息的value

    public ConsumerRecord(String topic, int partition, long offset, String key, String value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("ConsumerRecord={topiv='%s', partition=%d, offset%d, key='%s', value='%s'",
                topic, partition, offset, key, value);
    }
}
