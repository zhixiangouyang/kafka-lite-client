package org.example.kafkalite.producer;

public class ProducerRecord {
    private final String topic;
    private final String key;
    private final String value;

    public ProducerRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
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
} 