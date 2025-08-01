package org.example.kafkalite.producer;

public class ProducerConfig {
    private final int batchSize;    // 批量发送大小，默认16kb
    private final long lingerMs;    // 等待时间，默认5ms
    private final int maxRetries;   // 最大重试次数，默认3次
    private final long retryBackoffMs;  // 重试间隔，默认100ms
    private final int maxQueueSize;     // 发送队列大小，默认10000
    private final int connectionPoolSize; // 新增，连接池大小，默认60
    private String compressionType = "none";

    private ProducerConfig(Builder builder) {
        this.batchSize = builder.batchSize;
        this.lingerMs = builder.lingerMs;
        this.maxRetries = builder.maxRetries;
        this.retryBackoffMs = builder.retryBackoffMs;
        this.maxQueueSize = builder.maxQueueSize;
        this.compressionType = builder.compressionType;
        this.connectionPoolSize = builder.connectionPoolSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getLingerMs() {
        return lingerMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public static class Builder {
        private int batchSize = 16384; // 16KB
        private long lingerMs = 5;
        private int maxRetries = 3;
        private long retryBackoffMs = 100;
        private int maxQueueSize = 10000;
        private String compressionType = "none";
        private int connectionPoolSize = 60; // 新增，默认60

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder lingerMs(long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoffMs(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        public Builder compressionType(String compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder connectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public ProducerConfig build() {
            return new ProducerConfig(this);
        }
    }
} 