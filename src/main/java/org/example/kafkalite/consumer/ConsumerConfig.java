package org.example.kafkalite.consumer;

public class ConsumerConfig {
    private final boolean enableAutoCommit;   // 是否自动提交offset
    private final long autoCommitIntervalMs;  // 自动提交间隔
    private final int maxPollRecords;         // 单次最多拉取拉取消息条数
    private final int fetchMaxBytes;          // 单次最多拉取多大的消息
    private final long pollTimeoutMs;         // 拉取消息等待时间
    private final int maxRetries;             // 最大重试时间
    private final long retryBackoffMs;        // 重试间隔时间

    private ConsumerConfig(Builder builder) {
        this.enableAutoCommit = builder.enableAutoCommit;
        this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
        this.maxPollRecords = builder.maxPollRecords;
        this.fetchMaxBytes = builder.fetchMaxBytes;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.maxRetries = builder.maxRetries;
        this.retryBackoffMs = builder.retryBackoffMs;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public long getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public static class Builder {
        private boolean enableAutoCommit = false;         // 默认不自动提交
        private long autoCommitIntervalMs = 5000;         // 默认5s
        private int maxPollRecords = 500;                 // 默认最多拉500条
        private int fetchMaxBytes = 1024 * 1024;          // 默认1MB
        private long pollTimeoutMs = 1000;
        private int maxRetries = 3;
        private long retryBackoffMs = 1000;

        public Builder enableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder autoCommitIntervalMs(long autoCommitIntervalMs) {
            this.autoCommitIntervalMs = autoCommitIntervalMs;
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public Builder fetchMaxBytes(int fetchMaxBytes) {
            this.fetchMaxBytes = fetchMaxBytes;
            return this;
        }

        public Builder pollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
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

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
} 