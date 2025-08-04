package org.example.kafkalite.consumer;

public class ConsumerConfig {
    private boolean enableAutoCommit = true;
    private long autoCommitIntervalMs = 5000;
    private int maxPollRecords = 500;
    private int fetchMaxBytes = 1048576;
    private int maxRetries = 3;
    private long retryBackoffMs = 100;
    private long heartbeatIntervalMs = 3000;
    
    // 新增：定期元数据刷新相关配置
    private boolean enablePeriodicMetadataRefresh = true;
    private long metadataRefreshIntervalMs = 300000; // 5分钟
    private int metadataConnectionPoolSize = 10; // 元数据连接池大小

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public long getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public void setFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(long retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
        }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }
    
    // 新增：定期元数据刷新相关getter/setter
    public boolean isEnablePeriodicMetadataRefresh() {
        return enablePeriodicMetadataRefresh;
    }

    public void setEnablePeriodicMetadataRefresh(boolean enablePeriodicMetadataRefresh) {
        this.enablePeriodicMetadataRefresh = enablePeriodicMetadataRefresh;
    }

    public long getMetadataRefreshIntervalMs() {
        return metadataRefreshIntervalMs;
    }

    public void setMetadataRefreshIntervalMs(long metadataRefreshIntervalMs) {
        this.metadataRefreshIntervalMs = metadataRefreshIntervalMs;
    }

    public int getMetadataConnectionPoolSize() {
        return metadataConnectionPoolSize;
    }

    public void setMetadataConnectionPoolSize(int metadataConnectionPoolSize) {
        this.metadataConnectionPoolSize = metadataConnectionPoolSize;
    }
} 