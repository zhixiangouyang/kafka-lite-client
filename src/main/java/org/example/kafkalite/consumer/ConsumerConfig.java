package org.example.kafkalite.consumer;

public class ConsumerConfig {
    private boolean enableAutoCommit = true;
    private long autoCommitIntervalMs = 5000;
    private int maxPollRecords = 500;
    private int fetchMaxBytes = 1048576;
    private int fetchMaxWaitMs = 5000; // fetch请求最大等待时间，默认5秒
    private int maxRetries = 3;
    private long retryBackoffMs = 100;
    private long heartbeatIntervalMs = 3000;
    
    // 优化：元数据刷新相关配置
    private boolean enablePeriodicMetadataRefresh = true;
    private long metadataRefreshIntervalMs = 300000; // 5分钟 - 保持默认值，适合生产环境
    private int metadataConnectionPoolSize = 5; // 减少连接池大小，降低资源消耗
    
    // 新增：智能刷新配置
    private boolean enableSmartMetadataRefresh = true; // 启用智能刷新
    private long metadataRefreshOnErrorDelayMs = 30000; // 出错后30秒重试
    private int maxConsecutiveErrors = 3; // 最大连续错误次数
    
    // DR相关配置
    private Long dnsTtlMs = 300000L; // DNS TTL: 5分钟
    private Long healthCheckIntervalMs = 30000L; // 健康检查间隔: 30秒
    private Integer maxRetryCount = 3; // 最大重试次数

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

    public int getFetchMaxWaitMs() {
        return fetchMaxWaitMs;
    }

    public void setFetchMaxWaitMs(int fetchMaxWaitMs) {
        this.fetchMaxWaitMs = fetchMaxWaitMs;
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
    
    // 新增：智能元数据刷新相关getter/setter
    public boolean isEnableSmartMetadataRefresh() {
        return enableSmartMetadataRefresh;
    }

    public void setEnableSmartMetadataRefresh(boolean enableSmartMetadataRefresh) {
        this.enableSmartMetadataRefresh = enableSmartMetadataRefresh;
    }

    public long getMetadataRefreshOnErrorDelayMs() {
        return metadataRefreshOnErrorDelayMs;
    }

    public void setMetadataRefreshOnErrorDelayMs(long metadataRefreshOnErrorDelayMs) {
        this.metadataRefreshOnErrorDelayMs = metadataRefreshOnErrorDelayMs;
    }

    public int getMaxConsecutiveErrors() {
        return maxConsecutiveErrors;
    }

    public void setMaxConsecutiveErrors(int maxConsecutiveErrors) {
        this.maxConsecutiveErrors = maxConsecutiveErrors;
    }
    
    // DR相关的getter和setter
    public Long getDnsTtlMs() {
        return dnsTtlMs;
    }
    
    public void setDnsTtlMs(Long dnsTtlMs) {
        this.dnsTtlMs = dnsTtlMs;
    }
    
    public Long getHealthCheckIntervalMs() {
        return healthCheckIntervalMs;
    }
    
    public void setHealthCheckIntervalMs(Long healthCheckIntervalMs) {
        this.healthCheckIntervalMs = healthCheckIntervalMs;
    }
    
    public Integer getMaxRetryCount() {
        return maxRetryCount;
    }
    
    public void setMaxRetryCount(Integer maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }
} 