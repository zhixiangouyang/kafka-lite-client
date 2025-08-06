package org.example.kafkalite.consumer;

import org.example.kafkalite.dr.ClusterConfig;
import org.example.kafkalite.dr.FailoverManager;

import java.util.List;
import java.util.ArrayList;

/**
 * DR感知的Consumer配置类
 * 扩展基础ConsumerConfig，添加DR相关配置
 */
public class DrAwareConsumerConfig extends ConsumerConfig {
    
    // DR功能开关
    private boolean enableDrSupport = false;
    private boolean enableDynamicDnsResolution = true;
    
    // DNS配置
    private long dnsTtlMs = 60000; // 1分钟
    private long dnsRefreshIntervalMs = 300000; // 5分钟
    private int dnsMaxRetries = 3;
    private long dnsRetryDelayMs = 1000; // 1秒
    
    // 集群配置
    private List<ClusterConfig> clusters = new ArrayList<>();
    
    // 健康检查配置
    private long healthCheckIntervalMs = 30000; // 30秒
    private int healthCheckTimeoutMs = 5000; // 5秒
    private int healthCheckMaxFailures = 3;
    
    // 故障转移配置
    private FailoverManager.FailoverPolicy failoverPolicy = FailoverManager.FailoverPolicy.AUTO_WITH_FALLBACK;
    private long failoverCooldownMs = 300000; // 5分钟
    private long evaluationDelayMs = 10000; // 10秒
    private boolean enableAutoFailback = true;
    
    public DrAwareConsumerConfig(String groupId, List<String> bootstrapServers) {
        super(); // ConsumerConfig有默认构造函数
        // 存储这些参数供DR使用
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
    }
    
    // 添加字段存储构造参数
    private final String groupId;
    private final List<String> bootstrapServers;
    
    public String getGroupId() {
        return groupId;
    }
    
    public List<String> getBootstrapServers() {
        return new ArrayList<>(bootstrapServers);
    }
    
    // 静态builder方法
    public static Builder builder(String groupId, List<String> bootstrapServers) {
        return new Builder(groupId, bootstrapServers);
    }
    
    // Builder模式
    public static class Builder {
        private final DrAwareConsumerConfig config;
        
        public Builder(String groupId, List<String> bootstrapServers) {
            this.config = new DrAwareConsumerConfig(groupId, bootstrapServers);
        }
        
        public Builder enableDrSupport(boolean enable) {
            config.enableDrSupport = enable;
            return this;
        }
        
        public Builder enableDynamicDnsResolution(boolean enable) {
            config.enableDynamicDnsResolution = enable;
            return this;
        }
        
        public Builder dnsTtl(long ttlMs) {
            config.dnsTtlMs = ttlMs;
            return this;
        }
        
        public Builder dnsRefreshInterval(long intervalMs) {
            config.dnsRefreshIntervalMs = intervalMs;
            return this;
        }
        
        public Builder dnsMaxRetries(int maxRetries) {
            config.dnsMaxRetries = maxRetries;
            return this;
        }
        
        public Builder dnsRetryDelay(long delayMs) {
            config.dnsRetryDelayMs = delayMs;
            return this;
        }
        
        public Builder clusters(List<ClusterConfig> clusters) {
            config.clusters = new ArrayList<>(clusters);
            return this;
        }
        
        public Builder addCluster(ClusterConfig cluster) {
            config.clusters.add(cluster);
            return this;
        }
        
        public Builder healthCheckInterval(long intervalMs) {
            config.healthCheckIntervalMs = intervalMs;
            return this;
        }
        
        public Builder healthCheckTimeout(int timeoutMs) {
            config.healthCheckTimeoutMs = timeoutMs;
            return this;
        }
        
        public Builder healthCheckMaxFailures(int maxFailures) {
            config.healthCheckMaxFailures = maxFailures;
            return this;
        }
        
        public Builder failoverPolicy(FailoverManager.FailoverPolicy policy) {
            config.failoverPolicy = policy;
            return this;
        }
        
        public Builder failoverCooldown(long cooldownMs) {
            config.failoverCooldownMs = cooldownMs;
            return this;
        }
        
        public Builder evaluationDelay(long delayMs) {
            config.evaluationDelayMs = delayMs;
            return this;
        }
        
        public Builder enableAutoFailback(boolean enable) {
            config.enableAutoFailback = enable;
            return this;
        }
        
        public DrAwareConsumerConfig build() {
            return config;
        }
    }
    
    // Getters
    public boolean isEnableDrSupport() {
        return enableDrSupport;
    }
    
    public boolean isEnableDynamicDnsResolution() {
        return enableDynamicDnsResolution;
    }
    
    public long getDnsTtlMs() {
        return dnsTtlMs;
    }
    
    public long getDnsRefreshIntervalMs() {
        return dnsRefreshIntervalMs;
    }
    
    public int getDnsMaxRetries() {
        return dnsMaxRetries;
    }
    
    public long getDnsRetryDelayMs() {
        return dnsRetryDelayMs;
    }
    
    public List<ClusterConfig> getClusters() {
        return new ArrayList<>(clusters);
    }
    
    public long getHealthCheckIntervalMs() {
        return healthCheckIntervalMs;
    }
    
    public int getHealthCheckTimeoutMs() {
        return healthCheckTimeoutMs;
    }
    
    public int getHealthCheckMaxFailures() {
        return healthCheckMaxFailures;
    }
    
    public FailoverManager.FailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }
    
    public long getFailoverCooldownMs() {
        return failoverCooldownMs;
    }
    
    public long getEvaluationDelayMs() {
        return evaluationDelayMs;
    }
    
    public boolean isEnableAutoFailback() {
        return enableAutoFailback;
    }
    
    // 新增缺失的方法
    public long getFailoverEvaluationDelayMs() {
        return evaluationDelayMs;
    }
    
    public boolean isEnableClusterHealthCheck() {
        return true; // 默认启用健康检查
    }
    
    public long getClusterHealthCheckIntervalMs() {
        return healthCheckIntervalMs;
    }
    
    public int getClusterConnectionTimeoutMs() {
        return healthCheckTimeoutMs;
    }
    
    public int getClusterSocketTimeoutMs() {
        return healthCheckTimeoutMs;
    }
    
    // 继承自ConsumerConfig的方法
    public boolean isEnableAutoCommit() {
        return super.isEnableAutoCommit();
    }
    
    public long getAutoCommitIntervalMs() {
        return super.getAutoCommitIntervalMs();
    }
    
    public int getMaxPollRecords() {
        return super.getMaxPollRecords();
    }
    
    public int getFetchMaxBytes() {
        return super.getFetchMaxBytes();
    }
    
    public int getMaxRetries() {
        return super.getMaxRetries();
    }
    
    public long getRetryBackoffMs() {
        return super.getRetryBackoffMs();
    }
    
    public long getHeartbeatIntervalMs() {
        return super.getHeartbeatIntervalMs();
    }
    
    public boolean isEnablePeriodicMetadataRefresh() {
        return super.isEnablePeriodicMetadataRefresh();
    }
    
    public long getMetadataRefreshIntervalMs() {
        return super.getMetadataRefreshIntervalMs();
    }
    
    public int getMetadataConnectionPoolSize() {
        return super.getMetadataConnectionPoolSize();
    }
} 