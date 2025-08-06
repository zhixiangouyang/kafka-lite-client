package org.example.kafkalite.dr;

import java.util.List;
import java.util.Objects;

/**
 * Kafka集群配置
 * 支持DR场景下的多集群管理
 */
public class ClusterConfig {
    
    public enum ClusterType {
        PRIMARY,   // 主集群
        SECONDARY, // 备用集群
        STANDBY    // 备份集群
    }
    
    public enum ClusterState {
        ACTIVE,     // 活跃状态
        INACTIVE,   // 非活跃状态
        UNREACHABLE // 不可达状态
    }
    
    private final String clusterId;
    private final String name;
    private final ClusterType type;
    private final int priority; // 优先级，数字越小优先级越高
    private final List<String> bootstrapServers;
    
    // DR相关配置
    private final boolean enableDr;
    private final long healthCheckIntervalMs;
    private final long failoverTimeoutMs;
    private final int maxConsecutiveFailures;
    
    // 地理位置信息
    private final String region;
    private final String datacenter;
    
    // 运行时状态
    private volatile ClusterState state;
    private volatile long lastHealthCheckTime;
    private volatile int consecutiveFailures;
    private volatile String lastError;
    
    private ClusterConfig(Builder builder) {
        this.clusterId = builder.clusterId;
        this.name = builder.name;
        this.type = builder.type;
        this.priority = builder.priority;
        this.bootstrapServers = builder.bootstrapServers;
        this.enableDr = builder.enableDr;
        this.healthCheckIntervalMs = builder.healthCheckIntervalMs;
        this.failoverTimeoutMs = builder.failoverTimeoutMs;
        this.maxConsecutiveFailures = builder.maxConsecutiveFailures;
        this.region = builder.region;
        this.datacenter = builder.datacenter;
        
        // 初始状态
        this.state = ClusterState.INACTIVE;
        this.lastHealthCheckTime = 0;
        this.consecutiveFailures = 0;
        this.lastError = null;
    }
    
    // Getters
    public String getClusterId() {
        return clusterId;
    }
    
    public String getName() {
        return name;
    }
    
    public ClusterType getType() {
        return type;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }
    
    public boolean isEnableDr() {
        return enableDr;
    }
    
    public long getHealthCheckIntervalMs() {
        return healthCheckIntervalMs;
    }
    
    public long getFailoverTimeoutMs() {
        return failoverTimeoutMs;
    }
    
    public int getMaxConsecutiveFailures() {
        return maxConsecutiveFailures;
    }
    
    public String getRegion() {
        return region;
    }
    
    public String getDatacenter() {
        return datacenter;
    }
    
    public ClusterState getState() {
        return state;
    }
    
    public long getLastHealthCheckTime() {
        return lastHealthCheckTime;
    }
    
    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }
    
    public String getLastError() {
        return lastError;
    }
    
    // 状态管理方法
    public void setState(ClusterState state) {
        ClusterState oldState = this.state;
        this.state = state;
        this.lastHealthCheckTime = System.currentTimeMillis();
        
        if (state == ClusterState.ACTIVE) {
            this.consecutiveFailures = 0;
            this.lastError = null;
        }
        
        System.out.printf("[ClusterConfig] 集群状态变化: %s (%s) %s -> %s\n", 
            name, clusterId, oldState, state);
    }
    
    public void recordFailure(String error) {
        this.consecutiveFailures++;
        this.lastError = error;
        this.lastHealthCheckTime = System.currentTimeMillis();
        
        System.out.printf("[ClusterConfig] 集群故障记录: %s (%s) 连续失败=%d, 错误=%s\n", 
            name, clusterId, consecutiveFailures, error);
        
        // 如果连续失败次数超过阈值，标记为不可达
        if (consecutiveFailures >= maxConsecutiveFailures) {
            setState(ClusterState.UNREACHABLE);
        }
    }
    
    public void recordSuccess() {
        this.consecutiveFailures = 0;
        this.lastError = null;
        this.lastHealthCheckTime = System.currentTimeMillis();
        
        if (state != ClusterState.ACTIVE) {
            setState(ClusterState.ACTIVE);
        }
    }
    
    public boolean isHealthy() {
        return state == ClusterState.ACTIVE && consecutiveFailures == 0;
    }
    
    public boolean shouldFailover() {
        return state == ClusterState.UNREACHABLE || 
               consecutiveFailures >= maxConsecutiveFailures;
    }
    
    public boolean isPrimary() {
        return type == ClusterType.PRIMARY;
    }
    
    public boolean isSecondary() {
        return type == ClusterType.SECONDARY;
    }
    
    public boolean needsHealthCheck() {
        return System.currentTimeMillis() - lastHealthCheckTime >= healthCheckIntervalMs;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterConfig that = (ClusterConfig) o;
        return Objects.equals(clusterId, that.clusterId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }
    
    @Override
    public String toString() {
        return String.format("ClusterConfig{id='%s', name='%s', type=%s, priority=%d, state=%s, servers=%s, region='%s'}",
            clusterId, name, type, priority, state, bootstrapServers, region);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String clusterId;
        private String name;
        private ClusterType type = ClusterType.PRIMARY;
        private int priority = 0;
        private List<String> bootstrapServers;
        
        // DR配置默认值
        private boolean enableDr = true;
        private long healthCheckIntervalMs = 30000; // 30秒
        private long failoverTimeoutMs = 60000;     // 60秒
        private int maxConsecutiveFailures = 3;
        
        private String region = "default";
        private String datacenter = "default";
        
        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder type(ClusterType type) {
            this.type = type;
            return this;
        }
        
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }
        
        public Builder bootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }
        
        public Builder enableDr(boolean enableDr) {
            this.enableDr = enableDr;
            return this;
        }
        
        public Builder healthCheckIntervalMs(long healthCheckIntervalMs) {
            this.healthCheckIntervalMs = healthCheckIntervalMs;
            return this;
        }
        
        public Builder failoverTimeoutMs(long failoverTimeoutMs) {
            this.failoverTimeoutMs = failoverTimeoutMs;
            return this;
        }
        
        public Builder maxConsecutiveFailures(int maxConsecutiveFailures) {
            this.maxConsecutiveFailures = maxConsecutiveFailures;
            return this;
        }
        
        public Builder region(String region) {
            this.region = region;
            return this;
        }
        
        public Builder datacenter(String datacenter) {
            this.datacenter = datacenter;
            return this;
        }
        
        public ClusterConfig build() {
            if (clusterId == null || clusterId.isEmpty()) {
                throw new IllegalArgumentException("clusterId不能为空");
            }
            if (name == null || name.isEmpty()) {
                this.name = clusterId; // 默认使用clusterId作为name
            }
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrapServers不能为空");
            }
            
            return new ClusterConfig(this);
        }
    }
} 