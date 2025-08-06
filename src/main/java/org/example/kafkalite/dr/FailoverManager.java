package org.example.kafkalite.dr;

import org.example.kafkalite.network.DynamicDnsResolver;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.Comparator;

/**
 * 故障转移管理器
 * 
 * 功能：
 * 1. 监控集群健康状态
 * 2. 决策是否需要故障转移
 * 3. 执行集群切换操作
 * 4. 管理offset同步
 * 5. 处理DNS缓存刷新
 */
public class FailoverManager implements ClusterHealthChecker.HealthCheckListener {
    
    public interface FailoverListener {
        void onFailoverStarted(ClusterConfig fromCluster, ClusterConfig toCluster, String reason);
        void onFailoverCompleted(ClusterConfig fromCluster, ClusterConfig toCluster, boolean success);
        void onFailoverFailed(ClusterConfig fromCluster, ClusterConfig toCluster, String error);
        void beforeClusterSwitch(ClusterConfig fromCluster, ClusterConfig toCluster);
        void afterClusterSwitch(ClusterConfig fromCluster, ClusterConfig toCluster);
    }
    
    public enum FailoverPolicy {
        MANUAL_ONLY,        // 仅手动故障转移
        AUTO_TO_SECONDARY,  // 自动切换到备用集群
        AUTO_WITH_FALLBACK  // 自动切换，支持多级fallback
    }
    
    public enum FailoverState {
        STABLE,         // 稳定状态
        EVALUATING,     // 评估是否需要故障转移
        FAILING_OVER,   // 正在执行故障转移
        FAILED          // 故障转移失败
    }
    
    private final List<ClusterConfig> clusters;
    private final DynamicDnsResolver dnsResolver;
    private final FailoverListener listener;
    private final FailoverPolicy policy;
    
    // 当前活跃集群
    private final AtomicReference<ClusterConfig> activeCluster = new AtomicReference<>();
    
    // 故障转移状态
    private final AtomicReference<FailoverState> failoverState = new AtomicReference<>(FailoverState.STABLE);
    private final ReentrantLock failoverLock = new ReentrantLock();
    
    // 配置参数
    private final long failoverCooldownMs;
    private final long evaluationDelayMs;
    private final boolean enableAutoFailback;
    
    // 统计信息
    private final AtomicLong totalFailovers = new AtomicLong(0);
    private final AtomicLong successfulFailovers = new AtomicLong(0);
    private final AtomicLong failedFailovers = new AtomicLong(0);
    private final Map<String, Long> lastFailoverTime = new ConcurrentHashMap<>();
    
    // 运行时状态
    private volatile long lastFailoverAttempt = 0;
    private volatile String lastFailoverReason = null;
    
    public FailoverManager(List<ClusterConfig> clusters, 
                          DynamicDnsResolver dnsResolver,
                          FailoverListener listener) {
        this(clusters, dnsResolver, listener, FailoverPolicy.AUTO_WITH_FALLBACK, 300000, 10000, true);
    }
    
    public FailoverManager(List<ClusterConfig> clusters, 
                          DynamicDnsResolver dnsResolver,
                          FailoverListener listener,
                          FailoverPolicy policy,
                          long failoverCooldownMs,
                          long evaluationDelayMs,
                          boolean enableAutoFailback) {
        this.clusters = clusters;
        this.dnsResolver = dnsResolver;
        this.listener = listener;
        this.policy = policy;
        this.failoverCooldownMs = failoverCooldownMs;
        this.evaluationDelayMs = evaluationDelayMs;
        this.enableAutoFailback = enableAutoFailback;
        
        // 初始化时选择优先级最高的健康集群作为活跃集群
        initializeActiveCluster();
        
        System.out.printf("[FailoverManager] 初始化完成: 策略=%s, 冷却时间=%dms, 活跃集群=%s\n", 
            policy, failoverCooldownMs, 
            activeCluster.get() != null ? activeCluster.get().getName() : "无");
    }
    
    /**
     * 初始化活跃集群
     */
    private void initializeActiveCluster() {
        // 按优先级排序，选择优先级最高的集群
        Optional<ClusterConfig> primaryCluster = clusters.stream()
            .filter(c -> c.getType() == ClusterConfig.ClusterType.PRIMARY)
            .min(Comparator.comparingInt(ClusterConfig::getPriority));
            
        if (primaryCluster.isPresent()) {
            ClusterConfig selected = primaryCluster.get();
            selected.setState(ClusterConfig.ClusterState.ACTIVE);
            activeCluster.set(selected);
            System.out.printf("[FailoverManager] 选择主集群作为活跃集群: %s\n", selected.getName());
        } else {
            // 如果没有主集群，选择优先级最高的集群
            Optional<ClusterConfig> fallbackCluster = clusters.stream()
                .min(Comparator.comparingInt(ClusterConfig::getPriority));
                
            if (fallbackCluster.isPresent()) {
                ClusterConfig selected = fallbackCluster.get();
                selected.setState(ClusterConfig.ClusterState.ACTIVE);
                activeCluster.set(selected);
                System.out.printf("[FailoverManager] 选择fallback集群作为活跃集群: %s\n", selected.getName());
            }
        }
    }
    
    /**
     * 获取当前活跃集群
     */
    public ClusterConfig getActiveCluster() {
        return activeCluster.get();
    }
    
    /**
     * 获取当前故障转移状态
     */
    public FailoverState getFailoverState() {
        return failoverState.get();
    }
    
    /**
     * 手动故障转移到指定集群
     */
    public boolean manualFailover(String targetClusterId, String reason) {
        ClusterConfig targetCluster = findClusterById(targetClusterId);
        if (targetCluster == null) {
            System.err.printf("[FailoverManager] 手动故障转移失败: 未找到目标集群 %s\n", targetClusterId);
            return false;
        }
        
        System.out.printf("[FailoverManager] 开始手动故障转移: %s -> %s, 原因: %s\n", 
            activeCluster.get() != null ? activeCluster.get().getName() : "无", 
            targetCluster.getName(), reason);
        
        return performFailover(targetCluster, "手动故障转移: " + reason);
    }
    
    /**
     * 执行故障转移
     */
    private boolean performFailover(ClusterConfig targetCluster, String reason) {
        if (!failoverLock.tryLock()) {
            System.err.printf("[FailoverManager] 故障转移正在进行中，跳过新的转移请求\n");
            return false;
        }
        
        try {
            // 检查冷却时间
            if (isInCooldownPeriod()) {
                System.err.printf("[FailoverManager] 故障转移在冷却期内，跳过转移\n");
                return false;
            }
            
            ClusterConfig currentCluster = activeCluster.get();
            failoverState.set(FailoverState.FAILING_OVER);
            lastFailoverAttempt = System.currentTimeMillis();
            lastFailoverReason = reason;
            totalFailovers.incrementAndGet();
            
            System.out.printf("[FailoverManager] 开始执行故障转移: %s -> %s\n", 
                currentCluster != null ? currentCluster.getName() : "无", 
                targetCluster.getName());
            
            try {
                // 通知监听器故障转移开始
                if (listener != null) {
                    listener.onFailoverStarted(currentCluster, targetCluster, reason);
                }
                
                // 1. 刷新DNS缓存，获取最新的集群地址
                refreshDnsForCluster(targetCluster);
                
                // 2. 执行集群切换前的准备工作
                if (listener != null) {
                    listener.beforeClusterSwitch(currentCluster, targetCluster);
                }
                
                // 3. 切换活跃集群
                if (currentCluster != null) {
                    currentCluster.setState(ClusterConfig.ClusterState.INACTIVE);
                }
                targetCluster.setState(ClusterConfig.ClusterState.ACTIVE);
                activeCluster.set(targetCluster);
                
                // 4. 执行集群切换后的清理工作
                if (listener != null) {
                    listener.afterClusterSwitch(currentCluster, targetCluster);
                }
                
                // 记录成功
                successfulFailovers.incrementAndGet();
                lastFailoverTime.put(targetCluster.getClusterId(), System.currentTimeMillis());
                failoverState.set(FailoverState.STABLE);
                
                System.out.printf("[FailoverManager] 故障转移成功: %s -> %s\n", 
                    currentCluster != null ? currentCluster.getName() : "无", 
                    targetCluster.getName());
                
                if (listener != null) {
                    listener.onFailoverCompleted(currentCluster, targetCluster, true);
                }
                
                return true;
                
            } catch (Exception e) {
                // 故障转移失败
                failedFailovers.incrementAndGet();
                failoverState.set(FailoverState.FAILED);
                
                String error = "故障转移执行失败: " + e.getMessage();
                System.err.printf("[FailoverManager] %s\n", error);
                
                if (listener != null) {
                    listener.onFailoverFailed(currentCluster, targetCluster, error);
                    listener.onFailoverCompleted(currentCluster, targetCluster, false);
                }
                
                return false;
            }
            
        } finally {
            failoverLock.unlock();
        }
    }
    
    /**
     * 为指定集群刷新DNS缓存
     */
    private void refreshDnsForCluster(ClusterConfig cluster) {
        System.out.printf("[FailoverManager] 刷新集群DNS缓存: %s\n", cluster.getName());
        
        for (String server : cluster.getBootstrapServers()) {
            try {
                String[] parts = server.split(":");
                String hostname = parts[0];
                dnsResolver.forceRefresh(hostname);
            } catch (Exception e) {
                System.err.printf("[FailoverManager] 刷新DNS失败: %s, 错误: %s\n", server, e.getMessage());
            }
        }
        
        // 等待DNS解析完成
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 检查是否在冷却期内
     */
    private boolean isInCooldownPeriod() {
        return System.currentTimeMillis() - lastFailoverAttempt < failoverCooldownMs;
    }
    
    /**
     * 根据ID查找集群
     */
    private ClusterConfig findClusterById(String clusterId) {
        return clusters.stream()
            .filter(c -> c.getClusterId().equals(clusterId))
            .findFirst()
            .orElse(null);
    }
    
    /**
     * 选择下一个可用的集群
     */
    private ClusterConfig selectNextAvailableCluster(ClusterConfig excludeCluster) {
        return clusters.stream()
            .filter(c -> !c.equals(excludeCluster))
            .filter(c -> c.getState() != ClusterConfig.ClusterState.UNREACHABLE)
            .min(Comparator.comparingInt(ClusterConfig::getPriority))
            .orElse(null);
    }
    
    // 实现HealthCheckListener接口
    
    @Override
    public void onClusterHealthChanged(ClusterConfig cluster, boolean isHealthy, String reason) {
        // 如果是当前活跃集群变为不健康，记录但不立即故障转移
        if (!isHealthy && cluster.equals(activeCluster.get())) {
            System.out.printf("[FailoverManager] 活跃集群健康状态变化: %s, 健康=%s, 原因=%s\n", 
                cluster.getName(), isHealthy, reason);
        }
    }
    
    @Override
    public void onClusterUnreachable(ClusterConfig cluster, String reason) {
        // 如果当前活跃集群变为不可达，触发自动故障转移
        if (cluster.equals(activeCluster.get()) && policy != FailoverPolicy.MANUAL_ONLY) {
            System.err.printf("[FailoverManager] 活跃集群不可达，触发自动故障转移: %s, 原因: %s\n", 
                cluster.getName(), reason);
            
            ClusterConfig nextCluster = selectNextAvailableCluster(cluster);
            if (nextCluster != null) {
                // 延迟一段时间再执行故障转移，避免网络抖动
                new Thread(() -> {
                    try {
                        Thread.sleep(evaluationDelayMs);
                        // 再次检查集群是否真的不可达
                        if (cluster.shouldFailover() && cluster.equals(activeCluster.get())) {
                            performFailover(nextCluster, "自动故障转移: " + reason);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "auto-failover-" + System.currentTimeMillis()).start();
            } else {
                System.err.printf("[FailoverManager] 无可用的备用集群进行故障转移\n");
            }
        }
    }
    
    @Override
    public void onClusterRecovered(ClusterConfig cluster) {
        // 如果是优先级更高的集群恢复，考虑自动failback
        if (enableAutoFailback && cluster.getPriority() < activeCluster.get().getPriority()) {
            System.out.printf("[FailoverManager] 高优先级集群恢复，考虑自动failback: %s\n", cluster.getName());
            
            // 延迟执行failback，确保集群真正稳定
            new Thread(() -> {
                try {
                    Thread.sleep(evaluationDelayMs * 2); // failback需要更长的等待时间
                    if (cluster.isHealthy() && cluster.getPriority() < activeCluster.get().getPriority()) {
                        performFailover(cluster, "自动failback: 高优先级集群恢复");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "auto-failback-" + System.currentTimeMillis()).start();
        }
    }
    
    /**
     * 获取故障转移统计信息
     */
    public void printFailoverStats() {
        System.out.printf("[FailoverManager] 故障转移统计:\n");
        System.out.printf("  总转移次数: %d\n", totalFailovers.get());
        System.out.printf("  成功次数: %d\n", successfulFailovers.get());
        System.out.printf("  失败次数: %d\n", failedFailovers.get());
        System.out.printf("  当前状态: %s\n", failoverState.get());
        System.out.printf("  活跃集群: %s\n", 
            activeCluster.get() != null ? activeCluster.get().getName() : "无");
        System.out.printf("  最后转移时间: %s\n", 
            lastFailoverAttempt > 0 ? new java.util.Date(lastFailoverAttempt).toString() : "从未转移");
        System.out.printf("  最后转移原因: %s\n", 
            lastFailoverReason != null ? lastFailoverReason : "无");
        System.out.printf("  冷却期剩余: %dms\n", 
            Math.max(0, failoverCooldownMs - (System.currentTimeMillis() - lastFailoverAttempt)));
    }
} 