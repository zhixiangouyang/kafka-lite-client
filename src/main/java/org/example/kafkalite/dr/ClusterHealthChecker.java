package org.example.kafkalite.dr;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.network.DynamicDnsResolver;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 集群健康检查器
 * 
 * 功能：
 * 1. 定期检查各集群的连通性
 * 2. 监控broker可用性
 * 3. 检测网络分区
 * 4. 为DR故障转移提供决策依据
 */
public class ClusterHealthChecker {
    
    public interface HealthCheckListener {
        void onClusterHealthChanged(ClusterConfig cluster, boolean isHealthy, String reason);
        void onClusterUnreachable(ClusterConfig cluster, String reason);
        void onClusterRecovered(ClusterConfig cluster);
    }
    
    private final List<ClusterConfig> clusters;
    private final DynamicDnsResolver dnsResolver;
    private final HealthCheckListener listener;
    
    // 健康检查配置
    private final long healthCheckIntervalMs;
    private final int connectionTimeoutMs;
    private final int socketTimeoutMs;
    
    // 线程池
    private final ScheduledExecutorService healthCheckExecutor;
    private volatile boolean isShutdown = false;
    
    // 统计信息
    private final AtomicLong totalHealthChecks = new AtomicLong(0);
    private final AtomicLong successfulChecks = new AtomicLong(0);
    private final AtomicLong failedChecks = new AtomicLong(0);
    private final Map<String, Long> lastSuccessfulCheck = new ConcurrentHashMap<>();
    
    public ClusterHealthChecker(List<ClusterConfig> clusters, 
                               DynamicDnsResolver dnsResolver,
                               HealthCheckListener listener) {
        this(clusters, dnsResolver, listener, 30000, 5000, 10000);
    }
    
    public ClusterHealthChecker(List<ClusterConfig> clusters, 
                               DynamicDnsResolver dnsResolver,
                               HealthCheckListener listener,
                               long healthCheckIntervalMs,
                               int connectionTimeoutMs,
                               int socketTimeoutMs) {
        this.clusters = clusters;
        this.dnsResolver = dnsResolver;
        this.listener = listener;
        this.healthCheckIntervalMs = healthCheckIntervalMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        
        this.healthCheckExecutor = Executors.newScheduledThreadPool(
            Math.max(2, clusters.size()), 
            r -> {
                Thread t = new Thread(r, "cluster-health-checker-" + System.currentTimeMillis());
                t.setDaemon(true);
                return t;
            }
        );
        
        System.out.printf("[ClusterHealthChecker] 初始化完成: 监控集群数=%d, 检查间隔=%dms\n", 
            clusters.size(), healthCheckIntervalMs);
    }
    
    /**
     * 启动健康检查
     */
    public void start() {
        if (isShutdown) {
            throw new IllegalStateException("健康检查器已关闭");
        }
        
        System.out.printf("[ClusterHealthChecker] 启动集群健康检查\n");
        
        // 为每个集群启动独立的健康检查任务
        for (ClusterConfig cluster : clusters) {
            healthCheckExecutor.scheduleWithFixedDelay(
                () -> performHealthCheck(cluster),
                0, // 立即开始
                cluster.getHealthCheckIntervalMs(),
                TimeUnit.MILLISECONDS
            );
            
            System.out.printf("[ClusterHealthChecker] 为集群 %s 启动健康检查，间隔=%dms\n", 
                cluster.getName(), cluster.getHealthCheckIntervalMs());
        }
        
        // 启动总体统计报告任务
        healthCheckExecutor.scheduleWithFixedDelay(
            this::printHealthStats,
            60000, // 1分钟后开始
            300000, // 每5分钟打印一次
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * 执行单个集群的健康检查
     */
    private void performHealthCheck(ClusterConfig cluster) {
        if (isShutdown || !cluster.isEnableDr()) {
            return;
        }
        
        totalHealthChecks.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        try {
            boolean isHealthy = checkClusterConnectivity(cluster);   // 初始化将集群的bootstrap域名转化为IP地址并验证连通性
            long checkDuration = System.currentTimeMillis() - startTime;
            
            if (isHealthy) {
                successfulChecks.incrementAndGet();
                lastSuccessfulCheck.put(cluster.getClusterId(), System.currentTimeMillis());
                
                boolean wasUnhealthy = !cluster.isHealthy();
                cluster.recordSuccess();
                
                System.out.printf("[HealthCheck] 集群健康: %s, 耗时=%dms\n", 
                    cluster.getName(), checkDuration);
                
                if (wasUnhealthy && listener != null) {
                    listener.onClusterRecovered(cluster);
                }
                
                if (listener != null) {
                    listener.onClusterHealthChanged(cluster, true, "连接正常");
                }
                
            } else {
                failedChecks.incrementAndGet();
                String error = String.format("连接失败，耗时=%dms", checkDuration);
                
                boolean wasHealthy = cluster.isHealthy();
                cluster.recordFailure(error);
                
                System.err.printf("[HealthCheck] 集群异常: %s, %s\n", cluster.getName(), error);
                
                if (listener != null) {
                    listener.onClusterHealthChanged(cluster, false, error);
                    
                    if (cluster.shouldFailover() && wasHealthy) {
                        listener.onClusterUnreachable(cluster, error);
                    }
                }
            }
            
        } catch (Exception e) {
            failedChecks.incrementAndGet();
            String error = "健康检查异常: " + e.getMessage();
            
            cluster.recordFailure(error);
            System.err.printf("[HealthCheck] 集群检查异常: %s, 错误: %s\n", cluster.getName(), error);
            
            if (listener != null) {
                listener.onClusterHealthChanged(cluster, false, error);
            }
        }
    }
    
    /**
     * 检查集群连通性
     */
    private boolean checkClusterConnectivity(ClusterConfig cluster) {
        List<String> bootstrapServers = cluster.getBootstrapServers();
        
        // 通过DNS解析获取最新的IP地址
        List<String> resolvedServers = dnsResolver.resolveBootstrapServers(bootstrapServers);
        
        // 尝试连接至少一个broker
        for (String server : resolvedServers) {
            if (testBrokerConnection(server)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 测试单个broker连接
     */
    private boolean testBrokerConnection(String brokerAddress) {
        try {
            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            // 简单的Socket连接测试
            try (java.net.Socket socket = new java.net.Socket()) {
                socket.connect(new java.net.InetSocketAddress(host, port), 5000);
                return socket.isConnected();
            }
            
        } catch (Exception e) {
            System.err.printf("[HealthCheck] Broker连接测试失败: %s, 错误: %s\n", 
                brokerAddress, e.getMessage());
            return false;
        }
    }
    
    /**
     * 手动触发指定集群的健康检查
     */
    public void checkNow(String clusterId) {
        ClusterConfig cluster = clusters.stream()
            .filter(c -> c.getClusterId().equals(clusterId))
            .findFirst()
            .orElse(null);
            
        if (cluster != null) {
            healthCheckExecutor.submit(() -> performHealthCheck(cluster));
            System.out.printf("[ClusterHealthChecker] 手动触发集群健康检查: %s\n", cluster.getName());
        } else {
            System.err.printf("[ClusterHealthChecker] 未找到集群: %s\n", clusterId);
        }
    }
    
    /**
     * 检查所有集群
     */
    public void checkAllNow() {
        System.out.printf("[ClusterHealthChecker] 手动触发所有集群健康检查\n");
        for (ClusterConfig cluster : clusters) {
            healthCheckExecutor.submit(() -> performHealthCheck(cluster));
        }
    }
    
    /**
     * 获取健康的集群列表
     */
    public List<ClusterConfig> getHealthyClusters() {
        return clusters.stream()
            .filter(ClusterConfig::isHealthy)
            .collect(Collectors.toList());
    }
    
    /**
     * 获取不健康的集群列表
     */
    public List<ClusterConfig> getUnhealthyClusters() {
        return clusters.stream()
            .filter(c -> !c.isHealthy())
            .collect(Collectors.toList());
    }
    
    /**
     * 打印健康检查统计信息
     */
    public void printHealthStats() {
        long total = totalHealthChecks.get();
        long success = successfulChecks.get();
        long failed = failedChecks.get();
        double successRate = total > 0 ? (double) success / total * 100 : 0;
        
        System.out.printf("[HealthCheck] 统计信息:\n");
        System.out.printf("  总检查次数: %d\n", total);
        System.out.printf("  成功次数: %d\n", success);
        System.out.printf("  失败次数: %d\n", failed);
        System.out.printf("  成功率: %.2f%%\n", successRate);
        
        System.out.printf("  集群状态:\n");
        for (ClusterConfig cluster : clusters) {
            Long lastSuccess = lastSuccessfulCheck.get(cluster.getClusterId());
            String lastSuccessStr = lastSuccess != null ? 
                String.format("%dms前", System.currentTimeMillis() - lastSuccess) : "从未成功";
                
            System.out.printf("    %s (%s): %s, 连续失败=%d, 最后成功=%s\n",
                cluster.getName(), cluster.getClusterId(), cluster.getState(),
                cluster.getConsecutiveFailures(), lastSuccessStr);
        }
    }
    
    /**
     * 关闭健康检查器
     */
    public void shutdown() {
        isShutdown = true;
        
        if (healthCheckExecutor != null && !healthCheckExecutor.isShutdown()) {
            System.out.printf("[ClusterHealthChecker] 正在关闭健康检查器\n");
            
            healthCheckExecutor.shutdown();
            try {
                if (!healthCheckExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    healthCheckExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                healthCheckExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        System.out.printf("[ClusterHealthChecker] 健康检查器已关闭\n");
    }
} 