package org.example.kafkalite.consumer;

import org.example.kafkalite.dr.ClusterConfig;
import org.example.kafkalite.dr.ClusterHealthChecker;
import org.example.kafkalite.dr.FailoverManager;
import org.example.kafkalite.network.DynamicDnsResolver;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 支持DR的Kafka Consumer实现
 * 
 * 功能：
 * 1. 动态DNS解析，解决域名切换问题
 * 2. 多集群健康检查
 * 3. 自动故障转移
 * 4. Offset跨集群同步（可选）
 */
public class DrAwareKafkaConsumer implements KafkaLiteConsumer, FailoverManager.FailoverListener {
    
    private final String groupId;
    private final DrAwareConsumerConfig drConfig;
    
    // DR组件
    private final DynamicDnsResolver dnsResolver;
    private final ClusterHealthChecker healthChecker;
    private final FailoverManager failoverManager;
    
    // 当前活跃的Consumer实例
    private final AtomicReference<KafkaLiteConsumerImpl> activeConsumer = new AtomicReference<>();
    private final AtomicReference<ClusterConfig> activeCluster = new AtomicReference<>();
    
    // 状态管理
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private volatile List<String> subscribedTopics;
    
    public DrAwareKafkaConsumer(String groupId, DrAwareConsumerConfig config) {
        this.groupId = groupId;
        this.drConfig = config;
        
        if (config.isEnableDrSupport()) {
            // 初始化DNS解析器
            this.dnsResolver = config.isEnableDynamicDnsResolution() ? 
                new DynamicDnsResolver(
                    config.getDnsTtlMs(),
                    config.getDnsRefreshIntervalMs(),
                    config.getDnsMaxRetries(),
                    config.getDnsRetryDelayMs()
                ) : null;
            
            // 初始化故障转移管理器
            this.failoverManager = new FailoverManager(
                config.getClusters(),
                dnsResolver,
                this,
                config.getFailoverPolicy(),
                config.getFailoverCooldownMs(),
                config.getFailoverEvaluationDelayMs(),
                config.isEnableAutoFailback()
            );
            
            // 初始化健康检查器
            this.healthChecker = config.isEnableClusterHealthCheck() ?
                new ClusterHealthChecker(
                    config.getClusters(),
                    dnsResolver,
                    failoverManager,
                    config.getHealthCheckIntervalMs(),
                    config.getHealthCheckTimeoutMs(),
                    config.getHealthCheckTimeoutMs()
                ) : null;
            
            System.out.printf("[DrAwareKafkaConsumer] DR功能已启用: groupId=%s, 集群数=%d\n", 
                groupId, config.getClusters().size());
                
        } else {
            // 不启用DR功能，使用简化模式
            this.dnsResolver = null;
            this.healthChecker = null;
            this.failoverManager = null;
            
            System.out.printf("[DrAwareKafkaConsumer] DR功能未启用，使用单集群模式: groupId=%s\n", groupId);
        }
    }
    
    @Override
    public void subscribe(List<String> topics) {
        this.subscribedTopics = topics;
        
        if (drConfig.isEnableDrSupport()) {
            initializeDrMode(topics);
        } else {
            initializeSingleClusterMode(topics);
        }
        
        isInitialized.set(true);
        System.out.printf("[DrAwareKafkaConsumer] 订阅完成: topics=%s\n", topics);
    }
    
    /**
     * 初始化DR模式
     */
    private void initializeDrMode(List<String> topics) {
        // 启动健康检查
        if (healthChecker != null) {
            healthChecker.start();
        }
        
        // 获取当前活跃集群
        ClusterConfig initialCluster = failoverManager.getActiveCluster();
        if (initialCluster == null) {
            throw new RuntimeException("无可用的集群进行初始化");
        }
        
        // 创建并初始化Consumer
        createConsumerForCluster(initialCluster, topics);
        
        System.out.printf("[DrAwareKafkaConsumer] DR模式初始化完成: 活跃集群=%s\n", 
            initialCluster.getName());
    }
    
    /**
     * 初始化单集群模式
     */
    private void initializeSingleClusterMode(List<String> topics) {
        if (drConfig.getClusters().isEmpty()) {
            throw new RuntimeException("未配置任何集群");
        }
        
        ClusterConfig singleCluster = drConfig.getClusters().get(0);
        createConsumerForCluster(singleCluster, topics);
        
        System.out.printf("[DrAwareKafkaConsumer] 单集群模式初始化完成: 集群=%s\n", 
            singleCluster.getName());
    }
    
    /**
     * 为指定集群创建Consumer
     */
    private void createConsumerForCluster(ClusterConfig cluster, List<String> topics) {
        try {
            List<String> bootstrapServers = cluster.getBootstrapServers();
            
            // 如果启用了DNS解析，获取解析后的地址
            if (dnsResolver != null) {
                bootstrapServers = dnsResolver.resolveBootstrapServers(bootstrapServers);
            }
            
            // 创建新的Consumer实例
            ConsumerConfig baseConfig = createBaseConfig();
            KafkaLiteConsumerImpl newConsumer = new KafkaLiteConsumerImpl(groupId, bootstrapServers, baseConfig);
            
            // 订阅topics
            newConsumer.subscribe(topics);
            
            // 更新引用
            activeConsumer.set(newConsumer);
            activeCluster.set(cluster);
            
            System.out.printf("[DrAwareKafkaConsumer] 为集群创建Consumer: %s, servers=%s\n", 
                cluster.getName(), bootstrapServers);
                
        } catch (Exception e) {
            throw new RuntimeException("创建Consumer失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建基础配置
     */
    private ConsumerConfig createBaseConfig() {
        ConsumerConfig baseConfig = new ConsumerConfig();
        baseConfig.setEnableAutoCommit(drConfig.isEnableAutoCommit());
        baseConfig.setAutoCommitIntervalMs(drConfig.getAutoCommitIntervalMs());
        baseConfig.setMaxPollRecords(drConfig.getMaxPollRecords());
        baseConfig.setFetchMaxBytes(drConfig.getFetchMaxBytes());
        baseConfig.setMaxRetries(drConfig.getMaxRetries());
        baseConfig.setRetryBackoffMs(drConfig.getRetryBackoffMs());
        baseConfig.setHeartbeatIntervalMs(drConfig.getHeartbeatIntervalMs());
        baseConfig.setEnablePeriodicMetadataRefresh(drConfig.isEnablePeriodicMetadataRefresh());
        baseConfig.setMetadataRefreshIntervalMs(drConfig.getMetadataRefreshIntervalMs());
        baseConfig.setMetadataConnectionPoolSize(drConfig.getMetadataConnectionPoolSize());
        return baseConfig;
    }
    
    @Override
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (isClosed.get()) {
            throw new IllegalStateException("Consumer已关闭");
        }
        
        if (!isInitialized.get()) {
            throw new IllegalStateException("Consumer未初始化，请先调用subscribe()");
        }
        
        KafkaLiteConsumerImpl consumer = activeConsumer.get();
        if (consumer == null) {
            throw new IllegalStateException("无活跃的Consumer实例");
        }
        
        try {
            return consumer.poll(timeoutMs);
        } catch (Exception e) {
            // 如果启用了DR，将异常报告给故障转移管理器
            if (drConfig.isEnableDrSupport() && failoverManager != null) {
                ClusterConfig current = activeCluster.get();
                if (current != null) {
                    current.recordFailure("Poll异常: " + e.getMessage());
                }
            }
            throw e;
        }
    }
    
    @Override
    public void commitSync() {
        KafkaLiteConsumerImpl consumer = activeConsumer.get();
        if (consumer != null) {
            consumer.commitSync();
        }
    }
    
    @Override
    public void commitAsync() {
        KafkaLiteConsumerImpl consumer = activeConsumer.get();
        if (consumer != null) {
            consumer.commitAsync();
        }
    }
    
    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            System.out.printf("[DrAwareKafkaConsumer] 开始关闭Consumer: groupId=%s\n", groupId);
            
            // 关闭当前活跃的Consumer
            KafkaLiteConsumerImpl consumer = activeConsumer.get();
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.printf("[DrAwareKafkaConsumer] 关闭Consumer异常: %s\n", e.getMessage());
                }
            }
            
            // 关闭DR组件
            if (healthChecker != null) {
                healthChecker.shutdown();
            }
            
            if (dnsResolver != null) {
                dnsResolver.shutdown();
            }
            
            System.out.printf("[DrAwareKafkaConsumer] Consumer关闭完成: groupId=%s\n", groupId);
        }
    }
    
    // 实现FailoverListener接口
    
    @Override
    public void onFailoverStarted(ClusterConfig fromCluster, ClusterConfig toCluster, String reason) {
        System.out.printf("[DrAwareKafkaConsumer] 故障转移开始: %s -> %s, 原因: %s\n", 
            fromCluster != null ? fromCluster.getName() : "无", 
            toCluster.getName(), reason);
    }
    
    @Override
    public void onFailoverCompleted(ClusterConfig fromCluster, ClusterConfig toCluster, boolean success) {
        System.out.printf("[DrAwareKafkaConsumer] 故障转移完成: %s -> %s, 成功: %s\n", 
            fromCluster != null ? fromCluster.getName() : "无", 
            toCluster.getName(), success);
    }
    
    @Override
    public void onFailoverFailed(ClusterConfig fromCluster, ClusterConfig toCluster, String error) {
        System.err.printf("[DrAwareKafkaConsumer] 故障转移失败: %s -> %s, 错误: %s\n", 
            fromCluster != null ? fromCluster.getName() : "无", 
            toCluster.getName(), error);
    }
    
    @Override
    public void beforeClusterSwitch(ClusterConfig fromCluster, ClusterConfig toCluster) {
        System.out.printf("[DrAwareKafkaConsumer] 准备切换集群: %s -> %s\n", 
            fromCluster != null ? fromCluster.getName() : "无", 
            toCluster.getName());
        
        // 提交当前Consumer的offset
        try {
            KafkaLiteConsumerImpl currentConsumer = activeConsumer.get();
            if (currentConsumer != null) {
                currentConsumer.commitSync();
                System.out.printf("[DrAwareKafkaConsumer] 切换前offset提交成功\n");
            }
        } catch (Exception e) {
            System.err.printf("[DrAwareKafkaConsumer] 切换前offset提交失败: %s\n", e.getMessage());
        }
    }
    
    @Override
    public void afterClusterSwitch(ClusterConfig fromCluster, ClusterConfig toCluster) {
        System.out.printf("[DrAwareKafkaConsumer] 集群切换后处理: %s -> %s\n", 
            fromCluster != null ? fromCluster.getName() : "无", 
            toCluster.getName());
        
        try {
            // 关闭旧的Consumer
            KafkaLiteConsumerImpl oldConsumer = activeConsumer.get();
            if (oldConsumer != null) {
                oldConsumer.close();
            }
            
            // 为新集群创建Consumer
            if (subscribedTopics != null) {
                createConsumerForCluster(toCluster, subscribedTopics);
                System.out.printf("[DrAwareKafkaConsumer] 新集群Consumer创建成功: %s\n", toCluster.getName());
            }
            
        } catch (Exception e) {
            System.err.printf("[DrAwareKafkaConsumer] 集群切换后处理失败: %s\n", e.getMessage());
            throw new RuntimeException("集群切换失败", e);
        }
    }
    
    /**
     * 手动触发故障转移
     */
    public boolean manualFailover(String targetClusterId, String reason) {
        if (!drConfig.isEnableDrSupport() || failoverManager == null) {
            System.err.printf("[DrAwareKafkaConsumer] DR功能未启用，无法执行手动故障转移\n");
            return false;
        }
        
        return failoverManager.manualFailover(targetClusterId, reason);
    }
    
    /**
     * 获取当前活跃集群信息
     */
    public ClusterConfig getActiveCluster() {
        return activeCluster.get();
    }
    
    /**
     * 获取故障转移统计信息
     */
    public void printDrStats() {
        if (drConfig.isEnableDrSupport()) {
            if (failoverManager != null) {
                failoverManager.printFailoverStats();
            }
            if (healthChecker != null) {
                healthChecker.printHealthStats();
            }
            if (dnsResolver != null) {
                dnsResolver.printStats();
            }
        } else {
            System.out.printf("[DrAwareKafkaConsumer] DR功能未启用\n");
        }
    }
    
    /**
     * 强制刷新DNS缓存
     */
    public void refreshDnsCache() {
        if (dnsResolver != null) {
            dnsResolver.forceRefreshAll();
        }
    }
    
    /**
     * 手动触发健康检查
     */
    public void triggerHealthCheck() {
        if (healthChecker != null) {
            healthChecker.checkAllNow();
        }
    }
} 