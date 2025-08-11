package org.example.kafkalite.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * DNS感知的DR消费者
 * 自动响应外部DNS变化实现集群切换
 * 
 * 工作原理：
 * 1. 定期监控DNS解析结果变化
 * 2. 当DNS返回的IP列表发生变化时，自动重新分组集群
 * 3. 如果当前使用的集群IP被移除，自动切换到可用集群
 */
public class DnsAwareDrConsumer implements KafkaLiteConsumer {
    
    private final String domainWithPort;
    private final String groupId;
    private final ConsumerConfig config;
    
    // 当前状态
    private volatile DrAwareConsumerDecorator currentDrConsumer;
    private volatile List<String> subscribedTopics = new ArrayList<>();
    
    // DNS监控
    private final AtomicReference<Set<String>> lastResolvedIPs = new AtomicReference<>(new HashSet<>());
    private final ScheduledExecutorService dnsMonitor;
    private final long dnsCheckIntervalMs;
    
    // 集群状态
    private volatile String currentClusterSegment = null;
    private final AtomicBoolean isRebuilding = new AtomicBoolean(false);
    
    public DnsAwareDrConsumer(String domainWithPort, String groupId, ConsumerConfig config) {
        this.domainWithPort = domainWithPort;
        this.groupId = groupId;
        this.config = config;
        this.dnsCheckIntervalMs = config.getDnsTtlMs() != null ? config.getDnsTtlMs() / 6 : 30000; // 每30秒检查
        
        // 初始化DNS监控线程
        this.dnsMonitor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "DNS-Monitor");
            t.setDaemon(true);
            return t;
        });
        
        // 初始化DR消费者
        initializeDrConsumer();
        
        // 启动DNS监控
        startDnsMonitoring();
    }
    
    /**
     * 初始化DR消费者
     */
    private void initializeDrConsumer() {
        System.out.printf("[DnsAwareDrConsumer] 初始化DR消费者: %s\n", domainWithPort);
        
        try {
            // 解析当前DNS
            ClusterDiscovery.DiscoveryResult result = ClusterDiscovery.discoverClusters(domainWithPort, new ArrayList<>());
            Set<String> currentIPs = extractAllIPs(result);
            lastResolvedIPs.set(currentIPs);
            
            // 创建DR消费者
            ClusterDiscovery.ClusterInfo primary = result.getPrimaryCluster();
            ClusterDiscovery.ClusterInfo secondary = result.getSecondaryCluster();
            
            if (primary == null) {
                throw new RuntimeException("无法发现可用集群");
            }
            
            List<String> primaryServers = primary.getBrokers();
            List<String> secondaryServers = secondary != null ? secondary.getBrokers() : new ArrayList<>();
            
            currentDrConsumer = new DrAwareConsumerDecorator(primaryServers, secondaryServers, groupId, config);
            currentClusterSegment = getNetworkSegment(primaryServers.get(0).split(":")[0]);
            
            System.out.printf("[DnsAwareDrConsumer] 初始化完成: 主集群=%s, 备集群=%s, 当前网段=%s\n", 
                primaryServers, secondaryServers, currentClusterSegment);
                
        } catch (Exception e) {
            System.err.printf("[DnsAwareDrConsumer] 初始化失败: %s\n", e.getMessage());
            throw new RuntimeException("DR消费者初始化失败", e);
        }
    }
    
    /**
     * 启动DNS监控
     */
    private void startDnsMonitoring() {
        dnsMonitor.scheduleWithFixedDelay(() -> {
            try {
                checkDnsChanges();
            } catch (Exception e) {
                System.err.printf("[DnsAwareDrConsumer] DNS监控异常: %s\n", e.getMessage());
            }
        }, dnsCheckIntervalMs, dnsCheckIntervalMs, TimeUnit.MILLISECONDS);
        
        System.out.printf("[DnsAwareDrConsumer] DNS监控已启动，检查间隔: %dms\n", dnsCheckIntervalMs);
    }
    
    /**
     * 检查DNS变化
     */
    private void checkDnsChanges() {
        try {
            // 重新解析DNS
            ClusterDiscovery.DiscoveryResult result = ClusterDiscovery.discoverClusters(domainWithPort, new ArrayList<>());
            Set<String> newIPs = extractAllIPs(result);
            Set<String> oldIPs = lastResolvedIPs.get();
            
            // 检查是否有变化
            if (!newIPs.equals(oldIPs)) {
                System.out.printf("[DnsAwareDrConsumer] 检测到DNS变化:\n");
                System.out.printf("  旧IP列表: %s\n", oldIPs);
                System.out.printf("  新IP列表: %s\n", newIPs);
                
                // 检查当前集群是否还可用
                boolean currentClusterStillAvailable = checkCurrentClusterAvailability(newIPs);
                
                if (!currentClusterStillAvailable) {
                    System.out.println("[DnsAwareDrConsumer] 当前集群不再可用，触发DR切换");
                    rebuildDrConsumerWithNewDns(result, newIPs);
                } else {
                    System.out.println("[DnsAwareDrConsumer] 当前集群仍然可用，更新DNS缓存");
                    lastResolvedIPs.set(newIPs);
                }
            }
            
        } catch (Exception e) {
            System.err.printf("[DnsAwareDrConsumer] DNS变化检查失败: %s\n", e.getMessage());
        }
    }
    
    /**
     * 检查当前集群是否仍然可用
     */
    private boolean checkCurrentClusterAvailability(Set<String> newIPs) {
        if (currentClusterSegment == null) {
            return false;
        }
        
        // 检查当前网段的IP是否还在新的DNS解析结果中
        for (String ip : newIPs) {
            if (getNetworkSegment(ip).equals(currentClusterSegment)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 基于新的DNS结果重建DR消费者
     */
    private void rebuildDrConsumerWithNewDns(ClusterDiscovery.DiscoveryResult result, Set<String> newIPs) {
        if (!isRebuilding.compareAndSet(false, true)) {
            System.out.println("[DnsAwareDrConsumer] 正在重建中，跳过");
            return;
        }
        
        try {
            System.out.println("[DnsAwareDrConsumer] 开始重建DR消费者");
            
            // 提交当前偏移量
            if (currentDrConsumer != null) {
                try {
                    currentDrConsumer.commitSync();
                    System.out.println("[DnsAwareDrConsumer] 已提交当前偏移量");
                } catch (Exception e) {
                    System.err.printf("[DnsAwareDrConsumer] 提交偏移量失败: %s\n", e.getMessage());
                }
            }
            
            // 关闭旧的DR消费者
            if (currentDrConsumer != null) {
                try {
                    currentDrConsumer.close();
                    System.out.println("[DnsAwareDrConsumer] 已关闭旧的DR消费者");
                } catch (Exception e) {
                    System.err.printf("[DnsAwareDrConsumer] 关闭旧消费者失败: %s\n", e.getMessage());
                }
            }
            
            // 创建新的DR消费者
            ClusterDiscovery.ClusterInfo primary = result.getPrimaryCluster();
            ClusterDiscovery.ClusterInfo secondary = result.getSecondaryCluster();
            
            if (primary == null) {
                throw new RuntimeException("无法发现可用的集群");
            }
            
            List<String> primaryServers = primary.getBrokers();
            List<String> secondaryServers = secondary != null ? secondary.getBrokers() : new ArrayList<>();
            
            currentDrConsumer = new DrAwareConsumerDecorator(primaryServers, secondaryServers, groupId, config);
            currentClusterSegment = getNetworkSegment(primaryServers.get(0).split(":")[0]);
            
            // 重新订阅
            if (!subscribedTopics.isEmpty()) {
                currentDrConsumer.subscribe(subscribedTopics);
                System.out.printf("[DnsAwareDrConsumer] 重新订阅主题: %s\n", subscribedTopics);
            }
            
            // 更新DNS缓存
            lastResolvedIPs.set(newIPs);
            
            System.out.printf("[DnsAwareDrConsumer] DR消费者重建完成: 新主集群=%s, 新备集群=%s, 当前网段=%s\n",
                primaryServers, secondaryServers, currentClusterSegment);
                
        } catch (Exception e) {
            System.err.printf("[DnsAwareDrConsumer] 重建DR消费者失败: %s\n", e.getMessage());
        } finally {
            isRebuilding.set(false);
        }
    }
    
    /**
     * 从发现结果中提取所有IP
     */
    private Set<String> extractAllIPs(ClusterDiscovery.DiscoveryResult result) {
        Set<String> allIPs = new HashSet<>();
        for (ClusterDiscovery.ClusterInfo cluster : result.getClusters()) {
            for (String broker : cluster.getBrokers()) {
                String ip = broker.split(":")[0];
                allIPs.add(ip);
            }
        }
        return allIPs;
    }
    
    /**
     * 获取IP的网络段
     */
    private String getNetworkSegment(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length >= 3) {
            return parts[0] + "." + parts[1] + "." + parts[2];
        }
        return ip;
    }
    
    // ========== KafkaLiteConsumer接口实现 ==========
    
    @Override
    public void subscribe(List<String> topics) {
        this.subscribedTopics = new ArrayList<>(topics);
        if (currentDrConsumer != null) {
            currentDrConsumer.subscribe(topics);
        }
    }
    
    @Override
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (currentDrConsumer != null) {
            return currentDrConsumer.poll(timeoutMs);
        }
        return new ArrayList<>();
    }
    
    @Override
    public void commitSync() {
        if (currentDrConsumer != null) {
            currentDrConsumer.commitSync();
        }
    }
    
    @Override
    public void commitAsync() {
        if (currentDrConsumer != null) {
            currentDrConsumer.commitAsync();
        }
    }
    
    @Override
    public void close() {
        System.out.println("[DnsAwareDrConsumer] 开始关闭");
        
        // 关闭DNS监控
        dnsMonitor.shutdown();
        try {
            if (!dnsMonitor.awaitTermination(5, TimeUnit.SECONDS)) {
                dnsMonitor.shutdownNow();
            }
        } catch (InterruptedException e) {
            dnsMonitor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 关闭DR消费者
        if (currentDrConsumer != null) {
            currentDrConsumer.close();
        }
        
        System.out.println("[DnsAwareDrConsumer] 关闭完成");
    }
    
    // ========== 状态查询方法 ==========
    
    /**
     * 获取当前状态
     */
    public String getStatus() {
        String drStatus = currentDrConsumer != null ? currentDrConsumer.getStatus() : "未初始化";
        return String.format("DNS感知DR消费者 - 域名=%s, 当前网段=%s, 最新IP=%s, DR状态=[%s]", 
            domainWithPort, currentClusterSegment, lastResolvedIPs.get(), drStatus);
    }
    
    /**
     * 手动触发DNS检查
     */
    public void triggerDnsCheck() {
        System.out.println("[DnsAwareDrConsumer] 手动触发DNS检查");
        checkDnsChanges();
    }
    
    /**
     * 手动触发故障转移（委托给内部DR消费者）
     */
    public void triggerFailover() {
        if (currentDrConsumer != null) {
            currentDrConsumer.triggerFailover();
        }
    }
} 