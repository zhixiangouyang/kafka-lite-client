package org.example.kafkalite.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * DR感知的Kafka消费者装饰器
 * 使用装饰器模式为KafkaLiteConsumerImpl添加DR切换功能
 */
public class DrAwareConsumerDecorator implements KafkaLiteConsumer {
    
    // 基础消费者
    private volatile KafkaLiteConsumerImpl currentConsumer;
    
    // 集群配置
    private final List<String> primaryServers;
    private final List<String> secondaryServers;
    private final String groupId;
    private final ConsumerConfig config;
    
    // DR状态
    private final AtomicReference<String> currentCluster = new AtomicReference<>("PRIMARY");
    private final AtomicBoolean isHealthy = new AtomicBoolean(true);
    private volatile List<String> subscribedTopics = new ArrayList<>();
    
    // DNS解析缓存
    private final ConcurrentHashMap<String, DnsEntry> dnsCache = new ConcurrentHashMap<>();
    private final long dnsTtlMs;
    private final ScheduledExecutorService dnsRefresher;
    private final ScheduledExecutorService healthChecker;
    
    // 健康检查配置
    private final long healthCheckIntervalMs;
    private final int maxRetries;
    
    /**
     * DNS缓存条目
     */
    private static class DnsEntry {
        final String resolvedIp;
        final long timestamp;
        final long ttlMs;
        
        DnsEntry(String resolvedIp, long ttlMs) {
            this.resolvedIp = resolvedIp;
            this.timestamp = System.currentTimeMillis();
            this.ttlMs = ttlMs;
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > ttlMs;
        }
    }
    
    /**
     * 构造函数
     */
    public DrAwareConsumerDecorator(List<String> primaryServers, 
                                   List<String> secondaryServers,
                                   String groupId,
                                   ConsumerConfig config) {
        this.primaryServers = new ArrayList<>(primaryServers);
        this.secondaryServers = new ArrayList<>(secondaryServers);
        this.groupId = groupId;
        this.config = config;
        
        // DNS配置
        this.dnsTtlMs = config.getDnsTtlMs() != null ? config.getDnsTtlMs() : 300000; // 5分钟默认TTL
        this.dnsRefresher = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "DR-DNS-Refresher");
            t.setDaemon(true);
            return t;
        });
        this.healthChecker = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "DR-Health-Checker");
            t.setDaemon(true);
            return t;
        });
        
        // 健康检查配置
        this.healthCheckIntervalMs = config.getHealthCheckIntervalMs() != null ? 
            config.getHealthCheckIntervalMs() : 30000; // 30秒默认
        this.maxRetries = config.getMaxRetryCount() != null ? config.getMaxRetryCount() : 3;
        
        // 初始化主集群消费者
        this.currentConsumer = createConsumer(resolveServers(primaryServers));
        
        // 启动DNS刷新和健康检查
        startDnsRefresh();
        startHealthCheck();
        
        System.out.printf("[DrAwareConsumerDecorator] 初始化完成: 主集群=%s, 备集群=%s, 当前集群=%s\n", 
            primaryServers, secondaryServers, currentCluster.get());
    }
    
    /**
     * 订阅主题
     */
    @Override
    public void subscribe(List<String> topics) {
        this.subscribedTopics = new ArrayList<>(topics);
        if (currentConsumer != null) {
            currentConsumer.subscribe(topics);
        }
        System.out.printf("[DrAwareConsumerDecorator] 订阅主题: %s\n", topics);
    }
    
    /**
     * 消费消息
     */
    @Override
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (currentConsumer == null) {
            System.out.println("[DrAwareConsumerDecorator] 当前消费者为null，尝试重建");
            rebuildConsumer();
            return new ArrayList<>();
        }
        
        try {
            return currentConsumer.poll(timeoutMs);
        } catch (Exception e) {
            System.out.printf("[DrAwareConsumerDecorator] 消费异常: %s\n", e.getMessage());
            isHealthy.set(false);
            return new ArrayList<>();
        }
    }
    
    /**
     * 同步提交偏移量
     */
    @Override
    public void commitSync() {
        if (currentConsumer != null) {
            try {
                currentConsumer.commitSync();
            } catch (Exception e) {
                System.out.printf("[DrAwareConsumerDecorator] 提交偏移量失败: %s\n", e.getMessage());
            }
        }
    }
    
    /**
     * 异步提交偏移量
     */
    @Override
    public void commitAsync() {
        if (currentConsumer != null) {
            try {
                currentConsumer.commitAsync();
            } catch (Exception e) {
                System.out.printf("[DrAwareConsumerDecorator] 异步提交偏移量失败: %s\n", e.getMessage());
            }
        }
    }
    
    /**
     * 关闭消费者
     */
    @Override
    public void close() {
        System.out.println("[DrAwareConsumerDecorator] 开始关闭");
        
        // 提交最后的偏移量
        commitSync();
        
        // 关闭执行器
        dnsRefresher.shutdown();
        healthChecker.shutdown();
        
        // 关闭消费者
        if (currentConsumer != null) {
            currentConsumer.close();
        }
        
        System.out.println("[DrAwareConsumerDecorator] 关闭完成");
    }
    
    /**
     * 启动DNS定期刷新
     */
    private void startDnsRefresh() {
        dnsRefresher.scheduleWithFixedDelay(() -> {
            try {
                refreshExpiredDnsEntries();
            } catch (Exception e) {
                System.out.printf("[DrAwareConsumerDecorator] DNS刷新异常: %s\n", e.getMessage());
            }
        }, dnsTtlMs / 2, dnsTtlMs / 2, TimeUnit.MILLISECONDS);
        
        System.out.printf("[DrAwareConsumerDecorator] DNS定期刷新已启动，间隔: %dms\n", dnsTtlMs / 2);
    }
    
    /**
     * 启动健康检查
     */
    private void startHealthCheck() {
        healthChecker.scheduleWithFixedDelay(() -> {
            try {
                if (!isHealthy.get()) {
                    System.out.println("[DrAwareConsumerDecorator] 检测到不健康状态，尝试切换");
                    performFailover();
                }
            } catch (Exception e) {
                System.out.printf("[DrAwareConsumerDecorator] 健康检查异常: %s\n", e.getMessage());
            }
        }, healthCheckIntervalMs, healthCheckIntervalMs, TimeUnit.MILLISECONDS);
        
        System.out.printf("[DrAwareConsumerDecorator] 健康检查已启动，间隔: %dms\n", healthCheckIntervalMs);
    }
    
    /**
     * 执行故障转移
     */
    private void performFailover() {
        String current = currentCluster.get();
        System.out.printf("[DrAwareConsumerDecorator] 开始故障转移，当前集群: %s\n", current);
        
        // 提交当前偏移量
        commitSync();
        
        if ("PRIMARY".equals(current)) {
            switchToSecondary();
        } else {
            switchToPrimary();
        }
    }
    
    /**
     * 切换到备集群
     */
    private void switchToSecondary() {
        System.out.println("[DrAwareConsumerDecorator] 切换到备集群");
        
        // 强制刷新备集群DNS
        List<String> resolvedServers = forceRefreshDnsAndResolve(secondaryServers);
        
        // 重建消费者
        rebuildConsumer(resolvedServers);
        
        currentCluster.set("SECONDARY");
        isHealthy.set(true);
        
        System.out.printf("[DrAwareConsumerDecorator] 已切换到备集群: %s\n", resolvedServers);
    }
    
    /**
     * 切换到主集群
     */
    private void switchToPrimary() {
        System.out.println("[DrAwareConsumerDecorator] 切换到主集群");
        
        // 强制刷新主集群DNS
        List<String> resolvedServers = forceRefreshDnsAndResolve(primaryServers);
        
        // 重建消费者
        rebuildConsumer(resolvedServers);
        
        currentCluster.set("PRIMARY");
        isHealthy.set(true);

        System.out.printf("[DrAwareConsumerDecorator] 已切换到主集群: %s\n", primaryServers);
    }
    
    /**
     * 强制刷新指定服务器列表的DNS并返回解析后的服务器列表
     */
    private List<String> forceRefreshDnsAndResolve(List<String> servers) {
        System.out.printf("[DrAwareConsumerDecorator] 强制刷新DNS: %s\n", servers);
        List<String> resolvedServers = new ArrayList<>();
        
        for (String server : servers) {
            String hostname = extractHostname(server);
            String port = extractPort(server);
            
            if (isIpAddress(hostname)) {
                resolvedServers.add(server); // 已经是IP，直接使用
            } else {
                dnsCache.remove(hostname); // 移除缓存，强制重新解析
                List<String> ips = resolveAllHostnameIPs(hostname); // 解析并缓存所有IP
                resolvedServers.addAll(ips.stream().map(ip -> ip + ":" + port).collect(Collectors.toList()));
            }
        }
        
        return resolvedServers;
    }
    
    /**
     * 重建消费者
     */
    private void rebuildConsumer() {
        List<String> servers = "PRIMARY".equals(currentCluster.get()) ? 
            resolveServers(primaryServers) : resolveServers(secondaryServers);
        rebuildConsumer(servers);
    }
    
    private void rebuildConsumer(List<String> resolvedServers) {
        System.out.printf("[DrAwareConsumerDecorator] 重建消费者，服务器: %s\n", resolvedServers);
        
        // 关闭旧消费者
        if (currentConsumer != null) {
            try {
                currentConsumer.close();
            } catch (Exception e) {
                System.out.printf("[DrAwareConsumerDecorator] 关闭旧消费者异常: %s\n", e.getMessage());
            }
        }
        
        // 创建新消费者
        currentConsumer = createConsumer(resolvedServers);
        
        // 重新订阅
        if (!subscribedTopics.isEmpty()) {
            currentConsumer.subscribe(subscribedTopics);
        }
    }
    
    /**
     * 创建消费者
     */
    private KafkaLiteConsumerImpl createConsumer(List<String> servers) {
        System.out.printf("[DrAwareConsumerDecorator] 创建消费者，服务器: %s\n", servers);
        return new KafkaLiteConsumerImpl(groupId, servers, config);
    }
    
    /**
     * 解析服务器列表（域名转IP）
     */
    private List<String> resolveServers(List<String> servers) {
        List<String> resolved = new ArrayList<>();
        for (String server : servers) {
            String hostname = extractHostname(server);
            String port = extractPort(server);
            
            if (isIpAddress(hostname)) {
                resolved.add(server); // 已经是IP，直接使用
            } else {
                List<String> ips = resolveAllHostnameIPs(hostname);
                resolved.addAll(ips.stream().map(ip -> ip + ":" + port).collect(Collectors.toList()));
            }
        }
        return resolved;
    }
    
    /**
     * 解析主机名为所有IP地址
     */
    private List<String> resolveAllHostnameIPs(String hostname) {
        List<String> allIPs = new ArrayList<>();
        
        try {
            // 获取域名的所有IP地址
            InetAddress[] addresses = InetAddress.getAllByName(hostname);
            for (InetAddress address : addresses) {
                String ip = address.getHostAddress();
                allIPs.add(ip);
                System.out.printf("[DrAwareConsumerDecorator] DNS解析: %s -> %s\n", hostname, ip);
            }
            
            // 缓存所有IP (可以选择缓存第一个或所有)
            if (!allIPs.isEmpty()) {
                dnsCache.put(hostname, new DnsEntry(allIPs.get(0), dnsTtlMs));
            }
            
        } catch (UnknownHostException e) {
            System.out.printf("[DrAwareConsumerDecorator] DNS解析失败: %s, 错误: %s\n", hostname, e.getMessage());
        }
        
        return allIPs;
    }
    
    /**
     * 解析主机名为IP（保持向后兼容）
     */
    private String resolveHostname(String hostname) {
        // 检查缓存
        DnsEntry entry = dnsCache.get(hostname);
        if (entry != null && !entry.isExpired()) {
            return entry.resolvedIp;
        }
        
        // 获取所有IP，返回第一个
        List<String> allIPs = resolveAllHostnameIPs(hostname);
        return allIPs.isEmpty() ? hostname : allIPs.get(0);
    }
    
    /**
     * 刷新过期的DNS条目
     */
    private void refreshExpiredDnsEntries() {
        List<String> expiredHosts = new ArrayList<>();
        dnsCache.entrySet().removeIf(entry -> {
            if (entry.getValue().isExpired()) {
                expiredHosts.add(entry.getKey());
                return true;
            }
            return false;
        });
        
        if (!expiredHosts.isEmpty()) {
            System.out.printf("[DrAwareConsumerDecorator] 刷新过期DNS条目: %s\n", expiredHosts);
            for (String hostname : expiredHosts) {
                resolveAllHostnameIPs(hostname); // 刷新所有IP
            }
        }
    }
    
    /**
     * 提取主机名
     */
    private String extractHostname(String server) {
        return server.split(":")[0];
    }
    
    /**
     * 提取端口
     */
    private String extractPort(String server) {
        String[] parts = server.split(":");
        return parts.length > 1 ? parts[1] : "9092";
    }
    
    /**
     * 判断是否为IP地址
     */
    private boolean isIpAddress(String hostname) {
        return hostname.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
    }
    
    /**
     * 手动触发故障转移
     */
    public void triggerFailover() {
        System.out.println("[DrAwareConsumerDecorator] 手动触发故障转移");
        isHealthy.set(false);
    }
    
    /**
     * 获取当前状态
     */
    public String getStatus() {
        return String.format("集群=%s, 健康=%s, DNS缓存=%d条", 
            currentCluster.get(), isHealthy.get(), dnsCache.size());
    }
    
    /**
     * 打印DNS缓存
     */
    public void printDnsCache() {
        System.out.println("[DrAwareConsumerDecorator] DNS缓存状态:");
        dnsCache.forEach((hostname, entry) -> {
            System.out.printf("  %s -> %s (TTL剩余: %dms)\n", 
                hostname, entry.resolvedIp, 
                entry.ttlMs - (System.currentTimeMillis() - entry.timestamp));
        });
    }
} 