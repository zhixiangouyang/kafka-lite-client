package org.example.kafkalite.network;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * 动态DNS解析器 - 解决Kafka官方客户端DNS缓存问题
 * 
 * 功能：
 * 1. 域名解析和IP缓存
 * 2. 定期DNS刷新，支持DR切换
 * 3. 解析失败重试机制
 * 4. 缓存过期和清理
 */
public class DynamicDnsResolver {
    
    private static class ResolvedAddress {
        private final String ip;
        private final long resolvedTime;
        private final long ttl; // Time To Live in milliseconds
        
        public ResolvedAddress(String ip, long ttl) {
            this.ip = ip;
            this.resolvedTime = System.currentTimeMillis();
            this.ttl = ttl;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - resolvedTime > ttl;
        }
        
        public String getIp() {
            return ip;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - resolvedTime;
        }
    }
    
    // DNS缓存：hostname -> ResolvedAddress
    private final Map<String, ResolvedAddress> dnsCache = new ConcurrentHashMap<>();
    
    // 配置参数
    private final long defaultTtlMs;
    private final long refreshIntervalMs;
    private final int maxRetries;
    private final long retryDelayMs;
    
    // 定期刷新线程池
    private final ScheduledExecutorService refreshExecutor;
    private volatile boolean isShutdown = false;
    
    // 统计信息
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    private final AtomicLong refreshCount = new AtomicLong(0);
    private final AtomicLong failedResolves = new AtomicLong(0);
    
    public DynamicDnsResolver() {
        this(300000, 60000, 3, 1000); // 默认5分钟TTL，1分钟刷新间隔
    }
    
    public DynamicDnsResolver(long defaultTtlMs, long refreshIntervalMs, int maxRetries, long retryDelayMs) {
        this.defaultTtlMs = defaultTtlMs;
        this.refreshIntervalMs = refreshIntervalMs;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        
        this.refreshExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "dns-resolver-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        
        // 启动定期刷新任务
        startPeriodicRefresh();
        
        System.out.printf("[DynamicDnsResolver] 初始化完成: TTL=%dms, 刷新间隔=%dms, 最大重试=%d\n", 
            defaultTtlMs, refreshIntervalMs, maxRetries);
    }
    
    /**
     * 解析域名到IP地址，优先使用缓存
     * 如果是IP地址，直接返回
     * 如果缓存命中，返回缓存中的IP地址
     * 如果缓存miss，执行DNS解析
     * 如果DNS解析失败，返回原始hostname
     * 如果DNS解析成功，更新缓存
     * 如果DNS解析成功，返回解析后的IP地址
     */
    public String resolveToIp(String hostname) {
        // 如果已经是IP地址，直接返回
        if (isIpAddress(hostname)) {
            return hostname;
        }
        
        // 检查缓存
        ResolvedAddress cached = dnsCache.get(hostname);
        if (cached != null && !cached.isExpired()) {
            cacheHits.incrementAndGet();
            System.out.printf("[DnsResolver] 缓存命中: %s -> %s (缓存时间: %dms)\n", 
                hostname, cached.getIp(), cached.getAge());
            return cached.getIp();
        }
        
        // 缓存miss或过期，执行DNS解析
        cacheMisses.incrementAndGet();
        return performDnsResolution(hostname, true);
    }
    
    /**
     * 批量解析bootstrap服务器列表
     */
    public List<String> resolveBootstrapServers(List<String> servers) {
        List<String> resolvedServers = new ArrayList<>();
        
        for (String server : servers) {
            try {
                String[] parts = server.split(":");
                String hostname = parts[0];
                String port = parts.length > 1 ? parts[1] : "9092";
                
                String ip = resolveToIp(hostname);
                resolvedServers.add(ip + ":" + port);
                
                System.out.printf("[DnsResolver] 解析服务器: %s -> %s:%s\n", server, ip, port);
            } catch (Exception e) {
                System.err.printf("[DnsResolver] 解析服务器失败: %s, 错误: %s\n", server, e.getMessage());
                // 保留原始地址作为fallback
                resolvedServers.add(server);
            }
        }
        
        return resolvedServers;
    }
    
    /**
     * 强制刷新指定域名的DNS解析
     */
    public void forceRefresh(String hostname) {
        if (isIpAddress(hostname)) {
            return;
        }
        
        System.out.printf("[DnsResolver] 强制刷新DNS: %s\n", hostname);
        performDnsResolution(hostname, false);
    }
    
    /**
     * 强制刷新所有缓存的DNS记录
     * DR切换时，需要强制刷新所有缓存的DNS记录
     */
    public void forceRefreshAll() {
        System.out.printf("[DnsResolver] 强制刷新所有DNS缓存，当前缓存数量: %d\n", dnsCache.size());
        
        for (String hostname : dnsCache.keySet()) {
            refreshExecutor.submit(() -> {
                try {
                    performDnsResolution(hostname, false);
                } catch (Exception e) {
                    System.err.printf("[DnsResolver] 刷新DNS失败: %s, 错误: %s\n", hostname, e.getMessage());
                }
            });
        }
    }
    
    /**
     * 执行实际的DNS解析
     * 通过InetAddress.getByName(hostname) 解析IP地址
     */
    private String performDnsResolution(String hostname, boolean useCache) {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                System.out.printf("[DnsResolver] DNS解析尝试 %d/%d: %s\n", attempt, maxRetries, hostname);
                
                InetAddress address = InetAddress.getByName(hostname);
                String ip = address.getHostAddress();
                
                // 更新缓存
                dnsCache.put(hostname, new ResolvedAddress(ip, defaultTtlMs));
                
                System.out.printf("[DnsResolver] DNS解析成功: %s -> %s (尝试次数: %d)\n", 
                    hostname, ip, attempt);
                
                return ip;
                
            } catch (UnknownHostException e) {
                lastException = e;
                failedResolves.incrementAndGet();
                System.err.printf("[DnsResolver] DNS解析失败 (尝试 %d/%d): %s, 错误: %s\n", 
                    attempt, maxRetries, hostname, e.getMessage());
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs * attempt); // 指数退避
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        // 所有尝试都失败了
        System.err.printf("[DnsResolver] DNS解析最终失败: %s, 最后错误: %s\n", 
            hostname, lastException != null ? lastException.getMessage() : "未知错误");
        
        // 如果允许使用缓存且有过期的缓存，使用过期缓存作为fallback
        if (useCache) {
            ResolvedAddress expired = dnsCache.get(hostname);
            if (expired != null) {
                System.out.printf("[DnsResolver] 使用过期缓存作为fallback: %s -> %s (过期时间: %dms)\n", 
                    hostname, expired.getIp(), expired.getAge() - defaultTtlMs);
                return expired.getIp();
            }
        }
        
        // 最终fallback：返回原始hostname
        System.err.printf("[DnsResolver] 无可用IP，返回原始域名: %s\n", hostname);
        return hostname;
    }
    
    /**
     * 启动定期DNS刷新任务
     */
    private void startPeriodicRefresh() {
        refreshExecutor.scheduleWithFixedDelay(() -> {
            if (isShutdown) {
                return;
            }
            
            try {
                refreshExpiredEntries();
            } catch (Exception e) {
                System.err.printf("[DnsResolver] 定期刷新任务异常: %s\n", e.getMessage());
            }
        }, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
        
        System.out.printf("[DnsResolver] 启动定期DNS刷新任务，间隔: %dms\n", refreshIntervalMs);
    }
    
    /**
     * 刷新过期的DNS缓存条目
     */
    private void refreshExpiredEntries() {
        long startTime = System.currentTimeMillis();
        int refreshed = 0;
        int expired = 0;
        
        for (Map.Entry<String, ResolvedAddress> entry : dnsCache.entrySet()) {
            String hostname = entry.getKey();
            ResolvedAddress address = entry.getValue();
            
            if (address.isExpired()) {
                expired++;
                refreshExecutor.submit(() -> {
                    try {
                        performDnsResolution(hostname, false);
                    } catch (Exception e) {
                        System.err.printf("[DnsResolver] 后台刷新DNS失败: %s, 错误: %s\n", hostname, e.getMessage());
                    }
                });
                refreshed++;
            }
        }
        
        if (refreshed > 0) {
            refreshCount.addAndGet(refreshed);
            System.out.printf("[DnsResolver] 定期刷新完成: 过期条目=%d, 刷新任务=%d, 耗时=%dms\n", 
                expired, refreshed, System.currentTimeMillis() - startTime);
        }
    }
    
    /**
     * 检查是否为IP地址
     */
    private boolean isIpAddress(String hostname) {
        if (hostname == null || hostname.isEmpty()) {
            return false;
        }
        
        // 简单的IP地址检查（IPv4）
        String[] parts = hostname.split("\\.");
        if (parts.length != 4) {
            return false;
        }
        
        try {
            for (String part : parts) {
                int num = Integer.parseInt(part);
                if (num < 0 || num > 255) {
                    return false;
                }
            }
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    /**
     * 清理缓存
     */
    public void clearCache() {
        int cleared = dnsCache.size();
        dnsCache.clear();
        System.out.printf("[DnsResolver] 清理DNS缓存，清理条目数: %d\n", cleared);
    }
    
    /**
     * 获取统计信息
     */
    public void printStats() {
        System.out.printf("[DnsResolver] 统计信息:\n");
        System.out.printf("  缓存命中: %d\n", cacheHits.get());
        System.out.printf("  缓存miss: %d\n", cacheMisses.get());
        System.out.printf("  刷新次数: %d\n", refreshCount.get());
        System.out.printf("  解析失败: %d\n", failedResolves.get());
        System.out.printf("  当前缓存条目: %d\n", dnsCache.size());
        
        // 打印缓存详情
        for (Map.Entry<String, ResolvedAddress> entry : dnsCache.entrySet()) {
            ResolvedAddress addr = entry.getValue();
            System.out.printf("    %s -> %s (缓存时间: %dms, 过期: %s)\n", 
                entry.getKey(), addr.getIp(), addr.getAge(), addr.isExpired() ? "是" : "否");
        }
    }
    
    /**
     * 关闭DNS解析器
     */
    public void shutdown() {
        isShutdown = true;
        
        if (refreshExecutor != null && !refreshExecutor.isShutdown()) {
            refreshExecutor.shutdown();
            try {
                if (!refreshExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    refreshExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                refreshExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        System.out.printf("[DnsResolver] DNS解析器已关闭\n");
    }
} 