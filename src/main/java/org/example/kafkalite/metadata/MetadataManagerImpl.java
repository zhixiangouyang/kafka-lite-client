package org.example.kafkalite.metadata;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.monitor.MetricsCollector;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataManagerImpl implements MetadataManager {
    private volatile List<String> bootstrapServers;  // 改为volatile，支持动态更新
    
    // 新增：连接池管理
    private final Map<String, KafkaSocketClient.ConnectionPool> connectionPools = new ConcurrentHashMap<>();
    private final int connectionPoolSize;
    private volatile boolean connectionPoolsInitialized = false;

    // 缓存：topic -> partition -> leader broker地址
    private final Map<String, Map<Integer, String>> topicPartitionLeaders = new HashMap<>();

    // 缓存：nodeId -> brokerInfo
    private final Map<Integer, BrokerInfo> brokerMap = new HashMap<>();
    
    // 用于跟踪broker切换
    private volatile String lastUsedBroker = null;

    // 新增：智能元数据刷新策略
    private final SmartMetadataRefreshStrategy refreshStrategy = new SmartMetadataRefreshStrategy();
    
    // 新增：动态DNS支持
    private final String originalDomain;  // 原始域名:端口，用于重新解析
    
    // 新增：bootstrap servers变化回调
    private Runnable bootstrapServersChangedCallback;
    
    // 📊 指标收集器
    private final MetricsCollector metricsCollector;

    public MetadataManagerImpl(List<String> bootstrapServers) {
        this(bootstrapServers, 5); // 默认连接池大小10
    }
    
    public MetadataManagerImpl(List<String> bootstrapServers, int connectionPoolSize) {
        this.bootstrapServers = bootstrapServers;
        this.connectionPoolSize = connectionPoolSize;
        this.originalDomain = null;  // 传统模式，不支持动态DNS
        this.metricsCollector = new MetricsCollector("metadata-manager", "default");
    }
    
    /**
     * 新增：支持动态DNS的构造函数
     */
    public MetadataManagerImpl(List<String> bootstrapServers, int connectionPoolSize, String originalDomain) {
        this.bootstrapServers = bootstrapServers;
        this.connectionPoolSize = connectionPoolSize;
        this.originalDomain = originalDomain;
        this.metricsCollector = new MetricsCollector("metadata-manager", "dns-aware");  // 保存原始域名，用于重新解析
    }
    
    // 新增：初始化连接池
    private synchronized void initializeConnectionPools() {
        if (connectionPoolsInitialized) {
            return;
        }
        
        System.out.printf("[MetadataManagerImpl] 初始化元数据连接池，连接池大小: %d\n", connectionPoolSize);
        
        for (String broker : bootstrapServers) {
            try {
                String[] parts = broker.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                // 创建连接池
                KafkaSocketClient.ConnectionPool connectionPool = new KafkaSocketClient.ConnectionPool(host, port, connectionPoolSize);
                connectionPools.put(broker, connectionPool);
                
                System.out.printf("[MetadataManagerImpl] 已创建元数据连接池: %s:%d\n", host, port);
            } catch (Exception e) {
                System.err.printf("[MetadataManagerImpl] 创建元数据连接池失败: %s, 错误: %s\n", broker, e.getMessage());
            }
        }
        
        connectionPoolsInitialized = true;
    }
    
    // 新增：使用连接池发送请求
    private ByteBuffer sendRequestWithConnectionPool(String brokerAddress, ByteBuffer request) {
        KafkaSocketClient.ConnectionPool connectionPool = connectionPools.get(brokerAddress);
        if (connectionPool != null) {
            try {
                return connectionPool.sendAndReceive(request);
            } catch (Exception e) {
                System.err.printf("[MetadataManagerImpl] 连接池请求失败: %s, 错误: %s，回退到短连接\n", brokerAddress, e.getMessage());
                // 回退到短连接
                return sendRequestWithShortConnection(brokerAddress, request);
            }
        } else {
            // 没有连接池，使用短连接
            return sendRequestWithShortConnection(brokerAddress, request);
        }
    }
    
    // 新增：使用短连接发送请求（原有逻辑）
    private ByteBuffer sendRequestWithShortConnection(String brokerAddress, ByteBuffer request) {
        String[] parts = brokerAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return KafkaSocketClient.sendAndReceive(host, port, request);
    }

    @Override
    public void refreshMetadata(String topic) {
        refreshMetadata(topic, false, false);
    }
    
    /**
     * 🔧 新增：强制刷新元数据，绕过智能策略（用于集群切换）
     */
    public void forceRefreshMetadata(String topic) {
        refreshMetadata(topic, true, false, true); // 强制刷新
    }
    
    /**
     * 刷新元数据（增强版本）
     * @param topic 主题名称
     * @param isErrorTriggered 是否由错误触发
     * @param isProducerContext 是否在生产者上下文中
     */
    public void refreshMetadata(String topic, boolean isErrorTriggered, boolean isProducerContext) {
        refreshMetadata(topic, isErrorTriggered, isProducerContext, false);
    }
    
    /**
     * 刷新元数据（完整版本）
     * @param topic 主题名称
     * @param isErrorTriggered 是否由错误触发
     * @param isProducerContext 是否在生产者上下文中
     * @param forceRefresh 是否强制刷新，绕过智能策略
     */
    public void refreshMetadata(String topic, boolean isErrorTriggered, boolean isProducerContext, boolean forceRefresh) {
        long startTime = System.currentTimeMillis();
        
        // 📊 指标埋点: 元数据刷新尝试
        Map<String, String> labels = new HashMap<>();
        labels.put("topic", topic);
        labels.put("error_triggered", String.valueOf(isErrorTriggered));
        labels.put("producer_context", String.valueOf(isProducerContext));
        metricsCollector.incrementCounter("metadata.refresh.attempt", labels);
        
        // 🔧 兜底机制：每次refresh都检查DNS变化（主动发现域名指向变更）
        if (originalDomain != null) {
            checkDnsChangesProactively();
        }

        // 智能刷新检查（除非强制刷新）
        if (!forceRefresh && !refreshStrategy.shouldRefresh(topic, isErrorTriggered, isProducerContext)) {
            // 📊 指标埋点: 智能策略跳过
            metricsCollector.incrementCounter("metadata.refresh.skipped", labels);
            return;
        }
        
        if (forceRefresh) {
            System.out.printf("[MetadataManagerImpl] 强制刷新元数据: topic=%s\n", topic);
        }
        
        try {
            // 确保连接池已初始化
            if (!connectionPoolsInitialized) {
                initializeConnectionPools();
            }

            // 1. 编码 MetadataRequest 请求体
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            ByteBuffer request = MetadataRequestEncoder.encodeMetadataRequest(topics, 1);

            // 2. 选一个可用的broker发请求（添加故障转移逻辑）
            ByteBuffer response = null;
            Exception lastException = null;
            
            // 尝试所有broker，直到找到可用的
            String lastSuccessfulBroker = null;
            // 🔧 修复：创建副本避免ConcurrentModificationException
            List<String> currentBootstrapServers = new ArrayList<>(bootstrapServers);
            for (String brokerAddress : currentBootstrapServers) {
                try {
                    System.out.printf("[MetadataManagerImpl] 尝试连接broker: %s (topic=%s)\n", brokerAddress, topic);
                    response = sendRequestWithConnectionPool(brokerAddress, request);
                    System.out.printf("[BROKER切换] 成功连接到broker: %s (topic=%s)\n", brokerAddress, topic);
                    lastSuccessfulBroker = brokerAddress;
                    break; // 成功就退出循环
                } catch (Exception e) {
                    System.err.printf("[MetadataManagerImpl] Broker %s 不可用: %s\n", brokerAddress, e.getMessage());
                    lastException = e;
                    // 继续尝试下一个broker
                }
            }
            
            // 如果切换到了不同的broker，输出切换日志
            if (lastSuccessfulBroker != null && !lastSuccessfulBroker.equals(getLastUsedBroker())) {
                System.out.printf("[BROKER切换] 元数据服务切换: %s -> %s\n",
                    getLastUsedBroker() != null ? getLastUsedBroker() : "初始连接", 
                    lastSuccessfulBroker);
                setLastUsedBroker(lastSuccessfulBroker);
            }
            
            // 如果所有broker都失败了，尝试重新解析DNS
            if (response == null) {
                if (originalDomain != null) {
                    System.out.println("[MetadataManagerImpl] 所有broker都不可用，尝试重新解析DNS...");
                    List<String> newBootstrapServers = resolveDomainToIPs(originalDomain);
                    
                    // 检查是否获得了新的IP
                    if (!newBootstrapServers.equals(bootstrapServers)) {
                        System.out.printf("[MetadataManagerImpl] DNS重解析获得新IP: 旧=%s, 新=%s\n", 
                            bootstrapServers, newBootstrapServers);
                        
                        // 更新bootstrap servers
                        this.bootstrapServers = newBootstrapServers;
                        
                        // 清理旧连接池
                        clearOldConnectionPools();
                        connectionPoolsInitialized = false;
                        
                        // 重新初始化连接池
                        initializeConnectionPools();
                        
                        // 🔧 重要：通知所有相关组件更新连接
                        notifyBootstrapServersChanged(newBootstrapServers);
                        
                        // 用新的IP重试一次
                        List<String> newCurrentBootstrapServers = new ArrayList<>(bootstrapServers);
                        for (String brokerAddress : newCurrentBootstrapServers) {
                            try {
                                System.out.printf("[MetadataManagerImpl] 重解析后尝试连接broker: %s (topic=%s)\n", brokerAddress, topic);
                                response = sendRequestWithConnectionPool(brokerAddress, request);
                                System.out.printf("[BROKER切换] 重解析后成功连接到broker: %s (topic=%s)\n", brokerAddress, topic);
                                lastSuccessfulBroker = brokerAddress;
                                break;
                            } catch (Exception e) {
                                System.err.printf("[MetadataManagerImpl] 重解析后Broker %s 仍不可用: %s\n", brokerAddress, e.getMessage());
                                lastException = e;
                            }
                        }
                    } else {
                        System.out.println("[MetadataManagerImpl] DNS重解析未获得新IP，IP列表未变化");
                    }
                }
                
                // 如果重解析后仍然失败，抛出异常
                if (response == null) {
                    throw new RuntimeException("所有broker都不可用（包括重解析后的IP）", lastException);
                }
            }

            // 4. 解析响应
            Metadata metadata = MetadataResponseParser.parse(response);
            System.out.printf("[MetadataManagerImpl] 收到元数据: %s\n", metadata);

            // 5. 缓存更新
            brokerMap.clear();
            brokerMap.putAll(metadata.getBrokers());

            Map<Integer, String> leaders = metadata.getPartitionLeaders(topic);
            if (leaders != null) {
                topicPartitionLeaders.put(topic, leaders);
                System.out.printf("[MetadataManagerImpl] 更新分区leader: topic=%s, leaders=%s\n", topic, leaders);
                // 记录成功
                refreshStrategy.recordSuccess(topic);
                
                // 📊 指标埋点: 元数据刷新成功
                long endTime = System.currentTimeMillis();
                metricsCollector.incrementCounter(MetricsCollector.METRIC_METADATA_REFRESH, labels);
                metricsCollector.incrementCounter("metadata.refresh.success", labels);
                metricsCollector.recordLatency("metadata.refresh.latency", endTime - startTime, labels);
                
                // 记录分区数量
                metricsCollector.setGauge("metadata.partitions.count", leaders.size(), labels);
                
            } else {
                System.err.printf("[MetadataManagerImpl] 未找到分区leader: topic=%s\n", topic);
                // 记录错误
                refreshStrategy.recordError(topic);
                
                // 📊 指标埋点: 元数据解析失败
                metricsCollector.incrementCounter("metadata.refresh.parse_error", labels);
            }

        } catch (Exception e) {
            System.err.printf("[MetadataManagerImpl] 刷新元数据失败: topic=%s, 错误=%s\n", topic, e.getMessage());
            // 记录错误
            refreshStrategy.recordError(topic);
            
            // 📊 指标埋点: 元数据刷新失败
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter("metadata.refresh.error", labels);
            metricsCollector.recordLatency("metadata.refresh.error_latency", endTime - startTime, labels);
            
            throw new RuntimeException("Failed to refresh metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<Integer, String> getPartitionLeaders(String topic) {
        return topicPartitionLeaders.getOrDefault(topic, Collections.emptyMap());
    }
    
    // 新增：关闭连接池
    public void close() {
        System.out.println("[MetadataManagerImpl] 关闭元数据连接池");
        for (Map.Entry<String, KafkaSocketClient.ConnectionPool> entry : connectionPools.entrySet()) {
            try {
                entry.getValue().close();
                System.out.printf("[MetadataManagerImpl] 已关闭连接池: %s\n", entry.getKey());
            } catch (Exception e) {
                System.err.printf("[MetadataManagerImpl] 关闭连接池失败: %s, 错误: %s\n", entry.getKey(), e.getMessage());
            }
        }
        connectionPools.clear();
    }
    
    /**
     * 清理旧的连接池
     */
    private void clearOldConnectionPools() {
        System.out.println("[MetadataManagerImpl] 清理旧连接池...");
        for (KafkaSocketClient.ConnectionPool pool : connectionPools.values()) {
            try {
                pool.close();
            } catch (Exception e) {
                System.err.printf("[MetadataManagerImpl] 关闭旧连接池失败: %s\n", e.getMessage());
            }
        }
        connectionPools.clear();
    }
    
    /**
     * 解析域名为IP地址列表
     */
    private List<String> resolveDomainToIPs(String domainWithPort) {
        List<String> ips = new ArrayList<>();
        
        String[] parts = domainWithPort.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("域名格式错误，应为 domain:port，实际: " + domainWithPort);
        }
        
        String domain = parts[0];
        String port = parts[1];
        
        // 如果已经是IP地址，直接返回
        if (isIpAddress(domain)) {
            ips.add(domainWithPort);
            return ips;
        }
        
        try {
            java.net.InetAddress[] addresses = java.net.InetAddress.getAllByName(domain);
            for (java.net.InetAddress address : addresses) {
                String ip = address.getHostAddress();
                ips.add(ip + ":" + port);
                System.out.printf("[MetadataManagerImpl] DNS重解析: %s -> %s:%s\n", domain, ip, port);
            }
            
            if (ips.isEmpty()) {
                throw new RuntimeException("域名重解析失败，未获取到任何IP: " + domain);
            }
            
        } catch (java.net.UnknownHostException e) {
            throw new RuntimeException("域名重解析失败: " + domain + ", 错误: " + e.getMessage(), e);
        }
        
        return ips;
    }
    
    /**
     * 检查是否为IP地址
     */
    private boolean isIpAddress(String host) {
        String ipPattern = "^([0-9]{1,3}\\.){3}[0-9]{1,3}$";
        return host.matches(ipPattern);
    }
    
    /**
     * 设置bootstrap servers变化回调
     */
    public void setBootstrapServersChangedCallback(Runnable callback) {
        this.bootstrapServersChangedCallback = callback;
    }
    
         /**
      * 通知bootstrap servers已变化
      */
     private void notifyBootstrapServersChanged(List<String> newBootstrapServers) {
         System.out.printf("[MetadataManagerImpl] 通知组件bootstrap servers已更新: %s\n", newBootstrapServers);
         if (bootstrapServersChangedCallback != null) {
             try {
                 bootstrapServersChangedCallback.run();
             } catch (Exception e) {
                 System.err.printf("[MetadataManagerImpl] 执行bootstrap servers变化回调失败: %s\n", e.getMessage());
             }
         }
     }
     
         /**
     * 获取当前的bootstrap servers
     */
    public List<String> getBootstrapServers() {
        return new ArrayList<>(bootstrapServers);
    }
    
    /**
     * 主动检查DNS变化（兜底机制）
     * 每次refresh时都检查，发现域名指向变更时主动切换
     */
    private void checkDnsChangesProactively() {
        long startTime = System.currentTimeMillis();
        
        // 📊 指标埋点: DNS检查尝试
        metricsCollector.incrementCounter("dns.check.attempt");
        
        try {
            // 重新解析DNS
            List<String> newBootstrapServers = resolveDomainToIPs(originalDomain);
            
            // 📊 指标埋点: DNS解析成功
            long dnsLatency = System.currentTimeMillis() - startTime;
            metricsCollector.recordLatency(MetricsCollector.METRIC_DNS_RESOLUTION, dnsLatency);
            
            // 检查是否有变化
            if (!newBootstrapServers.equals(bootstrapServers)) {
                System.out.printf("[MetadataManagerImpl] 🔍 主动发现DNS变化:\n");
                System.out.printf("  当前IP列表: %s\n", bootstrapServers);
                System.out.printf("  新解析IP列表: %s\n", newBootstrapServers);
                System.out.println("  触发主动切换...");
                
                // 📊 指标埋点: DNS变化检测到
                metricsCollector.incrementCounter("dns.change.detected");
                metricsCollector.incrementCounter(MetricsCollector.METRIC_DR_SWITCH);
                
                // 更新bootstrap servers
                this.bootstrapServers = newBootstrapServers;
                
                // 清理旧连接池
                clearOldConnectionPools();
                connectionPoolsInitialized = false;
                
                // 重新初始化连接池
                initializeConnectionPools();
                
                // 通知所有相关组件更新连接
                notifyBootstrapServersChanged(newBootstrapServers);
                
                System.out.printf("[MetadataManagerImpl] ✅ 主动切换完成: %s\n", newBootstrapServers);
                
                // 📊 指标埋点: DNS切换成功
                metricsCollector.incrementCounter("dns.switch.success");
                
            } else {
                // 📊 指标埋点: DNS无变化
                metricsCollector.incrementCounter("dns.check.no_change");
                
                // DNS没有变化，可以输出调试信息（但不要太频繁）
                if (System.currentTimeMillis() % 60000 < 1000) { // 大约每分钟输出一次
                    System.out.printf("[MetadataManagerImpl] 🔍 DNS检查: 无变化 %s\n", bootstrapServers);
                }
            }
            
        } catch (Exception e) {
            System.err.printf("[MetadataManagerImpl] 主动DNS检查失败: %s\n", e.getMessage());
            
            // 📊 指标埋点: DNS检查失败
            metricsCollector.incrementCounter("dns.check.error");
            
            // 不抛出异常，避免影响正常的metadata刷新
        }
    }
    
    // 用于跟踪broker切换的辅助方法
    private String getLastUsedBroker() {
        return lastUsedBroker;
    }
    
    private void setLastUsedBroker(String broker) {
        this.lastUsedBroker = broker;
    }
}
