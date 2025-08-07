package org.example.kafkalite.metadata;

import org.example.kafkalite.core.KafkaSocketClient;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataManagerImpl implements MetadataManager {
    private final List<String> bootstrapServers;
    
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

    public MetadataManagerImpl(List<String> bootstrapServers) {
        this(bootstrapServers, 5); // 默认连接池大小10
    }
    
    public MetadataManagerImpl(List<String> bootstrapServers, int connectionPoolSize) {
        this.bootstrapServers = bootstrapServers;
        this.connectionPoolSize = connectionPoolSize;
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
     * 刷新元数据（增强版本）
     * @param topic 主题名称
     * @param isErrorTriggered 是否由错误触发
     * @param isProducerContext 是否在生产者上下文中
     */
    public void refreshMetadata(String topic, boolean isErrorTriggered, boolean isProducerContext) {
        // 智能刷新检查
        if (!refreshStrategy.shouldRefresh(topic, isErrorTriggered, isProducerContext)) {
            return;
        }
        
        try {
            // 确保连接池已初始化
            if (!connectionPoolsInitialized) {
                initializeConnectionPools();
            }
            
            System.out.printf("[MetadataManagerImpl] 刷新元数据: topic=%s\n", topic);
            
            // 1. 编码 MetadataRequest 请求体
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            ByteBuffer request = MetadataRequestEncoder.encodeMetadataRequest(topics, 1);

            // 2. 选一个可用的broker发请求（添加故障转移逻辑）
            ByteBuffer response = null;
            Exception lastException = null;
            
            // 尝试所有broker，直到找到可用的
            String lastSuccessfulBroker = null;
            for (String brokerAddress : bootstrapServers) {
                try {
                    System.out.printf("🔍 [MetadataManagerImpl] 尝试连接broker: %s (topic=%s)\n", brokerAddress, topic);
                    response = sendRequestWithConnectionPool(brokerAddress, request);
                    System.out.printf("✅ [BROKER切换] 成功连接到broker: %s (topic=%s)\n", brokerAddress, topic);
                    lastSuccessfulBroker = brokerAddress;
                    break; // 成功就退出循环
                } catch (Exception e) {
                    System.err.printf("❌ [MetadataManagerImpl] Broker %s 不可用: %s\n", brokerAddress, e.getMessage());
                    lastException = e;
                    // 继续尝试下一个broker
                }
            }
            
            // 如果切换到了不同的broker，输出切换日志
            if (lastSuccessfulBroker != null && !lastSuccessfulBroker.equals(getLastUsedBroker())) {
                System.out.printf("🔄 [BROKER切换] 元数据服务切换: %s -> %s\n", 
                    getLastUsedBroker() != null ? getLastUsedBroker() : "初始连接", 
                    lastSuccessfulBroker);
                setLastUsedBroker(lastSuccessfulBroker);
            }
            
            // 如果所有broker都失败了，抛出最后一个异常
            if (response == null) {
                throw new RuntimeException("所有broker都不可用", lastException);
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
            } else {
                System.err.printf("[MetadataManagerImpl] 未找到分区leader: topic=%s\n", topic);
                // 记录错误
                refreshStrategy.recordError(topic);
            }

        } catch (Exception e) {
            System.err.printf("[MetadataManagerImpl] 刷新元数据失败: topic=%s, 错误=%s\n", topic, e.getMessage());
            // 记录错误
            refreshStrategy.recordError(topic);
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
    
    // 用于跟踪broker切换的辅助方法
    private String getLastUsedBroker() {
        return lastUsedBroker;
    }
    
    private void setLastUsedBroker(String broker) {
        this.lastUsedBroker = broker;
    }
}
