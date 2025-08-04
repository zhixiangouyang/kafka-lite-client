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

    public MetadataManagerImpl(List<String> bootstrapServers) {
        this(bootstrapServers, 10); // 默认连接池大小10
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

            // 2. 选一个broker发请求
            String brokerAddress = bootstrapServers.get(0);
            
            // 3. 使用连接池或短连接发送请求
            ByteBuffer response = sendRequestWithConnectionPool(brokerAddress, request);

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
            } else {
                System.err.printf("[MetadataManagerImpl] 未找到分区leader: topic=%s\n", topic);
            }

        } catch (Exception e) {
            System.err.printf("[MetadataManagerImpl] 刷新元数据失败: topic=%s, 错误=%s\n", topic, e.getMessage());
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
}
