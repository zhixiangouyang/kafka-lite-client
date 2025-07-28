package org.example.kafkalite.metadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MetadataResponseParser {
    public static Metadata parse(ByteBuffer buffer) {
        try {
            // 打印响应字节但不影响buffer的position
            byte[] allBytes = new byte[buffer.remaining()];
            int originalPosition = buffer.position();
            buffer.get(allBytes);
            buffer.position(originalPosition);
            
            // 跳过总长度
            int totalSize = buffer.getInt();
            
            // 读取correlationId
        int correlationId = buffer.getInt();

            // 解析brokers数组
        int brokerCount = buffer.getInt();
            if (brokerCount < 0 || brokerCount > 1000) {
                throw new IllegalArgumentException("Invalid broker count: " + brokerCount);
            }
            
            Map<Integer, BrokerInfo> brokers = new HashMap<>();
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = buffer.getInt();
            String host = readString(buffer);
            int port = buffer.getInt();
                
                // 如果host为空，使用原始IP
                if (host == null || host.isEmpty()) {
                    host = "10.53.29.81";
                    // System.out.println("[MetadataResponseParser] Using original IP for empty host");
                }
                
                // System.out.printf("[MetadataResponseParser] Broker %d: nodeId=%d, host=%s, port=%d%n", 
                //         i, nodeId, host, port);
            brokers.put(nodeId, new BrokerInfo(nodeId, host, port));
        }

            // 解析topic元数据数组
        int topicCount = buffer.getInt();
            if (topicCount < 0 || topicCount > 1000) {
                throw new IllegalArgumentException("Invalid topic count: " + topicCount);
            }
            
            Map<String, Map<Integer, PartitionInfo>> topics = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            short errorCode = buffer.getShort();
                String topic = readString(buffer);
                if (topic == null) {
                    throw new IllegalArgumentException("Topic name cannot be null at index " + i);
                }
                // System.out.printf("[MetadataResponseParser] Topic %d: name=%s, errorCode=%d%n", 
                //         i, topic, errorCode);
                
                // 解析分区元数据数组
            int partitionCount = buffer.getInt();
                if (partitionCount < 0 || partitionCount > 1000) {
                    throw new IllegalArgumentException("Invalid partition count for topic " + topic + ": " + partitionCount);
                }
                // System.out.printf("[MetadataResponseParser] Partition count for topic %s: %d%n", 
                //         topic, partitionCount);
            
                Map<Integer, PartitionInfo> partitions = new HashMap<>();
            for (int j = 0; j < partitionCount; j++) {
                    short partitionErrorCode = buffer.getShort();
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();
                
                    // 跳过replicas数组
                    int replicaCount = buffer.getInt();
                    if (replicaCount < 0 || replicaCount > 1000) {
                        throw new IllegalArgumentException("Invalid replica count for partition " + partitionId + ": " + replicaCount);
                }
                    for (int k = 0; k < replicaCount; k++) {
                        buffer.getInt(); // replicaId
                    }
                    
                    // 跳过isr数组
                    int isrCount = buffer.getInt();
                    if (isrCount < 0 || isrCount > 1000) {
                        throw new IllegalArgumentException("Invalid ISR count for partition " + partitionId + ": " + isrCount);
                    }
                    for (int k = 0; k < isrCount; k++) {
                        buffer.getInt(); // isrId
                    }
                    
                    // 获取leader broker信息
                    BrokerInfo leaderBroker = brokers.get(leaderId);
                    if (leaderBroker == null) {
                        // System.err.printf("[MetadataResponseParser] Leader broker not found: topic=%s, partition=%d, leaderId=%d%n",
                        //         topic, partitionId, leaderId);
                        continue;
                    }
                    
                    // System.out.printf("[MetadataResponseParser] Partition %d: id=%d, errorCode=%d, leaderId=%d%n",
                    //         j, partitionId, partitionErrorCode, leaderId);
                    
                    String leaderAddress = leaderBroker.getHost() + ":" + leaderBroker.getPort();
                    partitions.put(partitionId, new PartitionInfo(partitionId, leaderAddress));
    }

                if (errorCode == 0) {
                    topics.put(topic, partitions);
                } else {
                    // System.err.printf("[MetadataResponseParser] Skipping topic %s due to error: %d%n", 
                    //         topic, errorCode);
    }
            }
            
            // System.out.println("[MetadataResponseParser] Parsing completed successfully");
            return new Metadata(brokers, topics);
            
        } catch (Exception e) {
            // System.err.println("[MetadataResponseParser] Error parsing metadata response: " + e.getMessage());
            // System.err.println("[MetadataResponseParser] Buffer position at error: " + buffer.position());
            e.printStackTrace();
            throw e;
        }
    }

    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) {
            return null;
        }
        if (length > buffer.remaining()) {
            throw new IllegalArgumentException("String length " + length + " exceeds remaining buffer size " + buffer.remaining());
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String str = new String(bytes, StandardCharsets.UTF_8);
        // System.out.println("[MetadataResponseParser] Read string: length=" + length + ", value='" + str + "'");
        return str;
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xff));
        }
        return sb.toString();
    }
}
