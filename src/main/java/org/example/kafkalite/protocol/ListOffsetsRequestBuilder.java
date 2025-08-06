package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ListOffsetsRequestBuilder {
    
    // 特殊时间戳常量
    public static final long EARLIEST_TIMESTAMP = -2L;  // 查询earliest offset
    public static final long LATEST_TIMESTAMP = -1L;    // 查询latest offset
    
    /**
     * 构建ListOffsets请求 (API Key: 2, Version: 1)
     * @param clientId 客户端ID
     * @param topicPartitions topic -> partitions 映射
     * @param timestamp 时间戳，使用EARLIEST_TIMESTAMP或LATEST_TIMESTAMP
     * @param correlationId 关联ID
     * @return 请求的ByteBuffer
     */
    public static ByteBuffer build(String clientId, Map<String, Integer[]> topicPartitions, 
                                 long timestamp, int correlationId) {
        int estimatedSize = 512;
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.position(4); // 预留4字节长度
        
        // 请求头
        short apiKey = 2;     // ListOffsets
        short apiVersion = 1; // 使用版本1
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        putString(buffer, clientId);
        
        // 请求体
        buffer.putInt(-1); // replica_id = -1 (normal consumer)
        
        // topics array
        buffer.putInt(topicPartitions.size());
        for (Map.Entry<String, Integer[]> entry : topicPartitions.entrySet()) {
            String topic = entry.getKey();
            Integer[] partitions = entry.getValue();
            
            putString(buffer, topic);
            
            // partitions array
            buffer.putInt(partitions.length);
            for (int partition : partitions) {
                buffer.putInt(partition);      // partition_index
                buffer.putLong(timestamp);     // timestamp
            }
        }
        
        // 回填长度
        int endPos = buffer.position();
        int totalLen = endPos - 4;
        buffer.putInt(0, totalLen);
        buffer.flip();
        
        // 调试信息
        System.out.printf("[ListOffsetsRequestBuilder] 构建请求: clientId=%s, timestamp=%d, topics=%s\n", 
            clientId, timestamp, topicPartitions.keySet());
        
        return buffer;
    }
    
    private static void putString(ByteBuffer buffer, String s) {
        if (s == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }
} 