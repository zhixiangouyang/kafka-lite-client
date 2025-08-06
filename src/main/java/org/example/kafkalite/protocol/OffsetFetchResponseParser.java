package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class OffsetFetchResponseParser {
    /**
     * 解析 OffsetFetchResponse v2，返回 topic->partition->offset
     */
    public static Map<String, Map<Integer, Long>> parse(ByteBuffer buffer) {
        Map<String, Map<Integer, Long>> result = new HashMap<>();
        buffer.getInt(); // 跳过total length字段
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            String topic = readString(buffer);
            int partitionCount = buffer.getInt();
            Map<Integer, Long> partMap = new HashMap<>();
            for (int j = 0; j < partitionCount; j++) {
                int partition = buffer.getInt();
                long offset = buffer.getLong();
                String metadata = readString(buffer);
                short errorCode = buffer.getShort();
                
                System.out.printf("[OffsetFetchResponse] topic=%s, partition=%d, offset=%d, errorCode=%d\n", 
                    topic, partition, offset, errorCode);
                
                // 只有在没有错误时才使用offset
                if (errorCode == 0) {
                partMap.put(partition, offset);
                } else {
                    System.out.printf("[OffsetFetchResponse] 错误: topic=%s, partition=%d, errorCode=%d\n", 
                        topic, partition, errorCode);
                    partMap.put(partition, -1L); // 表示没有有效的committed offset
                }
            }
            result.put(topic, partMap);
        }
        return result;
    }

    private static String readString(ByteBuffer buffer) {
        short len = buffer.getShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
} 