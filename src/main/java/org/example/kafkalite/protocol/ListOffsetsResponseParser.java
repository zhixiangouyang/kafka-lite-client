package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ListOffsetsResponseParser {
    
    public static class OffsetInfo {
        private final short errorCode;
        private final long timestamp;
        private final long offset;
        
        public OffsetInfo(short errorCode, long timestamp, long offset) {
            this.errorCode = errorCode;
            this.timestamp = timestamp;
            this.offset = offset;
        }
        
        public short getErrorCode() { return errorCode; }
        public long getTimestamp() { return timestamp; }
        public long getOffset() { return offset; }
        
        @Override
        public String toString() {
            return String.format("OffsetInfo(errorCode=%d, timestamp=%d, offset=%d)", 
                errorCode, timestamp, offset);
        }
    }
    
    /**
     * 解析ListOffsets响应 (Version: 3)
     * @param buffer 响应数据
     * @return topic -> partition -> OffsetInfo 映射
     */
    public static Map<String, Map<Integer, OffsetInfo>> parse(ByteBuffer buffer) {
        Map<String, Map<Integer, OffsetInfo>> result = new HashMap<>();
        
        try {
            // 跳过总长度字段
            buffer.getInt();
            
            // 读取correlationId
            int correlationId = buffer.getInt();
            
            // 读取throttle_time_ms (Version 3新增)
            int throttleTimeMs = buffer.getInt();
            System.out.printf("[ListOffsetsResponseParser] throttle_time_ms=%d\n", throttleTimeMs);
            
            // 读取topics数组
            int topicCount = buffer.getInt();
            
            for (int i = 0; i < topicCount; i++) {
                String topic = readString(buffer);
                
                // 读取partitions数组
                int partitionCount = buffer.getInt();
                Map<Integer, OffsetInfo> partitionMap = new HashMap<>();
                
                for (int j = 0; j < partitionCount; j++) {
                    int partitionIndex = buffer.getInt();
                    short errorCode = buffer.getShort();
                    long timestamp = buffer.getLong();
                    long offset = buffer.getLong();
                    
                    OffsetInfo offsetInfo = new OffsetInfo(errorCode, timestamp, offset);
                    partitionMap.put(partitionIndex, offsetInfo);
                }
                
                result.put(topic, partitionMap);
            }
            
            System.out.printf("[ListOffsetsResponseParser] 解析结果: %s\n", result);
            
        } catch (Exception e) {
            System.err.printf("[ListOffsetsResponseParser] 解析异常: %s\n", e.getMessage());
            e.printStackTrace();
            throw e;
        }
        
        return result;
    }
    
    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
} 