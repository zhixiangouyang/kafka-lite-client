package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;

public class HeartbeatResponseParser {
    public static short parse(ByteBuffer buffer) {
        try {
            // 调试：打印响应字节但不影响buffer的position
            byte[] allBytes = new byte[buffer.remaining()];
            int originalPosition = buffer.position();
            buffer.get(allBytes);
            buffer.position(originalPosition);
            
            // 跳过总长度字段（4字节）
            int totalSize = buffer.getInt();
            
            // 读取correlationId（4字节）
            int correlationId = buffer.getInt();
            
            // 读取errorCode（2字节 INT16）
            short errorCode = buffer.getShort();
            
            System.out.printf("[HeartbeatResponseParser] totalSize=%d, correlationId=%d, errorCode=%d\n", 
                totalSize, correlationId, errorCode);
                
            return errorCode;
            
        } catch (Exception e) {
            System.err.printf("[HeartbeatResponseParser] Error parsing response: %s\n", e.getMessage());
            System.err.printf("[HeartbeatResponseParser] Buffer position at error: %d, remaining: %d\n", 
                buffer.position(), buffer.remaining());
            e.printStackTrace();
            throw e;
        }
    }
} 