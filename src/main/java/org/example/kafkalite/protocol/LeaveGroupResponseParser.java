package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;

public class LeaveGroupResponseParser {
    
    public static short parse(ByteBuffer buffer) {
        try {
            // 跳过总长度
            int totalSize = buffer.getInt();
            
            // 读取correlationId
            int correlationId = buffer.getInt();
            
            // 读取errorCode
            short errorCode = buffer.getShort();
            
            return errorCode;
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse LeaveGroup response", e);
        }
    }
} 