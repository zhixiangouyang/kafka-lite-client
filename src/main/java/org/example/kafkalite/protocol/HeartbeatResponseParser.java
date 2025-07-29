package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;

public class HeartbeatResponseParser {
    public static short parse(ByteBuffer buffer) {
        // 跳过correlationId
        buffer.getInt();
        
        // 读取errorCode
        return buffer.getShort();
    }
} 