package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HeartbeatRequestBuilder {
    
    public static ByteBuffer build(String clientId, String groupId, int generationId, String memberId) {
        try {
            // 计算总大小
            int totalSize = 0;
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] memberIdBytes = memberId.getBytes(StandardCharsets.UTF_8);

            // 基础字段大小
            totalSize += 4; // size
            totalSize += 2; // apiKey
            totalSize += 2; // apiVersion
            totalSize += 4; // correlationId
            totalSize += 2 + clientIdBytes.length; // clientId
            totalSize += 2 + groupIdBytes.length; // groupId
            totalSize += 4; // generationId
            totalSize += 2 + memberIdBytes.length; // memberId
            
            // 分配buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
            // 写入请求头
            buffer.putInt(totalSize - 4); // size
            buffer.putShort((short) 12); // apiKey = 12 (Heartbeat)
            buffer.putShort((short) 0); // apiVersion = 0
            buffer.putInt(3); // correlationId
            
            // 写入基础字段
        buffer.putShort((short) clientIdBytes.length);
        buffer.put(clientIdBytes);

        buffer.putShort((short) groupIdBytes.length);
        buffer.put(groupIdBytes);
            
        buffer.putInt(generationId);
            
        buffer.putShort((short) memberIdBytes.length);
        buffer.put(memberIdBytes);

        buffer.flip();
            
            // 打印请求字节
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            buffer.rewind();
            // System.out.println("[HeartbeatRequestBuilder] Request bytes: " + bytesToHex(bytes));
            
        return buffer;
            
        } catch (Exception e) {
            // System.err.println("[HeartbeatRequestBuilder] Error building request: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to build Heartbeat request", e);
        }
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xff));
        }
        return sb.toString();
    }
} 