package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FindCoordinatorRequestBuilder {
    /**
     * 构造 FindCoordinatorRequest v0
     * @param clientId 客户端id
     * @param groupId 消费者组id
     * @param correlationId 协议相关id
     * @return ByteBuffer
     */
    public static ByteBuffer build(String clientId, String groupId, int correlationId) {
        // 计算总大小
        int totalSize = 0;
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
        
        // 固定头部
        totalSize += 4 + 2 + 2 + 4; // size + apiKey + apiVersion + correlationId
        
        // clientId
        totalSize += 2 + clientIdBytes.length;
        
        // groupId
        totalSize += 2 + groupIdBytes.length;
        
        // 分配buffer
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // 写入固定头部
        buffer.putInt(totalSize - 4); // size
        buffer.putShort((short) 10); // apiKey = 10 (FindCoordinator)
        buffer.putShort((short) 0); // apiVersion = 0
        buffer.putInt(correlationId);
        
        // 写入clientId
        if (clientIdBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("clientId too long: " + clientIdBytes.length + " bytes");
        }
        buffer.putShort((short) clientIdBytes.length);
        buffer.put(clientIdBytes);
        
        // 写入groupId
        if (groupIdBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("groupId too long: " + groupIdBytes.length + " bytes");
        }
        buffer.putShort((short) groupIdBytes.length);
        buffer.put(groupIdBytes);
        
        buffer.flip();
        
        // 打印请求字节
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();
        // System.out.println("[FindCoordinatorRequestBuilder] Request bytes: " + bytesToHex(bytes));
        
        return buffer;
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xff));
        }
        return sb.toString();
    }
} 