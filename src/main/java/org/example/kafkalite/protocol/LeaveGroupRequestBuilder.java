package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LeaveGroupRequestBuilder {
    
    public static ByteBuffer build(String clientId, String groupId, String memberId) {
        try {
            // 计算总大小
            int totalSize = 0;
            byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
            byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
            byte[] memberIdBytes = memberId.getBytes(StandardCharsets.UTF_8);
            
            // 固定头部
            totalSize += 4 + 2 + 2 + 4; // size + apiKey + apiVersion + correlationId
            
            // clientId
            totalSize += 2 + clientIdBytes.length;
            
            // groupId
            totalSize += 2 + groupIdBytes.length;
            
            // memberId
            totalSize += 2 + memberIdBytes.length;
            
            // 分配buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
            // 写入固定头部
            buffer.putInt(totalSize - 4); // size
            buffer.putShort((short) 13); // apiKey = 13 (LeaveGroup)
            buffer.putShort((short) 0); // apiVersion = 0
            buffer.putInt(1); // correlationId
            
            // 写入clientId
            buffer.putShort((short) clientIdBytes.length);
            buffer.put(clientIdBytes);
            
            // 写入groupId
            buffer.putShort((short) groupIdBytes.length);
            buffer.put(groupIdBytes);
            
            // 写入memberId
            buffer.putShort((short) memberIdBytes.length);
            buffer.put(memberIdBytes);
            
            buffer.flip();
            
            // 打印请求字节流
            byte[] bytes = new byte[buffer.remaining()];
            buffer.mark();
            buffer.get(bytes);
            buffer.reset();
            System.out.print("[LeaveGroupRequestBuilder] 请求字节流: ");
            for (byte b : bytes) System.out.printf("%02x ", b);
            System.out.println();
            
            return buffer;
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to build LeaveGroup request", e);
        }
    }
} 