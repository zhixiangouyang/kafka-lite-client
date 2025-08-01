package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class JoinGroupRequestBuilder {
    public static ByteBuffer build(String clientId, String groupId, String memberId, List<String> topics) {
        try {
            // 注释掉所有System.out.println和System.err.println日志，只保留异常抛出。
            
        // 计算总大小
        int totalSize = 0;
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] memberIdBytes = memberId.getBytes(StandardCharsets.UTF_8);
            byte[] protocolTypeBytes = "consumer".getBytes(StandardCharsets.UTF_8);
            byte[] protocolNameBytes = "range".getBytes(StandardCharsets.UTF_8);
        
        // 固定头部
            totalSize += 4 + 2 + 2 + 4; // size + apiKey + apiVersion + correlationId
        
        // clientId
        totalSize += 2 + clientIdBytes.length;
        
        // groupId
        totalSize += 2 + groupIdBytes.length;
        
        // sessionTimeout
            totalSize += 4;
            
            // rebalanceTimeout (v1+)
        totalSize += 4;
        
        // memberId
        totalSize += 2 + memberIdBytes.length;
        
        // protocolType
        totalSize += 2 + protocolTypeBytes.length;
        
            // groupProtocols array
        totalSize += 4;
        
            // protocol name
            totalSize += 2 + protocolNameBytes.length;
        
        // metadata
        ByteBuffer metadata = encodeMetadata(topics);
        totalSize += 4 + metadata.remaining();
        
        // 分配buffer
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // 写入固定头部
        buffer.putInt(totalSize - 4); // size
        buffer.putShort((short) 11); // apiKey = 11 (JoinGroup)
            buffer.putShort((short) 1); // apiVersion = 1
            buffer.putInt(1); // correlationId
        
        // 写入clientId
        buffer.putShort((short) clientIdBytes.length);
        buffer.put(clientIdBytes);
        
        // 写入groupId
        buffer.putShort((short) groupIdBytes.length);
        buffer.put(groupIdBytes);
        
        // 写入sessionTimeout
        buffer.putInt(30000); // 30s
            
            // 写入rebalanceTimeout
            buffer.putInt(300000); // 5min
        
        // 写入memberId
        buffer.putShort((short) memberIdBytes.length);
        buffer.put(memberIdBytes);
        
        // 写入protocolType
        buffer.putShort((short) protocolTypeBytes.length);
        buffer.put(protocolTypeBytes);
        
            // 写入groupProtocols array
        buffer.putInt(1); // array size = 1
        
            // 写入protocol name
            buffer.putShort((short) protocolNameBytes.length);
            buffer.put(protocolNameBytes);
        
        // 写入metadata
        buffer.putInt(metadata.remaining());
        buffer.put(metadata);
        
        buffer.flip();
            
        // 打印请求字节流
            byte[] bytes = new byte[buffer.remaining()];
        buffer.mark();
            buffer.get(bytes);
        buffer.reset();
        System.out.print("[JoinGroupRequestBuilder] 请求字节流: ");
        for (byte b : bytes) System.out.printf("%02x ", b);
        System.out.println();
            
        return buffer;
            
        } catch (Exception e) {
            // System.err.println("[JoinGroupRequestBuilder] Error building request: " + e.getMessage());
            throw e;
        }
    }
    
    private static ByteBuffer encodeMetadata(List<String> topics) {
        // 计算metadata大小
        int metadataSize = 2; // version
        metadataSize += 4; // topics array size
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            metadataSize += 2 + topicBytes.length;
        }
        metadataSize += 4; // user_data bytes length

        ByteBuffer metadata = ByteBuffer.allocate(metadataSize);

        // 写入version
        metadata.putShort((short) 0); // version = 0

        // 写入topics
        metadata.putInt(topics.size());
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            metadata.putShort((short) topicBytes.length);
            metadata.put(topicBytes);
        }

        // 写入user_data（空）
        metadata.putInt(0);

        metadata.flip();
        return metadata;
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xff));
        }
        return sb.toString();
    }
} 