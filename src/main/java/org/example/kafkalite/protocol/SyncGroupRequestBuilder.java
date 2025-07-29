package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SyncGroupRequestBuilder {
    
    public static ByteBuffer build(String clientId, String groupId, int generationId, String memberId, List<String> topics) {
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
                
                // 计算分配方案大小
                int assignmentSize = 2; // version
                assignmentSize += 4; // topic count
                for (String topic : topics) {
                    byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
                    assignmentSize += 2 + topicBytes.length; // topic
                    assignmentSize += 4; // partition count
                    assignmentSize += 4; // partition
                }
                assignmentSize += 4; // user_data bytes length
                
                totalSize += 4; // groupAssignment array length
                totalSize += 2 + memberIdBytes.length; // member id in assignment
                totalSize += 4; // assignment size
                totalSize += assignmentSize; // assignment bytes
            
            // 分配buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
                // 写入请求头
            buffer.putInt(totalSize - 4); // size
            buffer.putShort((short) 14); // apiKey = 14 (SyncGroup)
            buffer.putShort((short) 0); // apiVersion = 0
                buffer.putInt(2); // correlationId
            
                // 写入基础字段
            buffer.putShort((short) clientIdBytes.length);
            buffer.put(clientIdBytes);
            
            buffer.putShort((short) groupIdBytes.length);
            buffer.put(groupIdBytes);
            
            buffer.putInt(generationId);
            
                buffer.putShort((short) memberIdBytes.length);
                buffer.put(memberIdBytes);
                
                // 写入分组分配
                buffer.putInt(1); // groupAssignment array length = 1
                
                // 写入成员ID
            buffer.putShort((short) memberIdBytes.length);
            buffer.put(memberIdBytes);
            
                // 写入分配方案
                buffer.putInt(assignmentSize); // assignment size
                
                // 写入分配内容
                buffer.putShort((short) 0); // version = 0
                buffer.putInt(topics.size()); // topic count
                
                for (String topic : topics) {
                    byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
                    buffer.putShort((short) topicBytes.length);
                    buffer.put(topicBytes);
                    buffer.putInt(1); // partition count = 1
                    buffer.putInt(0); // partition = 0
                }
                // 写入user_data（空）
                buffer.putInt(0);
            
            buffer.flip();
                
                // 打印请求字节
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                buffer.rewind();
                // System.out.println("[SyncGroupRequestBuilder] Request bytes: " + bytesToHex(bytes));
                
            return buffer;
                
        } catch (Exception e) {
            // System.err.println("[SyncGroupRequestBuilder] Error building request: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to build SyncGroup request", e);
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