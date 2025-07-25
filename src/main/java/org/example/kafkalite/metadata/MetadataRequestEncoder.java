package org.example.kafkalite.metadata;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MetadataRequestEncoder {

    /**
     * 编码 MetadataRequest v0（基础版本，无tagged fields）
     */
    public static ByteBuffer encodeMetadataRequest(List<String> topics, int correlationId) {
        System.out.println("[MetadataRequestEncoder] Encoding request for topics: " + topics);
        
        // 计算总大小
        int totalSize = 0;
        
        // 固定头部
        totalSize += 4 + 2 + 2 + 4; // size + apiKey + apiVersion + correlationId
        
        // clientId
        String clientId = "kafka-lite";
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
        totalSize += 2 + clientIdBytes.length;

        // topics数组
        totalSize += 4; // topics数组长度
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            totalSize += 2 + topicBytes.length;
        }

        // 分配buffer
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // 写入固定头部
        buffer.putInt(totalSize - 4); // size
        buffer.putShort((short) 3); // apiKey = 3 (Metadata)
        buffer.putShort((short) 0); // apiVersion = 0
        buffer.putInt(correlationId);
        
        // 写入clientId
        if (clientIdBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("clientId too long: " + clientIdBytes.length + " bytes");
        }
        buffer.putShort((short) clientIdBytes.length);
        buffer.put(clientIdBytes);

        // 写入topics数组
        buffer.putInt(topics.size());
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            if (topicBytes.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("topic name too long: " + topicBytes.length + " bytes");
            }
            buffer.putShort((short) topicBytes.length);
            buffer.put(topicBytes);
        }

        buffer.flip();
        
        // 打印请求字节
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();
        System.out.println("[MetadataRequestEncoder] Request bytes: " + bytesToHex(bytes));
        
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