package org.example.kafkalite.metadata;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MetadataRequestEncoder {

    /**
     * 编码 MetadataRequest v0（基础版本，无tagged fields）
     */
    public static ByteBuffer encodeMetadataRequest(List<String> topics, int correlationId) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Topics list cannot be null or empty");
        }

        String clientId = "kafka-lite";
        short apiKey = 3;          // MetadataRequest
        short apiVersion = 0;      // 基础版本，无tagged fields

        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);

        // 预估buffer大小
        int estimatedSize = 4 + 2 + 2 + 4 + 2 + clientIdBytes.length + 4;
        for (String topic : topics) {
            if (topic == null) {
                throw new IllegalArgumentException("Topic name cannot be null");
            }
            estimatedSize += 2 + topic.getBytes(StandardCharsets.UTF_8).length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.position(4); // 预留长度字段

        // Request Header
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        buffer.putShort((short) clientIdBytes.length);
        buffer.put(clientIdBytes);

        // topics array
        buffer.putInt(topics.size());
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) topicBytes.length);
            buffer.put(topicBytes);
        }

        // Patch total length
        int endPos = buffer.position();
        int totalLen = endPos - 4;
        buffer.putInt(0, totalLen);
        buffer.flip();
        return buffer;
    }
}