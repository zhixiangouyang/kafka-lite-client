package org.example.kafkalite.metadata;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KafkaRequestEncoder {

    /**
     * 构造 MetadataRequest v9 请求头 + 请求体
     */
    public static ByteBuffer encodeMetadataRequest(List<String> topics, int correlationId) {
        String clientId = "kafka-lite";
        short apiKey = 3;
        short apiVersion = 9;

        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);

        // 预估 buffer 大小；足够容纳 header + topics
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        //———————————————————Request Header———————————————————
        buffer.position(4);

        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);

        buffer.putShort((short) clientIdBytes.length); // clientId长度
        buffer.put(clientIdBytes);                     // clientId内容

        //———————————————————Request Body———————————————————
        // topics array
        buffer.putInt(topics.size());
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) topicBytes.length);// string length
            buffer.put(topicBytes);                    // string bytes
        }

        // allow_auto_topic_creation(boolean as byte)
        buffer.put((byte) 1); //true

        //———————————————————Patch Length———————————————————
        int endPos = buffer.position();
        int length = endPos - 4;
        buffer.putInt(0, length);   // 回填长度

        buffer.flip();
        return buffer;
    }
}
