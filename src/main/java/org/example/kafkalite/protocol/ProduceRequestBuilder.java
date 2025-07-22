package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 用于发送消息的编码类（v0版本）
 */
public class ProduceRequestBuilder {

    public static ByteBuffer build(String clientId,
                                 String topic,
                                 int partition,
                                 ByteBuffer recordBatch,
                                 short acks,
                                 int timeoutMs,
                                 int correlationId) {
        byte[] clientBytes = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] recordBytes = new byte[recordBatch.remaining()];
        recordBatch.get(recordBytes);

        // 预估缓冲区大小
        ByteBuffer buf = ByteBuffer.allocate(1024 + recordBytes.length);
        buf.position(4);  // 预留4字节用于后面回填 total length

        // Request Header
        short apiKey = 0; // ProduceRequest
        short apiVersion = 0; // 基础版本

        buf.putShort(apiKey);
        buf.putShort(apiVersion);
        buf.putInt(correlationId);
        buf.putShort((short) clientBytes.length);
        buf.put(clientBytes);

        // Request Body
        buf.putShort(acks);
        buf.putInt(timeoutMs);

        // topics array
        buf.putInt(1); // 只有一个topic
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);

        // partitions array
        buf.putInt(1); // 只有一个partition
        buf.putInt(partition);

        // message set
        buf.putInt(recordBytes.length);
        buf.put(recordBytes);

        // 回填length
        int endPos = buf.position();
        int length = endPos - 4;
        buf.putInt(0, length);

        buf.flip();
        return buf;
    }
}
