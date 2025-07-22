package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 用于发送消息的编码类
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
        recordBatch.get(recordBytes);   // 复制 recordBatch 数据

        // 预估缓冲区大小（冲突）
        ByteBuffer buf = ByteBuffer.allocate(1024 + recordBytes.length);
        buf.position(4);  // 先跳过4字节，用于后面回填 total length

        //————————————————————————Request Header————————————————————————
        short apiKey = 0; // ProduceRequest
        short apiVersion = 3;

        buf.putShort(apiKey);
        buf.putShort(apiVersion);
        buf.putInt(correlationId);

        buf.putShort((short) clientBytes.length);
        buf.put(clientBytes);

        //——————————————————————ProduceRequest Body——————————————————————

        // transactionalId => null
        buf.putShort((short) -1);

        // acks
        buf.putShort(acks); // 一般为1（leader响应）

        // timeout
        buf.putInt(timeoutMs); // 例如 3000

        // [topic_data] array size
        buf.putInt(1);  // 只有一个topic

        // topic name
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);

        // partition array size
        buf.putInt(1); // 只有一个分区

        // partitionId
        buf.putInt(partition);

        // recordBytes
        buf.putInt(recordBytes.length);
        buf.put(recordBytes);

        //—————————————————————回填length———————————————————————
        int endPos = buf.position();
        int length = endPos - 4;
        buf.putInt(0, length);

        buf.flip();
        return buf;

    }
}
