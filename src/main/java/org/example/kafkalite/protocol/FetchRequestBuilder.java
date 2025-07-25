package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FetchRequestBuilder {
    /**
     * 构造 FetchRequest v0
     * @param clientId 客户端id
     * @param topic 拉取的topic
     * @param partition 分区号
     * @param fetchOffset 从哪个offset开始拉
     * @param maxBytes 本次最多拉多少字节
     * @param correlationId 协议相关id
     * @return ByteBuffer
     */
    public static ByteBuffer build(String clientId, String topic, int partition, long fetchOffset, int maxBytes, int correlationId) {
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int estimatedSize = 4 + 2 + 2 + 4 + 2 + clientIdBytes.length + 4 + 4 + 4 + 4 + 2 + topicBytes.length + 4 + 4 + 8 + 4;
        ByteBuffer buf = ByteBuffer.allocate(estimatedSize);
        buf.position(4);   // 预留4字节填充长度

        // 1. 请求头
        short apiKey = 1;   // FetchRequest
        short apiVersion = 0;
        buf.putShort(apiKey);
        buf.putShort(apiVersion);
        buf.putInt(correlationId);

        // clientId (int16长度+内容)
        buf.putShort((short) clientIdBytes.length);
        buf.put(clientIdBytes);

        // 2. 请求体
        buf.putInt(-1); // replicaId, -1表示不同消费者
        buf.putInt(1000);  // maxWaitTime, 最大等待时间ms
        buf.putInt(1);     // minBytes, 最小返回字节数

        // topics array
        buf.putInt(1);   // 只拉一个topic
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);

        // partitions array
        buf.putInt(1);   // 只拉一个分区
        buf.putInt(partition);
        buf.putLong(fetchOffset);
        buf.putInt(maxBytes);

        // 回填长度
        int endPos = buf.position();
        int totalLen = endPos - 4;
        buf.putInt(0, totalLen);
        buf.flip();
        return buf;
    }

    // 写入Kafka协议的可变长度string类型（int16长度+字节内容）
    private static void putString(ByteBuffer buffer, String s) {
        if (s == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("String too long: " + bytes.length + " bytes");
            }
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }
}
