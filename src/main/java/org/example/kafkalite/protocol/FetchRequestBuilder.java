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
        return build(clientId, topic, partition, fetchOffset, maxBytes, correlationId, 5000);
    }
    
    public static ByteBuffer build(String clientId, String topic, int partition, long fetchOffset, int maxBytes, int correlationId, int maxWaitTimeMs) {
        int estimatedSize = 256;
        ByteBuffer buf = ByteBuffer.allocate(estimatedSize);
        buf.position(4);   // 预留4字节填充长度

        // 1. 请求头
        short apiKey = 1;   // FetchRequest
        short apiVersion = 0;
        buf.putShort(apiKey);
        buf.putShort(apiVersion);
        buf.putInt(correlationId);

        // clientId (int16长度+内容)
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) clientIdBytes.length);
        buf.put(clientIdBytes);

        // 2. 请求体
        buf.putInt(-1); //replicaId, -1表示不同消费者
        buf.putInt(maxWaitTimeMs);  // 使用可配置的maxWaitTime
        buf.putInt(1);     // minBytes, 最小返回字节数

        // topics array
        buf.putInt(1);   // 只拉一个topic
        putString(buf, topic);

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

        // 打印所有参数和最终字节流
//        System.out.printf("[FetchRequestBuilder] 构造参数: clientId=%s, topic=%s, partition=%d, fetchOffset=%d, maxBytes=%d, correlationId=%d\n",
//            clientId, topic, partition, fetchOffset, maxBytes, correlationId);
        byte[] bytes = new byte[buf.remaining()];
        buf.mark();
        buf.get(bytes);
        buf.reset();
//        System.out.print("[FetchRequestBuilder] 请求字节流: ");
//        for (byte b : bytes) {
//            System.out.printf("%02x ", b);
//        }
//        System.out.println();

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
