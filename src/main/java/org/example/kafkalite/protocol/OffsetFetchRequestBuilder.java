package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OffsetFetchRequestBuilder {
    public static ByteBuffer build(String groupId, Map<String, Integer[]> topicPartitions, int correlationId, String clientId) {
        int estimatedSize = 512;
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.position(4); // 预留4字节长度

        // 请求头 - 修改为v2版本
        short apiKey = 9;
        short apiVersion = 2;
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        putString(buffer, clientId);

        // 请求体
        putString(buffer, groupId);

        // topics array
        buffer.putInt(topicPartitions.size());
        for (Map.Entry<String, Integer[]> entry : topicPartitions.entrySet()) {
            String topic = entry.getKey();
            Integer[] partitions = entry.getValue();

            putString(buffer, topic);
            buffer.putInt(partitions.length);
            for (int partition : partitions) {
                buffer.putInt(partition);
            }
        }

        // 回填长度
        int endPos = buffer.position();
        int totalLen = endPos - 4;
        buffer.putInt(0, totalLen);
        buffer.flip();

        // 打印请求字节流
        byte[] reqBytes = new byte[buffer.remaining()];
        buffer.mark();
        buffer.get(reqBytes);
        buffer.reset();
//        System.out.print("[OffsetFetchRequestBuilder] 请求字节流: ");
//        for (byte b : reqBytes) System.out.printf("%02x ", b);
//        System.out.println();

        return buffer;
    }

    public static void putString(ByteBuffer buffer, String s) {
        if (s == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }
} 