package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OffsetCommitRequestBuilder {
    public static ByteBuffer build(String groupId, Map<String, Map<Integer, Long>> offsets, int correlationId, String clientId) {
        int estimatedSize = 512;
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.position(4); // 预留4字节长度

        // 请求头
        short apiKey = 8;
        short apiVersion = 0;
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        putString(buffer, clientId);

        // 请求体
        putString(buffer, groupId);

        // topics array
        buffer.putInt(offsets.size());
        for (Map.Entry<String, Map<Integer, Long>> entry : offsets.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, Long> partitionMap = entry.getValue();

            // partitions array
            buffer.putInt(partitionMap.size());
            for (Map.Entry<Integer, Long> paritionEntry : partitionMap.entrySet()) {
                buffer.putInt(paritionEntry.getKey()); // partition
                buffer.putLong(paritionEntry.getValue()); // offset
                putString(buffer, "");   // metadata, v0必须有但可以为空
            }
        }

        // 回填长度
        int endPos = buffer.position();
        int totalLen = endPos - 4;
        buffer.putInt(0, totalLen);
        buffer.flip();
        return buffer;
    }

    public static void putString(ByteBuffer buffer, String s) {
        if (s == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) s.length());
            buffer.get(bytes);
        }
    }
}
