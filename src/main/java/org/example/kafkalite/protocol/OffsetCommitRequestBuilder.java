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

            putString(buffer, topic); // 修复：必须写入topic字段

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

        // 打印请求字节流
        // byte[] reqBytes = new byte[buffer.remaining()];
        // buffer.mark();
        // buffer.get(reqBytes);
        // buffer.reset();
        // System.out.print("[OffsetCommitRequestBuilder] 请求字节流: ");
        // for (byte b : reqBytes) System.out.printf("%02x ", b);
        // System.out.println();

        return buffer;
    }

    // v2协议实现
    public static ByteBuffer build(String groupId, Map<String, Map<Integer, Long>> offsets, int correlationId, String clientId, int generationId, String memberId, long retentionTime) {
        int estimatedSize = 512;
        // 新增详细参数日志
        System.out.printf("[OffsetCommitRequestBuilder] 参数: groupId=%s, generationId=%d, memberId=%s, retentionTime=%d, offsets=%s, apiVersion=%d, clientId=%s\n",
            groupId, generationId, memberId, retentionTime, offsets, 2, clientId);
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.position(4); // 预留4字节长度

        // 请求头
        short apiKey = 8;
        short apiVersion = 2;
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        putString(buffer, clientId);

        // 请求体
        putString(buffer, groupId);
        buffer.putInt(generationId);
        putString(buffer, memberId);
        buffer.putLong(retentionTime); // -1L表示broker默认

        // topics array
        buffer.putInt(offsets.size());
        for (Map.Entry<String, Map<Integer, Long>> entry : offsets.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, Long> partitionMap = entry.getValue();

            putString(buffer, topic);

            buffer.putInt(partitionMap.size());
            for (Map.Entry<Integer, Long> paritionEntry : partitionMap.entrySet()) {
                buffer.putInt(paritionEntry.getKey()); // partition
                buffer.putLong(paritionEntry.getValue()); // offset
                putString(buffer, "");   // metadata, 可为空
            }
        }

        // 回填长度
        int endPos = buffer.position();
        int totalLen = endPos - 4;
        buffer.putInt(0, totalLen);
        buffer.flip();

        // 打印请求字节流
        // byte[] reqBytes = new byte[buffer.remaining()];
        // buffer.mark();
        // buffer.get(reqBytes);
        // buffer.reset();
        // System.out.print("[OffsetCommitRequestBuilder] 请求字节流: ");
        // for (byte b : reqBytes) System.out.printf("%02x ", b);
        // System.out.println();

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
