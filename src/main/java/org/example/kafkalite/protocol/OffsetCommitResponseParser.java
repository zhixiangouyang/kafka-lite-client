package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class OffsetCommitResponseParser {
    /**
     * 解析 OffsetCommitResponse v0，返回 topic->partition->errorCode
     */
    public static Map<String, Map<Integer, Short>> parse(ByteBuffer buffer) {
        Map<String, Map<Integer, Short>> result = new HashMap<>();
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            String topic = readString(buffer);
            int partitionCount = buffer.getInt();
            Map<Integer, Short> partMap = new HashMap<>();
            for (int j = 0; j < partitionCount; j++) {
                int partition = buffer.getInt();
                short errorCode = buffer.getShort();
                partMap.put(partition, errorCode);
            }
            result.put(topic, partMap);
        }
        return result;
    }

    private static String readString(ByteBuffer buffer) {
        short len = buffer.getShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}