package org.example.kafkalite.metadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MetadataResponseParser {

    public static Metadata parse(ByteBuffer buffer) {
        Map<Integer, BrokerInfo> brokers = new HashMap<>();
        Map<String, Map<Integer, Integer>> topicPartitionLeaders = new HashMap<>();

        int correlationId = buffer.getInt(); // 响应头

        // ———————— brokers ————————
        int brokerCount = buffer.getInt();
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = buffer.getInt();
            String host = readString(buffer);
            int port = buffer.getInt();
            skipNullableString(buffer); // 跳过rack
            brokers.put(nodeId, new BrokerInfo(nodeId, host, port));
        }

        // ———————— topics ————————
        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            buffer.getShort();   // 跳过 topic error code
            String topicName = readString(buffer);
            buffer.get(); // 跳过is_internal
            int partitionCount = buffer.getInt();
            Map<Integer, Integer> partitionToLeader = new HashMap<>();
            for (int j = 0; j < partitionCount; j++) {
                buffer.getShort();   // 跳过partition error code
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();
                skipIntArray(buffer);  // 跳过 replicas
                skipIntArray(buffer);  // 跳过 isr
                partitionToLeader.put(partitionId, leaderId);
            }
            topicPartitionLeaders.put(topicName, partitionToLeader);
        }
        return new Metadata(brokers, topicPartitionLeaders);
    }

    private static void skipIntArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        if (size < 0) return;;
        buffer.position(buffer.position() + size * 4);
    }

    private static void skipNullableString(ByteBuffer buffer) {
        short len = buffer.getShort();
        if (len >= 0) buffer.position(buffer.position() + len);
    }

    private static String readString(ByteBuffer buffer) {
        short len = buffer.getShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
