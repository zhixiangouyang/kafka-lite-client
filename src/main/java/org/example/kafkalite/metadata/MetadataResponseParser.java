package org.example.kafkalite.metadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于解析metadata响应的解码类（v0版本）
 */
public class MetadataResponseParser {

    public static Metadata parse(ByteBuffer buffer) {
        Map<Integer, BrokerInfo> brokers = new HashMap<>();
        Map<String, Map<Integer, Integer>> topicPartitionLeaders = new HashMap<>();

        int correlationId = buffer.getInt();

        // brokers array
        int brokerCount = buffer.getInt();
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = buffer.getInt();
            String host = readString(buffer);
            int port = buffer.getInt();
            // v0版本没有rack字段
            brokers.put(nodeId, new BrokerInfo(nodeId, host, port));
        }

        // topics array
        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            short errorCode = buffer.getShort();
            String topicName = readString(buffer);
            
            Map<Integer, Integer> partitionToLeader = new HashMap<>();
            int partitionCount = buffer.getInt();
            
            for (int j = 0; j < partitionCount; j++) {
                short partitionError = buffer.getShort();
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();
                
                if (partitionError == 0) {
                    partitionToLeader.put(partitionId, leaderId);
                }
                
                // replicas array
                skipIntArray(buffer);
                // isr array
                skipIntArray(buffer);
            }
            
            if (errorCode == 0) {
                topicPartitionLeaders.put(topicName, partitionToLeader);
            }
        }

        return new Metadata(brokers, topicPartitionLeaders);
    }

    private static void skipIntArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        if (size > 0) {
            buffer.position(buffer.position() + size * 4);
        }
    }

    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
