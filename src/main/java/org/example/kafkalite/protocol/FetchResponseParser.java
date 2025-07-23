package org.example.kafkalite.protocol;

import org.example.kafkalite.consumer.ConsumerRecord;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FetchResponseParser {
    /**
     * 解析 FetchResponse v0， 返回 ConsumerRecord 列表
     */

    public static List<ConsumerRecord> parse(ByteBuffer buffer) {
        List<ConsumerRecord> records = new ArrayList<>();

        int correlationId = buffer.getInt();  // 响应头

        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            String topic =readString(buffer);

            int partitionCount = buffer.getInt();
            for (int j = 0; j < partitionCount; j++) {
                int partition = buffer.getInt();
                short errorCode = buffer.getShort();
                long highWater = buffer.getLong();

                int messageSetSize = buffer.getInt();
                int messageSetEnd = buffer.position() + messageSetSize;

                while (buffer.position() < messageSetEnd) {
                    long offset = buffer.getLong();
                    int messageSize = buffer.getInt();
                    int messageEnd = buffer.position() + messageSize;

                    int crc = buffer.getInt();
                    byte magic = buffer.get();
                    byte attributes = buffer.get();

                    // key
                    int keyLen = buffer.getInt();
                    String key = null;
                    if (keyLen >= 0) {
                        byte[] keyBytes = new byte[keyLen];
                        buffer.get(keyBytes);
                        key = new String(keyBytes, StandardCharsets.UTF_8);
                    }

                    // value
                    int valueLen = buffer.getInt();
                    String value = null;
                    if (valueLen > 0) {
                        byte[] valueBytes = new byte[valueLen];
                        buffer.get(valueBytes);
                        value = new String(valueBytes, StandardCharsets.UTF_8);
                    }

                    records.add(new ConsumerRecord(topic, partition, offset, key, value));

                    // 跳到下一个消息
                    buffer.position(messageEnd);
                }
            }
        }
        return  records;
    }

    private static String readString(ByteBuffer buffer) {
        short len = buffer.getShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
