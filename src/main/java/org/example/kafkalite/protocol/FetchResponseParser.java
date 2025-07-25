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

        // 打印响应前32字节和剩余字节数
        int previewLen = Math.min(buffer.remaining(), 32);
        byte[] preview = new byte[previewLen];
        buffer.mark();
        buffer.get(preview);
        buffer.reset();
        System.out.print("[FetchResponseParser] 响应前32字节: ");
        for (byte b : preview) {
            System.out.printf("%02x ", b);
        }
        System.out.printf(" 剩余字节: %d\n", buffer.remaining());

        // 跳过total length字段
        buffer.getInt();
        int correlationId = buffer.getInt();  // 响应头

        int topicCount = buffer.getInt(); // int32
        System.out.printf("[FetchResponseParser] topicCount=%d\n", topicCount);
        for (int i = 0; i < topicCount; i++) {
            String topic = readString(buffer);
            int partitionCount = buffer.getInt(); // 必须用int32
            System.out.printf("[FetchResponseParser] topic=%s, partitionCount=%d\n", topic, partitionCount);
            for (int j = 0; j < partitionCount; j++) {
                int partition = buffer.getInt();
                short errorCode = buffer.getShort();
                long highWater = buffer.getLong();

                int messageSetSize = buffer.getInt();
                int messageSetEnd = buffer.position() + messageSetSize;
                System.out.printf("[FetchResponseParser] topic=%s, partition=%d, messageSetSize=%d, bufferPos=%d\n",
                    topic, partition, messageSetSize, buffer.position());

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

                    System.out.printf("[FetchResponseParser] offset=%d, messageSize=%d, keyLen=%d, valueLen=%d\n",
                        offset, messageSize, keyLen, valueLen);

                    records.add(new ConsumerRecord(topic, partition, offset, key, value));

                    // 跳到下一个消息
                    buffer.position(messageEnd);
                }
            }
        }
        return  records;
    }

    private static String readString(ByteBuffer buffer) {
        int posBefore = buffer.position();
        short len = buffer.getShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        String str = new String(bytes, StandardCharsets.UTF_8);
        System.out.printf("[readString] posBefore=%d, len=%d, posAfter=%d, str=%s\n", posBefore, len, buffer.position(), str);
        return str;
    }
}
