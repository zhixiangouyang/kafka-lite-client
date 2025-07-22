package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 用于编码消息的工具类（v0版本）
 */
public class KafkaRecordEncoder {

    public static ByteBuffer encodeRecordBatch(String key, String value) {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        long timestamp = System.currentTimeMillis();

        // v0 消息格式：
        // offset: int64
        // messageSize: int32
        // crc: int32
        // magic: int8 (0 for v0)
        // attributes: int8 (0 for no compression)
        // key: bytes (int32 + data)
        // value: bytes (int32 + data)

        // 计算消息大小
        int messageSize = 1 + 1; // magic + attributes
        messageSize += 4 + (keyBytes != null ? keyBytes.length : 0); // key
        messageSize += 4 + (valueBytes != null ? valueBytes.length : 0); // value

        ByteBuffer buf = ByteBuffer.allocate(8 + 4 + 4 + messageSize); // offset + size + crc + message

        // offset (会被broker重写)
        buf.putLong(0L);

        // message size
        buf.putInt(messageSize + 4); // 加4是因为要包含crc

        // 记住crc位置
        int crcPosition = buf.position();
        buf.putInt(0); // 占位，之后回填

        // message
        buf.put((byte) 0); // magic = 0
        buf.put((byte) 0); // attributes = 0 (no compression)

        // key
        if (keyBytes != null) {
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
        } else {
            buf.putInt(-1);
        }

        // value
        if (valueBytes != null) {
            buf.putInt(valueBytes.length);
            buf.put(valueBytes);
        } else {
            buf.putInt(-1);
        }

        // 计算并回填crc
        byte[] messageBytes = new byte[messageSize];
        buf.position(crcPosition + 4);
        buf.get(messageBytes);

        CRC32 crc32 = new CRC32();
        crc32.update(messageBytes);
        buf.putInt(crcPosition, (int) crc32.getValue());

        buf.flip();
        return buf;
    }
}
