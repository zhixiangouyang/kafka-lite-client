package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 用于组合record批次消息的编码类
 */
public class KafkaRecordEncoder {

    public static ByteBuffer encodeRecordBatch(String key, String value) {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = key != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        long timestamp = System.currentTimeMillis();

        // 1. 创建一个大缓冲区，构造batch
        ByteBuffer buf = ByteBuffer.allocate(1024);

        // ————————————————RecordBatch Header————————————————
        buf.putLong(0L);           //baseOffset
        int batchLenPos = buf.position();
        buf.putInt(0);             //batchLength(稍后回填)
        buf.putInt(0);             //partitionLeaderEpoch
        buf.put((byte) 2);               //magic
        buf.putInt(0);             //crc(暂时为0)
        buf.putShort((short) 0);         //attributes
        buf.putInt(0);             //lastOffsetDelta
        buf.putLong(timestamp);          //firstTimestamp
        buf.putLong(timestamp);          //maxTimestamp
        buf.putLong(-1L);          //producerId
        buf.putShort((short) -1);        //producerEpoch
        buf.putInt(-1);            //baseSequence
        putVarInt(buf, 1);            //record count = 1

        // ————————————————Record Entry————————————————
        ByteBuffer recordBuf = ByteBuffer.allocate(512);
        putVarInt(recordBuf, 0);       //attributes
        putVarLong(recordBuf, 0);      //timestampDelta
        putVarInt(recordBuf, 0);       //offsetDelta
        putVarInt(recordBuf, keyBytes == null ? -1 : keyBytes.length);
        if (keyBytes != null) recordBuf.put(keyBytes);
        putVarInt(recordBuf, valueBytes == null ? -1 : valueBytes.length);
        if (valueBytes != null) recordBuf.put(valueBytes);       //headers = empty

        recordBuf.flip();
        putVarInt(buf, recordBuf.remaining());  //record length
        buf.put(recordBuf);

        // ————————————————回填batchLength————————————————
        int endPos = buf.position();
        int batchLength = endPos - batchLenPos - 4;
        buf.putInt(batchLenPos, batchLength);

        buf.flip();
        return buf;
    }

    private static void putVarLong(ByteBuffer buf, int value) {
        while ((value & 0xFFFFFF80) != 0L) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte)(value & 0x7F));
    }

    private static void putVarInt(ByteBuffer buf, int value) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte)(value & 0x7F));
    }
}
