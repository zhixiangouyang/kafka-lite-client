package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;

public class ProtocolUtils {

    /**
     * Kafka VarInt 编码（用于 Compact String、Compact Array 等字段）
     */
    public static void putVarInt(ByteBuffer buf, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) (value & 0x7F));
    }

    /**
     * Kafka VarLong 编码（主要用于 Record 中 timestampDelta、offsetDelta）
     */
    public static void putVarLong(ByteBuffer buf, long value) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) (value & 0x7F));
    }
}
