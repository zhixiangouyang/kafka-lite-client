package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.CRC32;
import java.util.zip.GZIPOutputStream;
import java.io.ByteArrayOutputStream;

import org.example.kafkalite.producer.ProducerRecord;
import org.xerial.snappy.Snappy;

/**
 * 用于编码消息的工具类（v0版本）
 */
public class KafkaRecordEncoder {

    // Kafka默认消息大小限制，通常为1MB
    private static final int MAX_MESSAGE_BYTES = 1024 * 1024;
    // 单条消息大小警告阈值
    private static final int MESSAGE_SIZE_WARNING_THRESHOLD = 100 * 1024; // 100KB
    
    // ThreadLocal缓存：每个线程重用字节数组，避免重复分配
    private static final ThreadLocal<byte[]> KEY_BYTES_CACHE = ThreadLocal.withInitial(() -> new byte[4096]);
    private static final ThreadLocal<byte[]> VALUE_BYTES_CACHE = ThreadLocal.withInitial(() -> new byte[4096]);
    
    /**
     * 安全获取字符串的字节数组，重用ThreadLocal缓存
     */
    private static byte[] getStringBytes(String str, boolean isKey) {
        if (str == null) {
            return null;
        }
        
        byte[] stringBytes = str.getBytes(StandardCharsets.UTF_8);
        int requiredSize = stringBytes.length;
        
        // 如果字符串太大，直接返回新数组，不缓存
        if (requiredSize > 8192) { // 8KB阈值
            return stringBytes;
        }
        
        // 从ThreadLocal缓存获取数组
        ThreadLocal<byte[]> cache = isKey ? KEY_BYTES_CACHE : VALUE_BYTES_CACHE;
        byte[] cachedArray = cache.get();
        
        // 如果缓存数组太小，扩容
        if (cachedArray.length < requiredSize) {
            cachedArray = new byte[Math.max(requiredSize * 2, 4096)];
            cache.set(cachedArray);
        }
        
        // 复制数据到缓存数组的前部分
        System.arraycopy(stringBytes, 0, cachedArray, 0, requiredSize);
        
        // 返回正确长度的新数组
        byte[] result = new byte[requiredSize];
        System.arraycopy(cachedArray, 0, result, 0, requiredSize);
        return result;
    }

    public static ByteBuffer encodeRecordBatch(String key, String value) {
        byte[] keyBytes = getStringBytes(key, true);
        byte[] valueBytes = getStringBytes(value, false);
        
        // 检查消息大小
        int messageSize = 1 + 1; // magic + attributes
        messageSize += 4 + (keyBytes != null ? keyBytes.length : 0); // key
        messageSize += 4 + (valueBytes != null ? valueBytes.length : 0); // value
        
        // 如果消息太大，打印警告
        if (messageSize > MESSAGE_SIZE_WARNING_THRESHOLD) {
            System.out.printf("警告: 消息大小(%d字节)超过警告阈值(%d字节)%n", 
                messageSize, MESSAGE_SIZE_WARNING_THRESHOLD);
        }

        // v0 消息格式：
        // offset: int64
        // messageSize: int32
        // crc: int32
        // magic: int8 (0 for v0)
        // attributes: int8 (0 for no compression)
        // key: bytes (int32 + data)
        // value: bytes (int32 + data)

        // 使用对象池分配ByteBuffer
        BufferPool bufferPool = BufferPool.getInstance();
        ByteBuffer buf = bufferPool.allocate(8 + 4 + 4 + messageSize); // offset + size + crc + message

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
    
    /**
     * 批量编码多条消息
     * 
     * @param records 待编码的消息列表
     * @return 编码后的ByteBuffer
     */
    public static ByteBuffer encodeBatchMessages(List<ProducerRecord> records) {
        if (records == null || records.isEmpty()) {
            return ByteBuffer.allocate(0);
        }
        
        if (records.size() == 1) {
            ProducerRecord record = records.get(0);
            return encodeRecordBatch(record.getKey(), record.getValue());
        }
        
        // 计算总大小并检查每条消息
        int totalSize = 0;
        List<ByteBuffer> validMessages = new ArrayList<>();
        
        for (ProducerRecord record : records) {
            ByteBuffer encoded = encodeRecordBatch(record.getKey(), record.getValue());
            int messageSize = encoded.remaining();
            
            // 检查单条消息是否超过限制
            if (messageSize > MAX_MESSAGE_BYTES) {
                System.err.println("警告: 消息大小(" + messageSize + "字节)超过限制(" + MAX_MESSAGE_BYTES + "字节)，已跳过");
                continue;
            }
            
            // 检查累计大小是否超过限制
            if (totalSize + messageSize > MAX_MESSAGE_BYTES) {
                // 如果添加此消息会超过限制，停止添加
                System.err.println("警告: 批量消息累计大小将超过限制(" + MAX_MESSAGE_BYTES + "字节)，剩余消息将在下一批次发送");
                break;
            }
            
            validMessages.add(encoded);
            totalSize += messageSize;
        }
        
        // 使用对象池分配批量缓冲区
        BufferPool bufferPool = BufferPool.getInstance();
        ByteBuffer batchBuffer = bufferPool.allocate(totalSize);
        
        // 复制所有有效消息
        for (ByteBuffer message : validMessages) {
            ByteBuffer duplicate = message.duplicate();
            duplicate.rewind(); // 确保position在开始位置
            batchBuffer.put(duplicate);
        }
        
        batchBuffer.flip();
        return batchBuffer;
    }
    
    /**
     * 优化版批量编码 - 直接构建批量消息，减少中间ByteBuffer的创建
     * 适用于发送大量小消息的场景
     */
    public static ByteBuffer encodeBatchMessagesOptimized(List<ProducerRecord> records) {
        if (records == null || records.isEmpty()) {
            return ByteBuffer.allocate(0);
        }
        
        if (records.size() == 1) {
            ProducerRecord record = records.get(0);
            return encodeRecordBatch(record.getKey(), record.getValue());
        }
        
        // 首先计算总大小
        int totalSize = 0;
        int validCount = 0;
        
        // 预分配数组存储每条消息的信息
        byte[][] keyBytesArray = new byte[records.size()][];
        byte[][] valueBytesArray = new byte[records.size()][];
        int[] messageSizes = new int[records.size()];
        
        for (int i = 0; i < records.size(); i++) {
            ProducerRecord record = records.get(i);
            byte[] keyBytes = getStringBytes(record.getKey(), true);
            byte[] valueBytes = getStringBytes(record.getValue(), false);
            
            // 计算单条消息大小
            int messageSize = 1 + 1; // magic + attributes
            messageSize += 4 + (keyBytes != null ? keyBytes.length : 0); // key
            messageSize += 4 + (valueBytes != null ? valueBytes.length : 0); // value
            
            // 每条消息的总大小包括offset(8) + size(4) + crc(4) + message内容
            int totalMessageSize = 8 + 4 + 4 + messageSize;
            
            // 检查单条消息是否超过限制
            if (totalMessageSize > MAX_MESSAGE_BYTES) {
                System.err.println("警告: 消息大小(" + totalMessageSize + "字节)超过限制(" + MAX_MESSAGE_BYTES + "字节)，已跳过");
                continue;
            }
            
            // 检查累计大小是否超过限制
            if (totalSize + totalMessageSize > MAX_MESSAGE_BYTES) {
                // 如果添加此消息会超过限制，停止添加
                System.err.println("警告: 批量消息累计大小将超过限制(" + MAX_MESSAGE_BYTES + "字节)，剩余消息将在下一批次发送");
                break;
            }
            
            // 存储消息信息
            keyBytesArray[validCount] = keyBytes;
            valueBytesArray[validCount] = valueBytes;
            messageSizes[validCount] = messageSize;
            
            totalSize += totalMessageSize;
            validCount++;
        }
        
        // 如果没有有效消息，返回空缓冲区
        if (validCount == 0) {
            return ByteBuffer.allocate(0);
        }
        
        // 使用对象池分配批量缓冲区  
        BufferPool bufferPool = BufferPool.getInstance();
        ByteBuffer batchBuffer = bufferPool.allocate(totalSize);
        
        // 直接构建批量消息
        for (int i = 0; i < validCount; i++) {
            byte[] keyBytes = keyBytesArray[i];
            byte[] valueBytes = valueBytesArray[i];
            int messageSize = messageSizes[i];
            
            // offset (会被broker重写)
            batchBuffer.putLong(0L);
            
            // message size
            batchBuffer.putInt(messageSize + 4); // 加4是因为要包含crc
            
            // 记住crc位置
            int crcPosition = batchBuffer.position();
            batchBuffer.putInt(0); // 占位，之后回填
            
            // 记住消息开始位置
            int messageStart = batchBuffer.position();
            
            // message
            batchBuffer.put((byte) 0); // magic = 0
            batchBuffer.put((byte) 0); // attributes = 0 (no compression)
            
            // key
            if (keyBytes != null) {
                batchBuffer.putInt(keyBytes.length);
                batchBuffer.put(keyBytes);
            } else {
                batchBuffer.putInt(-1);
            }
            
            // value
            if (valueBytes != null) {
                batchBuffer.putInt(valueBytes.length);
                batchBuffer.put(valueBytes);
            } else {
                batchBuffer.putInt(-1);
            }
            
            // 计算并回填crc
            int messageEnd = batchBuffer.position();
            byte[] messageBytes = new byte[messageEnd - messageStart];
            
            // 保存当前位置
            int currentPos = batchBuffer.position();
            
            // 读取消息内容用于计算CRC
            batchBuffer.position(messageStart);
            batchBuffer.get(messageBytes);
            
            // 计算CRC
            CRC32 crc32 = new CRC32();
            crc32.update(messageBytes);
            
            // 回填CRC
            batchBuffer.position(crcPosition);
            batchBuffer.putInt((int) crc32.getValue());
            
            // 恢复位置
            batchBuffer.position(currentPos);
        }
        
        batchBuffer.flip();
        return batchBuffer;
    }

    public static ByteBuffer encodeBatchMessagesOptimized(List<ProducerRecord> records, String compressionType) {
        if (records == null || records.isEmpty()) {
            return ByteBuffer.allocate(0);
        }
        if (records.size() == 1) {
            ProducerRecord record = records.get(0);
            return encodeRecordBatch(record.getKey(), record.getValue());
        }
        // 1. 组装多条普通消息（magic=0, attributes=0, key, value...）为消息集合
        // 修复：调用无压缩版本，避免递归调用
        ByteBuffer messageSetBuffer = encodeBatchMessages(records);
        byte[] messageSet = new byte[messageSetBuffer.remaining()];
        messageSetBuffer.get(messageSet);
        // 2. 压缩消息集合
        byte[] compressed = messageSet;
        byte attributes = 0; // 0: none, 1: gzip, 2: snappy
        if ("gzip".equalsIgnoreCase(compressionType)) {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(bos);
                gzip.write(messageSet);
                gzip.close();
                compressed = bos.toByteArray();
                attributes = 1;
            } catch (Exception e) {
                System.err.println("GZIP压缩失败: " + e.getMessage());
                compressed = messageSet;
                attributes = 0;
            }
        } else if ("snappy".equalsIgnoreCase(compressionType)) {
            try {
                compressed = Snappy.compress(messageSet);
                attributes = 2;
            } catch (Exception e) {
                System.err.println("Snappy压缩失败: " + e.getMessage());
                compressed = messageSet;
                attributes = 0;
            }
        }
        // 3. 构造外层消息（magic=0, attributes=压缩类型, key=null, value=compressed）
        int messageSize = 1 + 1 + 4 + 4 + compressed.length; // magic + attributes + key(-1) + value.length + value
        int totalSize = 8 + 4 + 4 + messageSize; // offset + size + crc + message
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putLong(0L); // offset
        buf.putInt(messageSize + 4); // size (包含crc)
        int crcPosition = buf.position();
        buf.putInt(0); // crc占位
        int messageStart = buf.position();
        buf.put((byte)0); // magic = 0
        buf.put(attributes); // attributes = 压缩类型
        buf.putInt(-1); // key=null
        buf.putInt(compressed.length);
        buf.put(compressed);
        // 4. 计算并回填crc
        int messageEnd = buf.position();
        byte[] messageBytes = new byte[messageEnd - messageStart];
        buf.position(messageStart);
        buf.get(messageBytes);
        CRC32 crc32 = new CRC32();
        crc32.update(messageBytes);
        buf.putInt(crcPosition, (int)crc32.getValue());
        buf.position(messageEnd);
        buf.flip();
        return buf;
    }
}
