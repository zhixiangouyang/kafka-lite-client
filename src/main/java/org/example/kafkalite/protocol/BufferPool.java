package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简单的ByteBuffer对象池，用于减少GC压力
 * 设计原则：线程安全、自动管理、不影响现有功能
 */
public class BufferPool {
    private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger poolSize = new AtomicInteger(0);
    private final int maxPoolSize;
    private final int bufferSize;
    
    // 默认配置：适中的池大小，避免过度缓存
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int DEFAULT_MAX_POOL_SIZE = 100; // 最多缓存100个buffer
    
    // 全局实例 - 线程安全的单例
    private static volatile BufferPool instance;
    
    public static BufferPool getInstance() {
        if (instance == null) {
            synchronized (BufferPool.class) {
                if (instance == null) {
                    instance = new BufferPool(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE);
                }
            }
        }
        return instance;
    }
    
    public BufferPool(int bufferSize, int maxPoolSize) {
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
    }
    
    /**
     * 获取ByteBuffer，优先从池中获取，如果池为空则创建新的
     * @param minCapacity 最小容量需求
     * @return 可用的ByteBuffer
     */
    public ByteBuffer allocate(int minCapacity) {
        // 如果需求容量远大于池中buffer大小，直接分配新的
        if (minCapacity > bufferSize * 2) {
            return ByteBuffer.allocate(minCapacity);
        }
        
        ByteBuffer buffer = pool.poll();
        if (buffer != null) {
            poolSize.decrementAndGet();
            buffer.clear(); // 重置buffer状态
            
            // 如果池中的buffer太小，扩容
            if (buffer.capacity() < minCapacity) {
                return ByteBuffer.allocate(Math.max(minCapacity, bufferSize));
            }
            return buffer;
        }
        
        // 池为空，分配新buffer
        return ByteBuffer.allocate(Math.max(minCapacity, bufferSize));
    }
    
    /**
     * 归还ByteBuffer到池中
     * @param buffer 要归还的buffer
     */
    public void release(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        
        // 只回收适当大小的buffer，避免内存浪费
        if (buffer.capacity() >= bufferSize/2 && buffer.capacity() <= bufferSize * 4) {
            int currentSize = poolSize.get();
            if (currentSize < maxPoolSize && poolSize.compareAndSet(currentSize, currentSize + 1)) {
                buffer.clear(); // 清理buffer状态
                pool.offer(buffer);
                return;
            }
        }
        
        // 如果池已满或buffer大小不合适，让JVM自动回收
    }
    
    /**
     * 获取当前池大小 - 用于监控
     */
    public int getPoolSize() {
        return poolSize.get();
    }
    
    /**
     * 清空池 - 用于清理
     */
    public void clear() {
        pool.clear();
        poolSize.set(0);
    }
} 