package org.example.kafkalite.client;

import org.example.kafkalite.protocol.BufferPool;

/**
 * 优化效果监控类
 */
public class OptimizationTestMonitor {
    
    public static void printOptimizationStats() {
        System.out.println("=== 优化效果监控 ===");
        
        // ByteBuffer池状态
        BufferPool bufferPool = BufferPool.getInstance();
        System.out.printf("📊 ByteBuffer池大小: %d\n", bufferPool.getPoolSize());
        
        // JVM内存信息
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();
        
        System.out.printf("💾 JVM内存使用: %.1fMB / %.1fMB (%.1f%%)\n", 
            usedMemory / 1024.0 / 1024.0,
            maxMemory / 1024.0 / 1024.0,
            (usedMemory * 100.0) / maxMemory);
        
        // GC建议
        System.gc(); // 建议GC，帮助回收
        
        System.out.println("==================");
    }
    
    /**
     * 在KafkaProducerMonitorTest中调用此方法
     */
    public static void schedulePeriodicMonitoring() {
        Thread monitorThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(30000); // 每30秒打印一次
                    printOptimizationStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.setName("optimization-monitor");
        monitorThread.start();
    }
} 