package org.example.kafkalite.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 智能元数据刷新策略
 * 防止过度频繁的元数据刷新，提高性能
 */
public class SmartMetadataRefreshStrategy {
    
    // 不同场景的刷新间隔
    public static final long ERROR_INTERVAL_MS = 5000;       // 出错后：5秒 (快速恢复)
    public static final long NORMAL_INTERVAL_MS = 60000;     // 正常情况：1分钟 (减少负载)
    public static final long PRODUCER_INTERVAL_MS = 600000;  // 生产者：5分钟 (最保守)
    public static final long MIN_INTERVAL_MS = 3000;         // 绝对最小：3秒 (防止过度频繁)
    
    private final Map<String, Long> lastRefreshTime = new ConcurrentHashMap<>();
    private final Map<String, Integer> consecutiveErrors = new ConcurrentHashMap<>();
    private final Object refreshLock = new Object();
    
    /**
     * 检查是否应该刷新元数据
     * @param topic 主题名称
     * @param isErrorTriggered 是否由错误触发
     * @param isProducerContext 是否在生产者上下文中
     * @return true表示应该刷新，false表示跳过
     */
    public boolean shouldRefresh(String topic, boolean isErrorTriggered, boolean isProducerContext) {
        synchronized (refreshLock) {
            Long lastRefresh = lastRefreshTime.get(topic);
            long currentTime = System.currentTimeMillis();
            
            if (lastRefresh == null) {
                // 首次刷新，直接允许
                lastRefreshTime.put(topic, currentTime);
                return true;
            }
            
            long timeSinceLastRefresh = currentTime - lastRefresh;
            long minInterval;
            
            if (isErrorTriggered) {
                // 错误触发的刷新，使用错误间隔
                minInterval = ERROR_INTERVAL_MS;
            } else if (isProducerContext) {
                // 生产者上下文，使用较长的间隔
                minInterval = PRODUCER_INTERVAL_MS;
            } else {
                // 正常刷新，使用正常间隔
                minInterval = NORMAL_INTERVAL_MS;
            }
            
            if (timeSinceLastRefresh < minInterval) {
                System.out.printf("[SmartRefresh] 跳过刷新: topic=%s, 距离上次=%dms, 最小间隔=%dms, 触发原因=%s\n", 
                    topic, timeSinceLastRefresh, minInterval, 
                    isErrorTriggered ? "错误" : (isProducerContext ? "生产者" : "定期"));
                return false;
            }
            
            // 允许刷新，更新时间戳
            lastRefreshTime.put(topic, currentTime);
            return true;
        }
    }
    
    /**
     * 记录刷新成功
     */
    public void recordSuccess(String topic) {
        consecutiveErrors.remove(topic);
    }
    
    /**
     * 记录刷新失败
     */
    public void recordError(String topic) {
        consecutiveErrors.merge(topic, 1, Integer::sum);
    }
    
    /**
     * 获取连续错误次数
     */
    public int getConsecutiveErrors(String topic) {
        return consecutiveErrors.getOrDefault(topic, 0);
    }
    
    /**
     * 清理过期的记录
     */
    public void cleanup() {
        long currentTime = System.currentTimeMillis();
        long expireTime = NORMAL_INTERVAL_MS * 2; // 2分钟后清理
        
        lastRefreshTime.entrySet().removeIf(entry -> 
            currentTime - entry.getValue() > expireTime);
        
        // 清理错误计数（如果长时间没有刷新）
        consecutiveErrors.entrySet().removeIf(entry -> 
            !lastRefreshTime.containsKey(entry.getKey()));
    }
} 