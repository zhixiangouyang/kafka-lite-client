package org.example.kafkalite.consumer;

/**
 * auto.offset.reset策略枚举
 * 提供类型安全的offset重置策略选项
 */
public enum OffsetResetStrategy {
    /**
     * 从分区最早可用的offset开始消费
     * 适用场景：确保不丢失任何历史消息
     */
    EARLIEST("earliest"),
    
    /**
     * 从分区最新的offset开始消费（跳过现有消息）
     * 适用场景：只关心新产生的消息，忽略历史消息
     */
    LATEST("latest"),
    
    /**
     * 没有已提交offset时抛出异常，不自动选择起始位置
     * 适用场景：严格的offset管理，必须明确指定消费起始点
     */
    NONE("none");
    
    private final String value;
    
    OffsetResetStrategy(String value) {
        this.value = value;
    }
    
    /**
     * 获取策略的字符串值
     * @return 策略字符串值
     */
    public String getValue() {
        return value;
    }
    
    /**
     * 从字符串值解析枚举
     * @param value 策略字符串值
     * @return 对应的枚举值
     * @throws IllegalArgumentException 如果值不支持
     */
    public static OffsetResetStrategy fromValue(String value) {
        if (value == null) {
            return EARLIEST; // 默认值
        }
        
        for (OffsetResetStrategy strategy : values()) {
            if (strategy.value.equalsIgnoreCase(value)) {
                return strategy;
            }
        }
        
        throw new IllegalArgumentException("Unknown auto.offset.reset strategy: " + value + 
            ". Supported values: earliest, latest, none");
    }
    
    @Override
    public String toString() {
        return value;
    }
} 