package org.example.kafkalite.metadata;

import java.util.Map;

public interface MetadataManager {
    void refreshMetadata(String topic);
    
    /**
     * 刷新元数据（增强版本）
     * @param topic 主题名称
     * @param isErrorTriggered 是否由错误触发
     * @param isProducerContext 是否在生产者上下文中
     */
    default void refreshMetadata(String topic, boolean isErrorTriggered, boolean isProducerContext) {
        // 默认实现：调用单参数版本（向后兼容）
        refreshMetadata(topic);
    }
    
    Map<Integer, String> getPartitionLeaders(String topic);
}
