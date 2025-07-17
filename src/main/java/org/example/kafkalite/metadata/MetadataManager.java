package org.example.kafkalite.metadata;

import java.util.Map;

public interface MetadataManager {
    /**
     * 拉取元数据并缓存
     */
    void refreshMetadata(String topic);

    /**
     * 获取 topic 的分区 leader映射（partitionId -> leader broker address）
     */
    Map<Integer, String> getPartitionLeaders(String topic);

    /**
     * 获取 partition数量
     */
    int getPartitionCount(String topic);
}
