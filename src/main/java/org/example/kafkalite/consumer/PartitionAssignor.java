package org.example.kafkalite.consumer;

import java.util.List;
import java.util.Map;

public interface PartitionAssignor {
    /**
     * 分配分区给消费者成员
     * @param members 消费者成员列表
     * @param topicPartitions topic到分区列表的映射
     * @return 成员ID到分区分配列表的映射
     */
    Map<String, List<PartitionAssignment>> assign(
        List<MemberInfo> members, 
        Map<String, List<Integer>> topicPartitions
    );
    
    /**
     * 获取分配策略名称
     * @return 策略名称
     */
    String name();
} 