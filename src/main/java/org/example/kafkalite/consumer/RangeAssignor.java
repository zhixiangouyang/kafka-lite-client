package org.example.kafkalite.consumer;

import java.util.*;

public class RangeAssignor implements PartitionAssignor {
    
    @Override
    public Map<String, List<PartitionAssignment>> assign(
            List<MemberInfo> members, 
            Map<String, List<Integer>> topicPartitions) {
        
        Map<String, List<PartitionAssignment>> assignments = new HashMap<>();
        
        // 初始化每个成员的分配列表
        for (MemberInfo member : members) {
            assignments.put(member.getMemberId(), new ArrayList<>());
        }
        
        // 按topic进行分配
        for (Map.Entry<String, List<Integer>> entry : topicPartitions.entrySet()) {
            String topic = entry.getKey();
            List<Integer> partitions = entry.getValue();
            
            // 过滤出订阅了该topic的成员
            List<MemberInfo> subscribedMembers = new ArrayList<>();
            for (MemberInfo member : members) {
                if (member.getSubscribedTopics().contains(topic)) {
                    subscribedMembers.add(member);
                }
            }
            
            if (subscribedMembers.isEmpty()) {
                continue; // 没有成员订阅该topic
            }
            
            // 按成员ID排序，确保分配的一致性
            subscribedMembers.sort(Comparator.comparing(MemberInfo::getMemberId));
            
            // Range分配：将分区按范围分配给成员
            int partitionsPerMember = partitions.size() / subscribedMembers.size();
            int remainingPartitions = partitions.size() % subscribedMembers.size();
            
            int partitionIndex = 0;
            for (int i = 0; i < subscribedMembers.size(); i++) {
                MemberInfo member = subscribedMembers.get(i);
                int memberPartitionCount = partitionsPerMember + (i < remainingPartitions ? 1 : 0);
                
                for (int j = 0; j < memberPartitionCount; j++) {
                    if (partitionIndex < partitions.size()) {
                        int partition = partitions.get(partitionIndex);
                        PartitionAssignment assignment = new PartitionAssignment(topic, partition);
                        assignments.get(member.getMemberId()).add(assignment);
                        partitionIndex++;
                    }
                }
            }
        }
        
        return assignments;
    }
    
    @Override
    public String name() {
        return "range";
    }
} 