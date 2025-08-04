package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.example.kafkalite.consumer.PartitionAssignment;

public class SyncGroupRequestBuilder {
    
    public static ByteBuffer build(String clientId, String groupId, int generationId, String memberId, List<String> topics) {
        // 为单个消费者构建简单的分配
        Map<String, List<PartitionAssignment>> assignments = new HashMap<>();
        List<PartitionAssignment> memberAssignments = new ArrayList<>();
        
        // 为每个topic分配分区0（简化处理）
        for (String topic : topics) {
            memberAssignments.add(new PartitionAssignment(topic, 0));
        }
        assignments.put(memberId, memberAssignments);
        
        return buildWithAssignments(clientId, groupId, generationId, memberId, assignments);
    }
    
    public static ByteBuffer buildEmptyAssignment(String clientId, String groupId, int generationId, String memberId) {
        // 非Leader消费者发送空的分配信息
        Map<String, List<PartitionAssignment>> assignments = new HashMap<>();
        // 创建一个新的方法调用来正确处理空的assignments
        return buildWithAssignments(clientId, groupId, generationId, memberId, assignments);
    }
    
    // 新的buildWithAssignments，强制要求传入memberId
    public static ByteBuffer buildWithAssignments(String clientId, String groupId, int generationId, String memberId, Map<String, List<PartitionAssignment>> assignments) {
        try {
            int totalSize = 0;
            byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
            byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
            // 基础字段
            totalSize += 4 + 2 + 2 + 4;
            totalSize += 2 + clientIdBytes.length;
            totalSize += 2 + groupIdBytes.length;
            totalSize += 4;
            // 主memberId字段
            byte[] mainMemberIdBytes = memberId.getBytes(StandardCharsets.UTF_8);
            totalSize += 2 + mainMemberIdBytes.length;
            // groupAssignment数组
            totalSize += 4;
            Map<String, byte[]> assignmentBytesMap = new HashMap<>();
            for (Map.Entry<String, List<PartitionAssignment>> entry : assignments.entrySet()) {
                String assignMemberId = entry.getKey();
                List<PartitionAssignment> memberAssignments = entry.getValue();
                byte[] assignMemberIdBytes = assignMemberId.getBytes(StandardCharsets.UTF_8);
                totalSize += 2 + assignMemberIdBytes.length;
                byte[] assignmentBytes = encodeMemberAssignment(memberAssignments);
                assignmentBytesMap.put(assignMemberId, assignmentBytes);
                totalSize += 4;
                totalSize += assignmentBytes.length;
            }
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            buffer.putInt(totalSize - 4);
            buffer.putShort((short) 14);
            buffer.putShort((short) 0);
            buffer.putInt(2);
            buffer.putShort((short) clientIdBytes.length);
            buffer.put(clientIdBytes);
            buffer.putShort((short) groupIdBytes.length);
            buffer.put(groupIdBytes);
            buffer.putInt(generationId);
            buffer.putShort((short) mainMemberIdBytes.length);
            buffer.put(mainMemberIdBytes);
            buffer.putInt(assignments.size());
            for (Map.Entry<String, List<PartitionAssignment>> entry : assignments.entrySet()) {
                String assignMemberId = entry.getKey();
                byte[] assignMemberIdBytes = assignMemberId.getBytes(StandardCharsets.UTF_8);
                buffer.putShort((short) assignMemberIdBytes.length);
                buffer.put(assignMemberIdBytes);
                byte[] assignmentBytes = assignmentBytesMap.get(assignMemberId);
                buffer.putInt(assignmentBytes.length);
                buffer.put(assignmentBytes);
            }
            buffer.flip();
            return buffer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build SyncGroup request", e);
        }
    }

    // 严格序列化Kafka MemberAssignment结构
    private static byte[] encodeMemberAssignment(List<PartitionAssignment> assignments) {
        try {
            // 按topic分组
            Map<String, List<Integer>> topicPartitions = new HashMap<>();
            for (PartitionAssignment assignment : assignments) {
                topicPartitions.computeIfAbsent(assignment.getTopic(), k -> new ArrayList<>())
                              .add(assignment.getPartition());
            }
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
            dos.writeShort(0); // version
            dos.writeInt(topicPartitions.size()); // topic count
            System.out.printf("[encodeMemberAssignment] version=0, topicCount=%d\n", topicPartitions.size());
            for (Map.Entry<String, List<Integer>> entry : topicPartitions.entrySet()) {
                byte[] topicBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeShort(topicBytes.length);
                dos.write(topicBytes);
                dos.writeInt(entry.getValue().size());
                System.out.printf("[encodeMemberAssignment] topic=%s, topicBytes.length=%d, partitionCount=%d\n", entry.getKey(), topicBytes.length, entry.getValue().size());
                for (Integer partition : entry.getValue()) {
                    dos.writeInt(partition);
                    System.out.printf("[encodeMemberAssignment]   partition=%d\n", partition);
                }
            }
            dos.writeInt(0); // user_data length = 0
            dos.flush();
            byte[] result = baos.toByteArray();
            System.out.printf("[encodeMemberAssignment] total assignmentBytes.length=%d\n", result.length);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode member assignment", e);
        }
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xff));
        }
        return sb.toString();
    }
} 