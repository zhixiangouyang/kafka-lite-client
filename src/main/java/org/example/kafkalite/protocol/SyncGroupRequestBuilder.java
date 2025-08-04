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
        
        return buildWithAssignments(clientId, groupId, generationId, assignments);
    }
    
    public static ByteBuffer buildWithAssignments(String clientId, String groupId, int generationId, 
                                                 Map<String, List<PartitionAssignment>> assignments) {
        try {
            // 计算总大小
            int totalSize = 0;
            byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
            byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);

            // 基础字段大小
            totalSize += 4; // size
            totalSize += 2; // apiKey
            totalSize += 2; // apiVersion
            totalSize += 4; // correlationId
            totalSize += 2 + clientIdBytes.length; // clientId
            totalSize += 2 + groupIdBytes.length; // groupId
            totalSize += 4; // generationId

            // 写入memberId字段（主结构体）
            String mainMemberIdStr = clientId;
            if (!assignments.isEmpty()) {
                mainMemberIdStr = assignments.keySet().iterator().next();
            }
            byte[] mainMemberIdBytes = mainMemberIdStr.getBytes(StandardCharsets.UTF_8);
            totalSize += 2 + mainMemberIdBytes.length; // 这里是遗漏的关键！

            // 计算groupAssignment数组大小
            totalSize += 4; // groupAssignment array length
            Map<String, byte[]> assignmentBytesMap = new HashMap<>();
            for (Map.Entry<String, List<PartitionAssignment>> entry : assignments.entrySet()) {
                String assignMemberId = entry.getKey();
                List<PartitionAssignment> memberAssignments = entry.getValue();
                byte[] assignMemberIdBytes = assignMemberId.getBytes(StandardCharsets.UTF_8);
                totalSize += 2 + assignMemberIdBytes.length; // member id
                // 严格序列化assignment内容
                byte[] assignmentBytes = encodeMemberAssignment(memberAssignments);
                assignmentBytesMap.put(assignMemberId, assignmentBytes);
                totalSize += 4; // assignment size
                totalSize += assignmentBytes.length; // assignment bytes
                System.out.printf("[SyncGroupRequestBuilder] assignMemberId=%s, assignmentBytes.length=%d\n", assignMemberId, assignmentBytes.length);
            }
            System.out.printf("[SyncGroupRequestBuilder] totalSize=%d, assignments.size=%d\n", totalSize, assignments.size());

            // 分配buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            System.out.printf("[SyncGroupRequestBuilder] buffer.capacity=%d\n", buffer.capacity());

            // 写入请求头
            buffer.putInt(totalSize - 4); // size
            buffer.putShort((short) 14); // apiKey = 14 (SyncGroup)
            buffer.putShort((short) 0); // apiVersion = 0
            buffer.putInt(2); // correlationId

            // 写入基础字段
            buffer.putShort((short) clientIdBytes.length);
            buffer.put(clientIdBytes);
            buffer.putShort((short) groupIdBytes.length);
            buffer.put(groupIdBytes);
            buffer.putInt(generationId);
            // 写入memberId字段（必须！）
            buffer.putShort((short) mainMemberIdBytes.length);
            buffer.put(mainMemberIdBytes);
            System.out.printf("[SyncGroupRequestBuilder] after main memberId, position=%d, remaining=%d\n", buffer.position(), buffer.remaining());
            // 写入groupAssignment数组
            buffer.putInt(assignments.size()); // groupAssignment array length
            for (Map.Entry<String, List<PartitionAssignment>> entry : assignments.entrySet()) {
                String assignMemberId = entry.getKey();
                byte[] assignMemberIdBytes = assignMemberId.getBytes(StandardCharsets.UTF_8);
                buffer.putShort((short) assignMemberIdBytes.length);
                buffer.put(assignMemberIdBytes);
                byte[] assignmentBytes = assignmentBytesMap.get(assignMemberId);
                buffer.putInt(assignmentBytes.length);
                System.out.printf("[SyncGroupRequestBuilder] before assignmentBytes, position=%d, remaining=%d, assignmentBytes.length=%d\n", buffer.position(), buffer.remaining(), assignmentBytes.length);
                buffer.put(assignmentBytes);
                System.out.printf("[SyncGroupRequestBuilder] after assignmentBytes, position=%d, remaining=%d\n", buffer.position(), buffer.remaining());
            }

            buffer.flip();
            // 打印请求字节流
            byte[] bytes = new byte[buffer.remaining()];
            buffer.mark();
            buffer.get(bytes);
            buffer.reset();
            System.out.print("[SyncGroupRequestBuilder] 请求字节流: ");
            for (byte b : bytes) System.out.printf("%02x ", b);
            System.out.println();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
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