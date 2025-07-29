package org.example.kafkalite.protocol;

import org.example.kafkalite.consumer.PartitionAssignment;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SyncGroupResponseParser {
    public static class SyncGroupResult {
        private final short errorCode;
        private final List<PartitionAssignment> assignments;
        
        public SyncGroupResult(short errorCode, List<PartitionAssignment> assignments) {
            this.errorCode = errorCode;
            this.assignments = assignments;
        }
        
        public short getErrorCode() {
            return errorCode;
        }
        
        public List<PartitionAssignment> getAssignments() {
            return assignments;
        }
        
        @Override
        public String toString() {
            return String.format("SyncGroupResult(errorCode=%d, assignments=%s)", errorCode, assignments);
        }
    }
    
    public static SyncGroupResult parse(ByteBuffer buffer) {
        try {
            // 注释掉所有System.out.println和System.err.println日志，只保留异常抛出。
            
            // 跳过总长度
            int totalSize = buffer.getInt();
            
            // 读取correlationId
            int correlationId = buffer.getInt();
            
            // 读取errorCode
            short errorCode = buffer.getShort();
            
            // 读取member assignment
            List<PartitionAssignment> assignments = new ArrayList<>();
            if (buffer.hasRemaining()) {
                // 读取assignment bytes
                int assignmentSize = buffer.getInt();
                if (assignmentSize > 0) {
                    byte[] assignmentBytes = new byte[assignmentSize];
                    buffer.get(assignmentBytes);
                    ByteBuffer assignmentBuffer = ByteBuffer.wrap(assignmentBytes);

                    // 读取version
                    if (assignmentBuffer.remaining() < 2) {
                        System.err.println("[SyncGroupResponseParser] Not enough bytes for version");
                        return new SyncGroupResult(errorCode, assignments);
                    }
                    short version = assignmentBuffer.getShort();

                    // 读取topic数量
                    if (assignmentBuffer.remaining() < 4) {
                        System.err.println("[SyncGroupResponseParser] Not enough bytes for topic count");
                        return new SyncGroupResult(errorCode, assignments);
                    }
                    int topicCount = assignmentBuffer.getInt();

                    // 读取每个topic的分区
                    for (int i = 0; i < topicCount; i++) {
                        if (assignmentBuffer.remaining() < 2) {
                            System.err.println("[SyncGroupResponseParser] Not enough bytes for topic length");
                            break;
                        }
                        short topicLen = assignmentBuffer.getShort();
                        if (assignmentBuffer.remaining() < topicLen) {
                            System.err.println("[SyncGroupResponseParser] Not enough bytes for topic name");
                            break;
                        }
                        byte[] topicBytes = new byte[topicLen];
                        assignmentBuffer.get(topicBytes);
                        String topic = new String(topicBytes, StandardCharsets.UTF_8);

                        if (assignmentBuffer.remaining() < 4) {
                            System.err.println("[SyncGroupResponseParser] Not enough bytes for partition count");
                            break;
                        }
                        int partitionCount = assignmentBuffer.getInt();

                        for (int j = 0; j < partitionCount; j++) {
                            if (assignmentBuffer.remaining() < 4) {
                                System.err.println("[SyncGroupResponseParser] Not enough bytes for partition id");
                                break;
                            }
                            int partition = assignmentBuffer.getInt();
                            assignments.add(new PartitionAssignment(topic, partition));
                        }
                    }

                    // 读取user_data
                    if (assignmentBuffer.remaining() >= 4) {
                        int userDataLen = assignmentBuffer.getInt();
                        if (userDataLen > 0 && assignmentBuffer.remaining() >= userDataLen) {
                            byte[] userData = new byte[userDataLen];
                            assignmentBuffer.get(userData);
                            // 可选：打印user_data内容
                        }
                    } else {
                        System.out.println("[SyncGroupResponseParser] No user_data field present");
                    }
                } else {
                    System.out.println("[SyncGroupResponseParser] assignmentSize is 0, no assignments");
                }
            }
            
            SyncGroupResult result = new SyncGroupResult(errorCode, assignments);
            return result;
            
        } catch (Exception e) {
            System.err.println("[SyncGroupResponseParser] Error parsing response: " + e.getMessage());
            System.err.println("[SyncGroupResponseParser] Buffer position at error: " + buffer.position());
            e.printStackTrace();
            throw e;
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