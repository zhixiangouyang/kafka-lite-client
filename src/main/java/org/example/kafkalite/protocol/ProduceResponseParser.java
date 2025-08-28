package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * ProduceResponse解析器 (Version 0)
 * 解析生产者响应，包含错误码、base_offset等信息
 * 注意：Version 0不包含log_append_time_ms和throttle_time_ms字段
 */
public class ProduceResponseParser {
    
    /**
     * 分区响应信息
     */
    public static class PartitionResponse {
        private final int partition;
        private final short errorCode;
        private final long baseOffset;
        private final long logAppendTimeMs;
        
        public PartitionResponse(int partition, short errorCode, long baseOffset, long logAppendTimeMs) {
            this.partition = partition;
            this.errorCode = errorCode;
            this.baseOffset = baseOffset;
            this.logAppendTimeMs = logAppendTimeMs;
        }
        
        public int getPartition() { return partition; }
        public short getErrorCode() { return errorCode; }
        public long getBaseOffset() { return baseOffset; }
        public long getLogAppendTimeMs() { return logAppendTimeMs; }
        
        public boolean isSuccess() { return errorCode == 0; }
        
        /**
         * 获取错误描述
         */
        public String getErrorDescription() {
            switch (errorCode) {
                case 0: return "NO_ERROR";
                case 1: return "OFFSET_OUT_OF_RANGE";
                case 2: return "CORRUPT_MESSAGE";
                case 3: return "UNKNOWN_TOPIC_OR_PARTITION";
                case 4: return "INVALID_FETCH_SIZE";
                case 5: return "LEADER_NOT_AVAILABLE";
                case 6: return "NOT_LEADER_FOR_PARTITION";
                case 7: return "REQUEST_TIMED_OUT";
                case 8: return "BROKER_NOT_AVAILABLE";
                case 9: return "REPLICA_NOT_AVAILABLE";
                case 10: return "MESSAGE_TOO_LARGE";
                case 11: return "STALE_CONTROLLER_EPOCH";
                case 12: return "OFFSET_METADATA_TOO_LARGE";
                case 13: return "NETWORK_EXCEPTION";
                case 14: return "COORDINATOR_LOAD_IN_PROGRESS";
                case 15: return "COORDINATOR_NOT_AVAILABLE";
                case 16: return "NOT_COORDINATOR";
                case 17: return "INVALID_TOPIC_EXCEPTION";
                case 18: return "RECORD_LIST_TOO_LARGE";
                case 19: return "NOT_ENOUGH_REPLICAS";
                case 20: return "NOT_ENOUGH_REPLICAS_AFTER_APPEND";
                case 21: return "INVALID_REQUIRED_ACKS";
                case 22: return "ILLEGAL_GENERATION";
                case 23: return "INCONSISTENT_GROUP_PROTOCOL";
                case 24: return "INVALID_GROUP_ID";
                case 25: return "UNKNOWN_MEMBER_ID";
                case 26: return "INVALID_SESSION_TIMEOUT";
                case 27: return "REBALANCE_IN_PROGRESS";
                case 28: return "INVALID_COMMIT_OFFSET_SIZE";
                case 29: return "TOPIC_AUTHORIZATION_FAILED";
                case 30: return "GROUP_AUTHORIZATION_FAILED";
                case 31: return "CLUSTER_AUTHORIZATION_FAILED";
                case 32: return "INVALID_TIMESTAMP";
                case 33: return "UNSUPPORTED_SASL_MECHANISM";
                case 34: return "ILLEGAL_SASL_STATE";
                case 35: return "UNSUPPORTED_VERSION";
                case 36: return "TOPIC_ALREADY_EXISTS";
                case 37: return "INVALID_PARTITIONS";
                case 38: return "INVALID_REPLICATION_FACTOR";
                case 39: return "INVALID_REPLICA_ASSIGNMENT";
                case 40: return "INVALID_CONFIG";
                case 41: return "NOT_CONTROLLER";
                case 42: return "INVALID_REQUEST";
                case 43: return "UNSUPPORTED_FOR_MESSAGE_FORMAT";
                case 44: return "POLICY_VIOLATION";
                case 45: return "OUT_OF_ORDER_SEQUENCE_NUMBER";
                case 46: return "DUPLICATE_SEQUENCE_NUMBER";
                case 47: return "INVALID_PRODUCER_EPOCH";
                case 48: return "INVALID_TXN_STATE";
                case 49: return "INVALID_PRODUCER_ID_MAPPING";
                case 50: return "INVALID_TRANSACTION_TIMEOUT";
                case 51: return "CONCURRENT_TRANSACTIONS";
                default: return "UNKNOWN_ERROR_" + errorCode;
            }
        }
        
        @Override
        public String toString() {
            if (logAppendTimeMs == -1L) {
                // Version 0 没有logAppendTimeMs
                return String.format("PartitionResponse(partition=%d, errorCode=%d[%s], baseOffset=%d)", 
                    partition, errorCode, getErrorDescription(), baseOffset);
            } else {
                return String.format("PartitionResponse(partition=%d, errorCode=%d[%s], baseOffset=%d, logAppendTimeMs=%d)", 
                    partition, errorCode, getErrorDescription(), baseOffset, logAppendTimeMs);
            }
        }
    }
    
    /**
     * Topic响应信息
     */
    public static class TopicResponse {
        private final String topic;
        private final Map<Integer, PartitionResponse> partitionResponses;
        
        public TopicResponse(String topic) {
            this.topic = topic;
            this.partitionResponses = new HashMap<>();
        }
        
        public String getTopic() { return topic; }
        public Map<Integer, PartitionResponse> getPartitionResponses() { return partitionResponses; }
        
        public void addPartitionResponse(PartitionResponse response) {
            partitionResponses.put(response.getPartition(), response);
        }
        
        public PartitionResponse getPartitionResponse(int partition) {
            return partitionResponses.get(partition);
        }
        
        /**
         * 检查是否所有分区都成功
         */
        public boolean isAllSuccess() {
            return partitionResponses.values().stream().allMatch(PartitionResponse::isSuccess);
        }
        
        /**
         * 获取失败的分区
         */
        public Map<Integer, PartitionResponse> getFailedPartitions() {
            Map<Integer, PartitionResponse> failed = new HashMap<>();
            partitionResponses.forEach((partition, response) -> {
                if (!response.isSuccess()) {
                    failed.put(partition, response);
                }
            });
            return failed;
        }
    }
    
    /**
     * 完整的Produce响应
     */
    public static class ProduceResponse {
        private final int throttleTimeMs;
        private final Map<String, TopicResponse> topicResponses;
        
        public ProduceResponse(int throttleTimeMs) {
            this.throttleTimeMs = throttleTimeMs;
            this.topicResponses = new HashMap<>();
        }
        
        public int getThrottleTimeMs() { return throttleTimeMs; }
        public Map<String, TopicResponse> getTopicResponses() { return topicResponses; }
        
        public void addTopicResponse(TopicResponse response) {
            topicResponses.put(response.getTopic(), response);
        }
        
        public TopicResponse getTopicResponse(String topic) {
            return topicResponses.get(topic);
        }
        
        /**
         * 检查是否所有topic的所有分区都成功
         */
        public boolean isAllSuccess() {
            return topicResponses.values().stream().allMatch(TopicResponse::isAllSuccess);
        }
        
        /**
         * 获取所有失败信息
         */
        public Map<String, Map<Integer, PartitionResponse>> getAllFailures() {
            Map<String, Map<Integer, PartitionResponse>> allFailures = new HashMap<>();
            topicResponses.forEach((topic, topicResponse) -> {
                Map<Integer, PartitionResponse> failed = topicResponse.getFailedPartitions();
                if (!failed.isEmpty()) {
                    allFailures.put(topic, failed);
                }
            });
            return allFailures;
        }
        
        @Override
        public String toString() {
            return String.format("ProduceResponse(throttleTimeMs=%d, topics=%s)", 
                throttleTimeMs, topicResponses.keySet());
        }
    }
    
    /**
     * 解析ProduceResponse (Version 0)
     * @param buffer 响应数据
     * @return ProduceResponse对象
     */
    public static ProduceResponse parse(ByteBuffer buffer) {
        try {
            // 跳过总长度字段
            buffer.getInt();
            
            // 读取correlationId
            int correlationId = buffer.getInt();
            
            // 读取responses数组
            int topicCount = buffer.getInt();
            
            if (topicCount < 0 || topicCount > 100) {
                throw new IllegalArgumentException("Invalid topic count: " + topicCount);
            }
            
            ProduceResponse response = new ProduceResponse(0); // Version 0没有throttle_time_ms
            
            for (int i = 0; i < topicCount; i++) {
                // 读取topic名称
                String topic = readString(buffer);
                TopicResponse topicResponse = new TopicResponse(topic);
                
                // 读取partition_responses数组
                int partitionCount = buffer.getInt();
                
                if (partitionCount < 0 || partitionCount > 100) {
                    throw new IllegalArgumentException("Invalid partition count: " + partitionCount);
                }
                
                for (int j = 0; j < partitionCount; j++) {
                    int partition = buffer.getInt();          // index (partition)
                    short errorCode = buffer.getShort();      // error_code
                    long baseOffset = buffer.getLong();       // base_offset
                    
                    // Version 0 没有 log_append_time_ms 字段！
                    long logAppendTimeMs = -1L; // 默认值
                    
                    PartitionResponse partitionResponse = new PartitionResponse(
                        partition, errorCode, baseOffset, logAppendTimeMs);
                    topicResponse.addPartitionResponse(partitionResponse);
                }
                
                response.addTopicResponse(topicResponse);
            }
            
            // Version 0 没有throttle_time_ms字段，所以不需要读取
            
            return response;
            
        } catch (Exception e) {
            System.err.printf("[ProduceResponseParser] 解析异常: %s\n", e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    
    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
} 