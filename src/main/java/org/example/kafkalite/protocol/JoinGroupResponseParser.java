package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JoinGroupResponseParser {
    public static class JoinGroupResult {
        private final short errorCode;
        private final int generationId;
        private final String protocolName;
        private final String leaderId;
        private final String memberId;
        private final List<String> members;
        
        public JoinGroupResult(short errorCode, int generationId, String protocolName, 
                             String leaderId, String memberId, List<String> members) {
            this.errorCode = errorCode;
            this.generationId = generationId;
            this.protocolName = protocolName;
            this.leaderId = leaderId;
            this.memberId = memberId;
            this.members = members;
        }
        
        public short getErrorCode() {
            return errorCode;
        }
        
        public int getGenerationId() {
            return generationId;
        }
        
        public String getProtocolName() {
            return protocolName;
        }
        
        public String getLeaderId() {
            return leaderId;
        }
        
        public String getMemberId() {
            return memberId;
        }
        
        public List<String> getMembers() {
            return members;
        }
        
        @Override
        public String toString() {
            return String.format("JoinGroupResult(errorCode=%d, generationId=%d, protocol=%s, leaderId=%s, memberId=%s, members=%s)",
                    errorCode, generationId, protocolName, leaderId, memberId, members);
        }
    }
    
    public static JoinGroupResult parse(ByteBuffer buffer) {
        try {
            // 打印响应字节但不影响buffer的position
            byte[] allBytes = new byte[buffer.remaining()];
            int originalPosition = buffer.position();
            buffer.get(allBytes);
            buffer.position(originalPosition);
            
            // 跳过总长度
            int totalSize = buffer.getInt();
            
            // 读取correlationId
            int correlationId = buffer.getInt();
            
            // 读取errorCode
            short errorCode = buffer.getShort();
            
            // 读取generationId
            int generationId = buffer.getInt();
            
            // 读取protocol name
            String protocolName = readString(buffer);
            
            // 读取leader
            String leaderId = readString(buffer);
            
            // 读取member id
            String memberId = readString(buffer);
            
            // 读取members数组
            int memberCount = buffer.getInt();
            if (memberCount < 0 || memberCount > 1000) {
                throw new IllegalArgumentException("Invalid member count: " + memberCount);
            }
            
            List<String> members = new ArrayList<>();
            for (int i = 0; i < memberCount; i++) {
                String member = readString(buffer);
                members.add(member);
            }
            
            JoinGroupResult result = new JoinGroupResult(errorCode, generationId, protocolName, leaderId, memberId, members);
            return result;
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) {
            return null;
        }
        if (length > buffer.remaining()) {
            throw new IllegalArgumentException("String length " + length + " exceeds remaining buffer size " + buffer.remaining());
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String str = new String(bytes, StandardCharsets.UTF_8);
        return str;
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xff));
        }
        return sb.toString();
    }
} 