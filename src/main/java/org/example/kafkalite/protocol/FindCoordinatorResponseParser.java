package org.example.kafkalite.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FindCoordinatorResponseParser {
    public static class CoordinatorInfo {
        private final short errorCode;
        private final int nodeId;
        private final String host;
        private final int port;
        
        public CoordinatorInfo(short errorCode, int nodeId, String host, int port) {
            this.errorCode = errorCode;
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }
        
        public short getErrorCode() {
            return errorCode;
        }
        
        public int getNodeId() {
            return nodeId;
        }
        
        public String getHost() {
            return host;
        }
        
        public int getPort() {
            return port;
        }
        
        @Override
        public String toString() {
            return String.format("CoordinatorInfo(errorCode=%d, nodeId=%d, host=%s, port=%d)",
                    errorCode, nodeId, host, port);
        }
    }
    
    public static CoordinatorInfo parse(ByteBuffer buffer) {
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
            
            // 读取nodeId
            int nodeId = buffer.getInt();
            
            // 读取host
            short hostLen = buffer.getShort();
            String host;
            if (hostLen <= 0) {
                host = "10.53.29.81"; // 使用原始IP
            } else {
                byte[] hostBytes = new byte[hostLen];
                buffer.get(hostBytes);
                host = new String(hostBytes, StandardCharsets.UTF_8);
            }
            
            // 读取port
            int port = buffer.getInt();
            
            CoordinatorInfo info = new CoordinatorInfo(errorCode, nodeId, host, port);
            return info;
            
        } catch (Exception e) {
            System.err.println("[FindCoordinatorResponseParser] Error parsing response: " + e.getMessage());
            System.err.println("[FindCoordinatorResponseParser] Buffer position at error: " + buffer.position());
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