package org.example.kafkalite.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class KafkaSocketClient {
    public static ByteBuffer sendAndReceive(String host, int port, ByteBuffer request) {
        // System.out.println("[KafkaSocketClient] Connecting to " + host + ":" + port);
        
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            socket.setSoTimeout(30000); // 30秒超时
            
            // 发送请求
            OutputStream out = socket.getOutputStream();
            byte[] requestBytes = new byte[request.remaining()];
            request.get(requestBytes);
            out.write(requestBytes);
            out.flush();
            // System.out.println("[KafkaSocketClient] Sent " + requestBytes.length + " bytes");
            
            // 读取响应大小
            InputStream in = socket.getInputStream();
            byte[] sizeBuf = new byte[4];
            readFully(in, sizeBuf);
            int size = ByteBuffer.wrap(sizeBuf).getInt();
            // System.out.println("[KafkaSocketClient] Response size: " + size + " bytes");
            
            if (size < 0 || size > 10 * 1024 * 1024) { // 最大10MB
                throw new IllegalArgumentException("Invalid response size: " + size);
            }

            // 读取响应内容
            byte[] responseBuf = new byte[size + 4]; // 包含size字段
            System.arraycopy(sizeBuf, 0, responseBuf, 0, 4);
            readFully(in, responseBuf, 4, size);
            
            ByteBuffer response = ByteBuffer.wrap(responseBuf);
            // System.out.println("[KafkaSocketClient] Received response successfully");
            return response;
            
        } catch (Exception e) {
            System.err.println("[KafkaSocketClient] Error: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.err.println("[KafkaSocketClient] Error closing socket: " + e.getMessage());
        }
    }
        }
    }
    
    private static void readFully(InputStream in, byte[] buf) throws IOException {
        readFully(in, buf, 0, buf.length);
    }
    
    private static void readFully(InputStream in, byte[] buf, int offset, int len) throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf, offset + totalRead, len - totalRead);
            if (read == -1) {
                throw new IOException("End of stream reached after reading " + totalRead + " bytes, expected " + len + " bytes");
        }
            totalRead += read;
    }
    }
}
