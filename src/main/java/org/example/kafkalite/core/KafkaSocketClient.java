package org.example.kafkalite.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    
    /**
     * Kafka连接池实现，提高网络连接效率
     */
    public static class ConnectionPool {
        private final String host;
        private final int port;
        private final BlockingQueue<Socket> socketPool;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        
        public ConnectionPool(String host, int port, int poolSize) {
            this.host = host;
            this.port = port;
            this.socketPool = new ArrayBlockingQueue<>(poolSize);
            
            // 预创建连接
            for (int i = 0; i < poolSize; i++) {
                try {
                    Socket socket = createSocket();
                    socketPool.offer(socket);
                } catch (Exception e) {
                    System.err.println("[ConnectionPool] Failed to create initial connection: " + e.getMessage());
                    // 继续创建其他连接
                }
            }
        }
        
        private Socket createSocket() throws IOException {
            Socket socket = new Socket(host, port);
            socket.setSoTimeout(30000); // 30秒超时
            return socket;
        }
        
        public ByteBuffer sendAndReceive(ByteBuffer request) {
            if (closed.get()) {
                throw new IllegalStateException("Connection pool is closed");
            }
            
            Socket socket = null;
            try {
                // 从池中获取连接，如果没有可用连接则创建新的
                socket = socketPool.poll(100, TimeUnit.MILLISECONDS);
                if (socket == null || socket.isClosed()) {
                    socket = createSocket();
                }
                
                // 创建请求数据的副本，避免修改原始ByteBuffer
                ByteBuffer requestCopy = ByteBuffer.allocate(request.remaining());
                // 确保原始buffer的position正确
                ByteBuffer duplicated = request.duplicate();
                duplicated.rewind(); // 重置position到开始位置
                requestCopy.put(duplicated);
                requestCopy.flip();
                
                // 发送请求
                OutputStream out = socket.getOutputStream();
                byte[] requestBytes = new byte[requestCopy.remaining()];
                requestCopy.get(requestBytes);
                out.write(requestBytes);
                out.flush();
                
                // 读取响应大小
                InputStream in = socket.getInputStream();
                byte[] sizeBuf = new byte[4];
                readFully(in, sizeBuf);
                int size = ByteBuffer.wrap(sizeBuf).getInt();
                
                if (size < 0 || size > 10 * 1024 * 1024) { // 最大10MB
                    throw new IllegalArgumentException("Invalid response size: " + size);
                }

                // 读取响应内容
                byte[] responseBuf = new byte[size + 4]; // 包含size字段
                System.arraycopy(sizeBuf, 0, responseBuf, 0, 4);
                readFully(in, responseBuf, 4, size);
                
                ByteBuffer response = ByteBuffer.wrap(responseBuf);
                
                // 将连接放回池中
                if (!closed.get() && !socket.isClosed()) {
                    socketPool.offer(socket);
                    socket = null; // 防止finally中关闭
                }
                
                return response;
                
            } catch (Exception e) {
                System.err.println("[ConnectionPool] Error: " + e.getMessage());
                // 连接出错，关闭并不放回池中
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException ce) {
                        // 忽略关闭错误
                    }
                }
                throw new RuntimeException(e);
            } finally {
                // 如果连接没有放回池中，关闭它
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        System.err.println("[ConnectionPool] Error closing socket: " + e.getMessage());
                    }
                }
            }
        }
        
        public void close() {
            if (closed.compareAndSet(false, true)) {
                // 关闭所有连接
                Socket socket;
                while ((socket = socketPool.poll()) != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        System.err.println("[ConnectionPool] Error closing socket during pool shutdown: " + e.getMessage());
                    }
                }
            }
        }
    }
}
