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
import java.util.concurrent.atomic.AtomicLong;

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
        private final int maxPoolSize;
        private final int connectionTimeout = 5000; // 连接超时时间5秒
        private final int socketTimeout = 30000;    // 套接字超时时间30秒
        private final AtomicLong totalConnections = new AtomicLong(0);
        private final AtomicLong successfulRequests = new AtomicLong(0);
        private final AtomicLong failedRequests = new AtomicLong(0);
        
        public ConnectionPool(String host, int port, int poolSize) {
            this.host = host;
            this.port = port;
            this.maxPoolSize = poolSize;
            this.socketPool = new ArrayBlockingQueue<>(poolSize);
            
            // 预创建连接
            int initialConnections = Math.min(5, poolSize); // 初始创建更多连接
            for (int i = 0; i < initialConnections; i++) {
                try {
                    Socket socket = createSocket();
                    socketPool.offer(socket);
                    totalConnections.incrementAndGet();
                    System.out.printf("已创建到 %s:%d 的初始连接 %d/%d%n", host, port, i+1, initialConnections);
                } catch (Exception e) {
                    System.err.printf("创建初始连接失败: %s:%d, 错误: %s%n", host, port, e.getMessage());
                    // 继续创建其他连接
                }
            }
            
            if (socketPool.isEmpty()) {
                System.err.printf("警告: 无法创建任何到 %s:%d 的初始连接%n", host, port);
            }
        }
        
        private Socket createSocket() throws IOException {
            Socket socket = new Socket();
            // 设置TCP_NODELAY禁用Nagle算法，减少延迟
            socket.setTcpNoDelay(true);
            // 设置发送和接收缓冲区大小
            socket.setSendBufferSize(128 * 1024); // 128KB
            socket.setReceiveBufferSize(128 * 1024); // 128KB
            socket.connect(new java.net.InetSocketAddress(host, port), connectionTimeout);
            socket.setSoTimeout(socketTimeout);
            return socket;
        }
        
        public ByteBuffer sendAndReceive(ByteBuffer request) {
            if (closed.get()) {
                throw new IllegalStateException("Connection pool is closed");
            }
            
            Socket socket = null;
            boolean returnToPool = true;
            
            try {
                // 从池中获取连接，如果没有可用连接则创建新的
                socket = socketPool.poll(100, TimeUnit.MILLISECONDS);
                
                if (socket == null) {
                    // 🔧 修复竞态条件：原子性地检查和递增连接数
                    long currentConnections = totalConnections.get();
                    if (currentConnections < maxPoolSize && totalConnections.compareAndSet(currentConnections, currentConnections + 1)) {
                        // 成功预留了一个连接槽位
                        try {
                            System.out.printf("池中无可用连接，创建新连接到 %s:%d (当前连接数: %d/%d)%n", host, port, currentConnections + 1, maxPoolSize);
                            socket = createSocket();
                        } catch (Exception e) {
                            // 创建失败，回滚连接数
                            totalConnections.decrementAndGet();
                            throw e;
                        }
                    } else {
                        // 已达到最大连接数，等待连接可用
                        System.out.printf("已达到最大连接数 %d，等待连接可用: %s:%d (当前连接数: %d)%n", maxPoolSize, host, port, currentConnections);
                        socket = socketPool.take(); // 阻塞等待
                    }
                } else if (socket.isClosed() || !socket.isConnected()) {
                    // 连接已关闭或断开，创建新的
                    System.out.printf("检测到已关闭连接，创建新连接到 %s:%d%n", host, port);
                    try {
                        socket.close();
                        // 注意：不减少totalConnections，因为我们马上要创建一个新的来替换它
                    } catch (IOException ignored) {}
                    socket = createSocket();
                    // 不需要incrementAndGet，因为我们只是替换了一个连接
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
                    System.err.printf("无效的响应大小: %d bytes%n", size);
                    returnToPool = false; // 不要将这个连接返回池中
                    throw new IllegalArgumentException("Invalid response size: " + size);
                }

                // 读取响应内容
                byte[] responseBuf = new byte[size + 4]; // 包含size字段
                System.arraycopy(sizeBuf, 0, responseBuf, 0, 4);
                readFully(in, responseBuf, 4, size);
                
                ByteBuffer response = ByteBuffer.wrap(responseBuf);
                
                // 🔧 修复：总是尝试将连接放回池中（如果连接有效）
                if (returnToPool && !closed.get() && socket != null && !socket.isClosed() && socket.isConnected()) {
                    boolean offered = socketPool.offer(socket);
                    if (offered) {
                        socket = null; // 防止finally中关闭
                        // System.out.printf("[DEBUG] 连接已归还到池: %s:%d, 池大小: %d\n", host, port, socketPool.size());
                    } else {
                        System.err.printf("连接池已满，无法归还连接: %s:%d\n", host, port);
                        // 连接池满了，说明有问题，强制关闭这个连接
                        returnToPool = false;
                    }
                }
                
                successfulRequests.incrementAndGet();
                return response;
                
            } catch (InterruptedException e) {
                System.err.printf("获取连接被中断: %s:%d%n", host, port);
                Thread.currentThread().interrupt();
                returnToPool = false; // 不要将这个连接返回池中
                failedRequests.incrementAndGet();
                throw new RuntimeException("Interrupted while getting connection", e);
            } catch (IOException e) {
                System.err.printf("连接IO错误: %s:%d, 错误: %s%n", host, port, e.getMessage());
                returnToPool = false; // 网络错误，不要将这个连接返回池中
                failedRequests.incrementAndGet();
                throw new RuntimeException("IO error with connection: " + e.getMessage(), e);
            } catch (java.nio.BufferOverflowException e) {
                System.err.printf("缓冲区溢出: %s:%d, 错误: %s%n", host, port, e.getMessage());
                // 🔧 BufferOverflowException通常是数据问题，连接可能还是好的
                // 但为了安全起见，也不归还连接
                returnToPool = false;
                failedRequests.incrementAndGet();
                throw new RuntimeException("Buffer overflow: " + e.getMessage(), e);
            } catch (Exception e) {
                System.err.printf("连接错误: %s:%d, 错误: %s%n", host, port, e.getMessage());
                // 🔧 对于未知异常，检查连接状态来决定是否归还
                if (socket != null && !socket.isClosed() && socket.isConnected()) {
                    System.out.printf("未知异常但连接仍有效: %s:%d, 尝试归还连接\n", host, port);
                    // 连接看起来还是好的，可以尝试归还
                } else {
                    returnToPool = false;
                }
                failedRequests.incrementAndGet();
                throw new RuntimeException(e);
            } finally {
                // 如果连接没有放回池中，关闭它
                if (socket != null && (!returnToPool || closed.get())) {
                    try {
                        socket.close();
                        totalConnections.decrementAndGet();
                    } catch (IOException e) {
                        System.err.printf("关闭连接错误: %s:%d, 错误: %s%n", host, port, e.getMessage());
                    }
                }
            }
        }
        
        public void close() {
            if (closed.compareAndSet(false, true)) {
                System.out.printf("关闭连接池: %s:%d%n", host, port);
                // 关闭所有连接
                Socket socket;
                int closedCount = 0;
                while ((socket = socketPool.poll()) != null) {
                    try {
                        socket.close();
                        closedCount++;
                    } catch (IOException e) {
                        System.err.printf("关闭连接错误: %s:%d, 错误: %s%n", host, port, e.getMessage());
                    }
                }
                System.out.printf("已关闭 %d 个连接: %s:%d, 成功请求: %d, 失败请求: %d%n", 
                    closedCount, host, port, successfulRequests.get(), failedRequests.get());
            }
        }
    }
}
