package org.example.kafkalite.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class KafkaSingleSocketClient {
    private final String host;
    private final int port;
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private final Object lock = new Object();

    public KafkaSingleSocketClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setSoTimeout(30000);
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    public ByteBuffer sendAndReceive(ByteBuffer request) throws IOException {
        synchronized (lock) {
            try {
                // 发送请求
                byte[] requestBytes = new byte[request.remaining()];
                request.get(requestBytes);
                out.write(requestBytes);
                out.flush();

                // 读取响应长度
                byte[] sizeBuf = new byte[4];
                readFully(in, sizeBuf);
                int size = ByteBuffer.wrap(sizeBuf).getInt();
                if (size < 0 || size > 10 * 1024 * 1024) {
                    throw new IllegalArgumentException("Invalid response size: " + size);
                }
                // 读取响应内容
                byte[] responseBuf = new byte[size + 4];
                System.arraycopy(sizeBuf, 0, responseBuf, 0, 4);
                readFully(in, responseBuf, 4, size);
                return ByteBuffer.wrap(responseBuf);
            } catch (IOException e) {
                close();
                throw e;
            }
        }
    }

    private void readFully(InputStream in, byte[] buf) throws IOException {
        readFully(in, buf, 0, buf.length);
    }
    private void readFully(InputStream in, byte[] buf, int offset, int len) throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf, offset + totalRead, len - totalRead);
            if (read == -1) {
                throw new IOException("End of stream reached after reading " + totalRead + " bytes, expected " + len + " bytes");
            }
            totalRead += read;
        }
    }

    public void close() {
        synchronized (lock) {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
} 