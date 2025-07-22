package org.example.kafkalite.core;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class KafkaSocketClient {

    /**
     * 发送请求并接受Kafka响应
     */
    public static ByteBuffer sendAndReceive(String brokerHost, int brokerPort, ByteBuffer request) throws Exception {
        try (Socket socket = new Socket(brokerHost, brokerPort)) {
            socket.setSoTimeout(3000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // ————发送请求————
            byte[] requestBytes = new byte[request.remaining()];
            request.get(requestBytes);
            out.write(requestBytes);
            out.flush();

            // ————读取响应长度（前4字节）————
            byte[] lenBytes = new byte[4];
            readFully(in, lenBytes, 0, 4);
            int responseLength = ByteBuffer.wrap(lenBytes).getInt();

            // ————读取响应体————
            byte[] respBytes = new byte[responseLength];
            readFully(in, respBytes, 0, responseLength);

            return ByteBuffer.wrap(respBytes);
        }
    }

    /**
     * 阻塞式读取，直到读满 length个字节
     */
    private static void readFully(InputStream in, byte[] buf, int off, int len) throws Exception {
        int read = 0;
        while (read < len) {
            int r = in.read(buf, off + read, len - read);
            if (r == -1) throw new RuntimeException("stream closed");
            read += r;
        }
    }

}
