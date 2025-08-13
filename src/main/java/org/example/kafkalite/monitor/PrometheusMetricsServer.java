package org.example.kafkalite.monitor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Prometheus指标HTTP服务器
 * 负责暴露/metrics端点供Prometheus抓取指标数据
 */
public class PrometheusMetricsServer {
    private final MetricsCollector metricsCollector;
    private final int port;
    private HttpServer server;
    private volatile boolean running = false;
    
    public PrometheusMetricsServer(MetricsCollector metricsCollector, int port) {
        this.metricsCollector = metricsCollector;
        this.port = port;
    }
    
    public PrometheusMetricsServer(MetricsCollector metricsCollector) {
        this(metricsCollector, 8080); // 默认端口8080
    }
    
    /**
     * 启动HTTP服务器
     */
    public void start() throws IOException {
        if (running) {
            System.out.println("[PrometheusMetricsServer] 服务器已经在运行中，端口: " + port);
            return;
        }
        
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/health", new HealthHandler());
        server.setExecutor(Executors.newFixedThreadPool(2)); // 使用线程池处理请求
        
        server.start();
        running = true;
        
        System.out.printf("[PrometheusMetricsServer] ✅ Prometheus指标服务器已启动\n");
        System.out.printf("  📊 指标端点: http://localhost:%d/metrics\n", port);
        System.out.printf("  💚 健康检查: http://localhost:%d/health\n", port);
        System.out.printf("  🔧 线程池: 2个工作线程\n");
    }
    
    /**
     * 停止HTTP服务器
     */
    public void stop() {
        if (!running || server == null) {
            System.out.println("[PrometheusMetricsServer] 服务器未在运行或已停止");
            return;
        }
        
        server.stop(5); // 等待5秒钟让现有请求完成
        running = false;
        
        System.out.printf("[PrometheusMetricsServer] ❌ Prometheus指标服务器已停止 (端口: %d)\n", port);
    }
    
    /**
     * 检查服务器是否正在运行
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * 获取服务器端口
     */
    public int getPort() {
        return port;
    }
    
    /**
     * 处理/metrics请求的Handler
     */
    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                // 处理CORS预检请求
                if ("OPTIONS".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                    exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
                    exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                    sendResponse(exchange, 200, "");
                    return;
                }
                
                if (!"GET".equals(exchange.getRequestMethod())) {
                    // 只支持GET请求
                    sendResponse(exchange, 405, "Method Not Allowed");
                    return;
                }
                
                // 生成Prometheus格式的指标数据
                String metricsData = metricsCollector.toPrometheusFormat();
                
                // 设置响应头
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control", "no-cache");
                // 添加CORS头，允许浏览器跨域访问
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
                exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                
                // 发送响应
                sendResponse(exchange, 200, metricsData);
                
                // 记录访问日志
                String clientAddress = exchange.getRemoteAddress().getAddress().getHostAddress();
                System.out.printf("[PrometheusMetricsServer] 📊 指标请求: %s -> 200 OK (%d bytes)\n", 
                    clientAddress, metricsData.getBytes(StandardCharsets.UTF_8).length);
                
            } catch (Exception e) {
                System.err.printf("[PrometheusMetricsServer] ❌ 处理指标请求时出错: %s\n", e.getMessage());
                e.printStackTrace();
                
                try {
                    sendResponse(exchange, 500, "Internal Server Error: " + e.getMessage());
                } catch (IOException ioException) {
                    System.err.printf("[PrometheusMetricsServer] ❌ 发送错误响应失败: %s\n", ioException.getMessage());
                }
            }
        }
    }
    
    /**
     * 处理/health请求的Handler
     */
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                // 处理CORS预检请求
                if ("OPTIONS".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                    exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
                    exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                    sendResponse(exchange, 200, "");
                    return;
                }
                
                if (!"GET".equals(exchange.getRequestMethod())) {
                    sendResponse(exchange, 405, "Method Not Allowed");
                    return;
                }
                
                // 生成健康检查响应
                String healthResponse = generateHealthResponse();
                
                // 设置响应头
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                // 添加CORS头，允许浏览器跨域访问
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
                exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                
                // 发送响应
                sendResponse(exchange, 200, healthResponse);
                
            } catch (Exception e) {
                System.err.printf("[PrometheusMetricsServer] ❌ 处理健康检查请求时出错: %s\n", e.getMessage());
                sendResponse(exchange, 500, "{\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
            }
        }
        
        private String generateHealthResponse() {
            long currentTime = System.currentTimeMillis();
            return String.format(
                "{\n" +
                "  \"status\": \"UP\",\n" +
                "  \"timestamp\": %d,\n" +
                "  \"server\": {\n" +
                "    \"port\": %d,\n" +
                "    \"running\": %s\n" +
                "  },\n" +
                "  \"metrics\": {\n" +
                "    \"collector_class\": \"%s\"\n" +
                "  }\n" +
                "}",
                currentTime,
                port,
                running,
                metricsCollector.getClass().getSimpleName()
            );
        }
    }
    
    /**
     * 发送HTTP响应的辅助方法
     */
    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
            os.flush();
        }
    }
    
    /**
     * 创建一个便捷的工厂方法
     */
    public static PrometheusMetricsServer create(MetricsCollector metricsCollector, int port) {
        return new PrometheusMetricsServer(metricsCollector, port);
    }
    
    /**
     * 创建一个使用默认端口的便捷工厂方法
     */
    public static PrometheusMetricsServer create(MetricsCollector metricsCollector) {
        return new PrometheusMetricsServer(metricsCollector);
    }
} 