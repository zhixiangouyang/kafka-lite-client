package org.example.kafkalite.client;

import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.core.KafkaSocketClient;

import java.util.Arrays;
import java.util.List;

/**
 * 基础连接测试
 * 只测试broker连接和元数据获取，不涉及消费者组
 */
public class BasicConnectionTest {
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 基础连接测试开始 ===");
        
        // 配置多个broker地址
        List<String> bootstrapServers = Arrays.asList(
            "localhost:9093",  // broker1
            "localhost:9094"   // broker2
        );
        
        MetadataManagerImpl metadataManager = null;
        
        try {
            // 创建元数据管理器
            metadataManager = new MetadataManagerImpl(bootstrapServers);
            System.out.println("元数据管理器创建成功");
            
            // 测试连接到各个broker
            for (String server : bootstrapServers) {
                System.out.println("\n=== 测试连接: " + server + " ===");
                testBrokerConnection(server);
            }
            
            // 测试元数据获取
            System.out.println("\n=== 测试元数据获取 ===");
            testMetadataRetrieval(metadataManager);
            
            // 模拟broker故障测试
            System.out.println("\n=== Broker故障模拟测试说明 ===");
            System.out.println("现在可以进行以下测试：");
            System.out.println("1. 保持当前程序运行");
            System.out.println("2. 在另一个终端停止其中一个broker：");
            System.out.println("   - 停止9093: 在kafka集群目录执行停止命令");
            System.out.println("   - 停止9094: 同样停止对应的broker");
            System.out.println("3. 观察客户端重新获取元数据的行为");
            
            // 持续测试连接120秒（2分钟），给你足够时间手动停止broker
            for (int i = 0; i < 120; i++) {
                try {
                    System.out.printf("[%02d秒] 测试元数据刷新...\n", i + 1);
                    
                    // 每3秒刷新一次元数据，更频繁地检测变化
                    if (i % 3 == 0) {
                        testMetadataRetrieval(metadataManager);
                    }
                    
                    // 每30秒提醒一次操作
                    if (i % 30 == 0 && i > 0) {
                        System.out.println("\n🔄 提醒：现在可以停止一个broker来测试故障切换！");
                        System.out.println("   停止命令示例：./bin/kafka-server-stop.sh");
                    }
                    
                    Thread.sleep(1000);
                } catch (Exception e) {
                    System.err.println("测试过程中出现异常: " + e.getMessage());
                }
            }
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (metadataManager != null) {
                try {
                    metadataManager.close();
                    System.out.println("元数据管理器已关闭");
                } catch (Exception e) {
                    System.err.println("关闭元数据管理器时出现异常: " + e.getMessage());
                }
            }
        }
        
        System.out.println("\n=== 基础连接测试结束 ===");
    }
    
    private static void testBrokerConnection(String server) {
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try {
            // 使用KafkaSocketClient的静态方法测试连接
            // 创建一个简单的测试请求（空请求）
            java.nio.ByteBuffer testRequest = java.nio.ByteBuffer.allocate(8);
            testRequest.putInt(4); // 请求长度
            testRequest.putInt(0); // 测试correlationId
            testRequest.flip();
            
            // 尝试发送请求，如果成功说明连接正常
            java.nio.ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, testRequest);
            System.out.println("✅ 连接成功: " + server);
        } catch (Exception e) {
            System.err.println("❌ 连接失败: " + server + " - " + e.getMessage());
        }
    }
    
    private static void testMetadataRetrieval(MetadataManager metadataManager) {
        try {
            // 尝试获取指定topic的元数据
            metadataManager.refreshMetadata("cluster-test-topic");
            System.out.println("✅ 元数据获取成功");
        } catch (Exception e) {
            System.err.println("❌ 元数据获取失败: " + e.getMessage());
        }
    }
} 