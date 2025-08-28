package org.example.kafkalite.client;

import org.example.kafkalite.producer.*;

import java.util.Arrays;

/**
 * ProduceResponse响应处理测试
 * 验证生产者是否正确处理acks=-1时的响应和错误码
 */
public class ProduceResponseTest {
    
    public static void main(String[] args) {
        System.out.println("=== ProduceResponse响应处理测试 ===");
        System.out.println("测试生产者响应处理，特别是acks=-1时的副本确认机制");
        System.out.println("现在会解析ProduceResponse，处理错误码和baseOffset");
        System.out.println();
        
        // 配置broker地址
        String broker;
        if (args.length > 0) {
            broker = args[0];
        } else {
            broker = "localhost:9092"; // 默认broker地址
        }
        System.out.println("🔗 使用Broker: " + broker);
        
        // 测试不同的acks配置
        testProducerWithAcks(broker, (short) 1);  // acks=1 (leader确认)
        testProducerWithAcks(broker, (short) -1); // acks=-1 (所有副本确认)
        
        System.out.println("=== 测试完成 ===");
        System.out.println();
        System.out.println("🔍 关键观察点:");
        System.out.println("1. [ProduceResponseParser] 解析完成日志");
        System.out.println("2. ✅ [Producer] 成功发送消息，包含baseOffset");
        System.out.println("3. ⚠️ [Producer] Broker节流时间（如果有）");
        System.out.println("4. ❌ [Producer] 发送失败时的具体错误码和描述");
        System.out.println("5. 🔄 [Producer] 可重试错误 vs 💀 不可重试错误");
        System.out.println("6. acks=-1时确保所有副本都确认后才返回成功");
    }
    
    private static void testProducerWithAcks(String broker, short acks) {
        System.out.println("========================================");
        System.out.printf("🧪 测试acks=%d配置\n", acks);
        if (acks == -1) {
            System.out.println("   所有副本必须确认后才算发送成功");
        } else if (acks == 1) {
            System.out.println("   仅leader确认后就算发送成功");
        }
        System.out.println("========================================");
        
        KafkaLiteProducerImpl producer = null;
        try {
            // 创建生产者配置
            ProducerConfig config = new ProducerConfig.Builder()
                    .batchSize(500)     // 较小批次，便于观察单个响应
                    .lingerMs(10)       // 短等待时间
                    .maxRetries(2)      // 较少重试次数，便于测试
                    .compressionType("none") // 无压缩，便于调试
                    .acks(acks)         // 关键：测试不同的acks配置
                    .build();
            
            // 创建生产者实例
            producer = new KafkaLiteProducerImpl(
                    Arrays.asList(broker),
                    new HashPartitioner(),
                    config
            );
            
            System.out.printf("✅ 生产者创建成功，acks=%d\n", acks);
            System.out.println("📋 观察以下日志:");
            System.out.println("  - [ProduceResponseParser] 解析完成");
            System.out.println("  - [Producer] 成功/失败信息");
            System.out.println("  - baseOffset和错误码信息");
            System.out.println();
            
            // 发送测试消息
            String testTopic = "response-test-topic"; // 请根据实际情况修改
            for (int i = 0; i < 3; i++) {
                try {
                    String message = String.format("测试消息-%d (acks=%d)", i, acks);
                    ProducerRecord record = new ProducerRecord(testTopic, "key-" + i, message);
                    
                    System.out.printf("🚀 发送消息 %d: %s\n", i + 1, message);
                    
                    // 使用同步发送，便于观察响应
                    producer.sendSync(record);
                    
                    System.out.printf("✅ 消息 %d 发送成功\n", i + 1);
                    
                    // 稍微等待，让日志输出完整
                    Thread.sleep(500);
                    
                } catch (Exception e) {
                    System.err.printf("❌ 消息 %d 发送失败: %s\n", i + 1, e.getMessage());
                    
                    // 分析错误类型
                    if (e.getMessage().contains("NOT_ENOUGH_REPLICAS")) {
                        System.err.println("💡 提示: 可能是副本不足，请检查topic的副本配置");
                    } else if (e.getMessage().contains("LEADER_NOT_AVAILABLE")) {
                        System.err.println("💡 提示: leader不可用，可能是分区正在重新选举");
                    }
                }
            }
            
            System.out.printf("✅ acks=%d 测试完成\n", acks);
            
        } catch (Exception e) {
            System.err.printf("❌ 生产者测试失败: acks=%d, 错误=%s\n", acks, e.getMessage());
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
                System.out.printf("🔒 acks=%d 生产者已关闭\n", acks);
            }
        }
        
        System.out.println();
    }
}

/**
 * 运行说明:
 * 
 * 1. 编译运行:
 *    java -cp target/classes org.example.kafkalite.client.ProduceResponseTest
 * 
 * 2. 指定broker:
 *    java -cp target/classes org.example.kafkalite.client.ProduceResponseTest "your-broker:9092"
 * 
 * 3. 预期效果:
 *    - acks=1: 较快响应，只需leader确认
 *    - acks=-1: 较慢响应，需要所有副本确认
 *    - 可以看到详细的错误码处理
 *    - 可重试 vs 不可重试错误的区分
 * 
 * 4. 测试场景:
 *    - 正常发送: 观察baseOffset递增
 *    - 副本不足: acks=-1时可能触发NOT_ENOUGH_REPLICAS错误
 *    - 网络问题: 观察重试逻辑
 *    - Broker节流: 观察throttle_time_ms字段
 */ 