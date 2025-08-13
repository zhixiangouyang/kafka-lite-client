package org.example.kafkalite.client;

import org.example.kafkalite.consumer.ConsumerConfig;
import org.example.kafkalite.consumer.ConsumerRecord;
import org.example.kafkalite.consumer.KafkaLiteConsumer;
import org.example.kafkalite.consumer.KafkaLiteConsumerImpl;

import java.util.Arrays;
import java.util.List;

/**
 * 调试Fetch问题的特殊版本
 * 添加更多日志来诊断为什么拉取不到消息
 */
public class DebugFetchIssue {
    public static void main(String[] args) {
        String broker = "10.251.183.199:27462";
        
        ConsumerConfig config = new ConsumerConfig();
        config.setEnableAutoCommit(false);  // 禁用自动提交，手动控制
        config.setFetchMaxBytes(1024 * 1024);
        
        KafkaLiteConsumer consumer = new KafkaLiteConsumerImpl(
            "debug-fetch-test-group",  // 新的测试组
            Arrays.asList(broker),
            config
        );
        
        try {
            System.out.println("=== 🔍 开始调试Fetch问题 ===");
            
            // 订阅topic
            consumer.subscribe(Arrays.asList("ouyangTest6"));
            
            System.out.println("✅ 订阅完成，开始调试poll...");
            
            // 进行几次poll来看具体问题
            for (int i = 0; i < 3; i++) {
                System.out.printf("\n--- 第%d次Poll ---\n", i + 1);
                
                long startTime = System.currentTimeMillis();
                List<ConsumerRecord> records = consumer.poll(5000);  // 5秒超时
                long duration = System.currentTimeMillis() - startTime;
                
                System.out.printf("Poll结果: 消息数=%d, 耗时=%dms\n", records.size(), duration);
                
                if (!records.isEmpty()) {
                    System.out.println("🎉 拉取到消息了！");
                    for (ConsumerRecord record : records) {
                        System.out.printf("  消息: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
                            record.getTopic(),
                            record.getPartition(), 
                            record.getOffset(),
                            record.getKey(),
                            record.getValue() != null ? record.getValue().substring(0, Math.min(50, record.getValue().length())) + "..." : "null"
                        );
                    }
                    break;  // 拉取到消息就退出
                } else {
                    System.out.println("❌ 没有拉取到消息");
                }
                
                Thread.sleep(1000);  // 等待1秒再试
            }
            
        } catch (Exception e) {
            System.err.println("❌ 调试过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("=== 🏁 调试结束 ===");
        }
    }
} 