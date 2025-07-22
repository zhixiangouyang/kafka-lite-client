package org.example.kafkalite.producer;

import org.example.kafkalite.core.KafkaSocketClient;
import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.protocol.KafkaRecordEncoder;
import org.example.kafkalite.protocol.ProduceRequestBuilder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class KafkaLiteProducerImpl implements KafkaLiteProducer{

    private final Partitioner partitioner;
    private final MetadataManager metadataManager;


    public KafkaLiteProducerImpl(List<String> bootstrapServers, Partitioner partitioner, int partitionCount) {
        this.partitioner = partitioner;
        this.metadataManager = new MetadataManagerImpl(bootstrapServers);
    }

    @Override
    public void send(String topic, String key, String value) {
        try {// 1. 刷新元数据（若已存在可内部跳过）
            metadataManager.refreshMetadata(topic);

            // 2. 获取partition -> broker 映射
            Map<Integer, String> partitionToBroker = metadataManager.getPartitionLeaders(topic);
            int partitionCount = partitionToBroker.size();

            if (partitionCount == 0) {
                throw new RuntimeException("No partition found for topic: " + topic);
            }

            // 3. 使用分区器决定发送到哪个分区
            int partition = partitioner.partition(topic, key, partitionCount);

            // 4. 获取 partition 对应的 broker
            String brokerAddress = partitionToBroker.get(partition);
            if (brokerAddress == null) {
                throw new RuntimeException("No leader broker found for topic = " + topic + ", partition = " + partition);
            }

            String[] parts = brokerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            // 5. 构造 RecordBatch（Kafka协议标准格式）
            ByteBuffer recordBatch = KafkaRecordEncoder.encodeRecordBatch(key, value);

            // 6. 构造ProduceRequest（包含header + body）
            ByteBuffer request = ProduceRequestBuilder.build(
                    "kafka-lite", topic, partition, recordBatch,
                    (short) 1,  //acks
                    3000,
                    1
            );

            // 7. 通过socket发送
            KafkaSocketClient.sendAndReceive(host, port, request);

            System.out.printf("Sent message to topic=%s, partition=%d, broker=%s, key=%s, value=%s%n",
                    topic, partition, brokerAddress, key, value);

        } catch (Exception e) {
            System.err.println("Send failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        // TODO:flush方法
        System.out.println("Flushing producer");

    }

    @Override
    public void close() {
        // TODO:clean up connection
        System.out.println("Closing producer");

    }

}
