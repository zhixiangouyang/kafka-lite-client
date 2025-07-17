package org.example.kafkalite.producer;

import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;

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
        // 1. 刷新元数据（若已存在可内部跳过）
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

        //5. 模拟发送（下一阶段用Kafka真正发送）
        System.out.printf("Send to topic=%s, partition=%d, leaderBroker=%s, key=%s, value=%s%n",
                topic, partition, brokerAddress, key, value);
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
