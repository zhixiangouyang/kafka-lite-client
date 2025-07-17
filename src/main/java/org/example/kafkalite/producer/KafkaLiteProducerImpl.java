package org.example.kafkalite.producer;

import java.util.List;

public class KafkaLiteProducerImpl implements KafkaLiteProducer{
    private final List<String> bootstrapServers;
    private final Partitioner partitioner;
    // TODO:后续支持动态获取
    private final int partitionCount;

    public KafkaLiteProducerImpl(List<String> bootstrapServers, Partitioner partitioner, int partitionCount) {
        this.bootstrapServers = bootstrapServers;
        this.partitioner = partitioner;
        this.partitionCount = partitionCount;
    }

    @Override
    public void send(String topic, String key, String value) {
        int partition = partitioner.partition(topic, key, partitionCount);
        String broker = selectBrokerForPartition(partition);

        // TODO:后续实现通过Kafka协议建立连接并发送
        System.out.printf("Send to topic=%s, partition=%d, broker=%s, key=%s, value=%s\\n",
                topic, partition, broker, key, value);

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

    private String selectBrokerForPartition(int partition) {
        // TODO:后续通过metadata决定该partiton的leader broker
        return bootstrapServers.get(partition % bootstrapServers.size());
    }

}
