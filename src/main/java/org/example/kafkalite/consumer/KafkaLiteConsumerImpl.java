package org.example.kafkalite.consumer;

import org.example.kafkalite.metadata.MetadataManager;
import org.example.kafkalite.metadata.MetadataManagerImpl;
import org.example.kafkalite.monitor.MetricsCollector;
import org.example.kafkalite.protocol.FetchRequestBuilder;
import org.example.kafkalite.protocol.FetchResponseParser;
import org.example.kafkalite.protocol.SyncGroupResponseParser;
import org.example.kafkalite.core.KafkaSocketClient;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaLiteConsumerImpl implements KafkaLiteConsumer {
    private volatile List<String> bootstrapServers;  // 改为volatile，支持DNS重解析后更新
    private final String groupId;
    private final String clientId;
    private final MetadataManager metadataManager;
    private final OffsetManager offsetManager;
    private final ConsumerConfig config;
    private final MetricsCollector metricsCollector;
    private final ConsumerCoordinator coordinator;
    private List<String> subscribedTopics = new ArrayList<>();
    private final Map<String, Map<Integer, String>> topicPartitionLeaders = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    // 新增：定期元数据刷新相关
    private ScheduledExecutorService metadataRefreshExecutor;
    private final AtomicBoolean metadataRefreshStarted = new AtomicBoolean(false);
    
    public KafkaLiteConsumerImpl(String groupId, List<String> bootstrapServers, ConsumerConfig config) {
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
        this.config = config;
        // 修复：使用基于groupId的固定clientId，确保offset持久化
        this.clientId = "kafka-lite-" + groupId.replaceAll("[^a-zA-Z0-9-]", "-");
        
        // 使用配置的连接池大小创建MetadataManager
        this.metadataManager = new MetadataManagerImpl(bootstrapServers, config.getMetadataConnectionPoolSize());
        this.offsetManager = new OffsetManager(groupId, bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.coordinator = new ConsumerCoordinator(clientId, groupId, config, bootstrapServers);
        // 共享 MetadataManager，避免重复创建连接池
        this.coordinator.setMetadataManager(this.metadataManager);
        this.offsetManager.setCoordinator(this.coordinator);
        this.offsetManager.setConfig(config); // 新增：设置配置
        // coordinatorSocket在coordinator.initializeGroup()后才会被创建
    }
    
    /**
     * 新增：支持域名的构造函数
     * 支持传入域名:端口形式，自动解析为多个IP地址
     */
    public KafkaLiteConsumerImpl(String groupId, String domainWithPort, ConsumerConfig config) {
        this.groupId = groupId;
        this.config = config;
        // 修复：使用基于groupId的固定clientId，确保offset持久化
        this.clientId = "kafka-lite-" + groupId.replaceAll("[^a-zA-Z0-9-]", "-");
        
        // 解析域名为IP列表
        this.bootstrapServers = resolveDomainToIPs(domainWithPort);
        System.out.printf("[KafkaLiteConsumerImpl] 域名 %s 解析到 %d 个IP: %s\n", 
            domainWithPort, bootstrapServers.size(), bootstrapServers);
        
        // 使用配置的连接池大小创建支持动态DNS的MetadataManager
        this.metadataManager = new MetadataManagerImpl(bootstrapServers, config.getMetadataConnectionPoolSize(), domainWithPort);
        this.offsetManager = new OffsetManager(groupId, bootstrapServers);
        this.metricsCollector = new MetricsCollector();
        this.coordinator = new ConsumerCoordinator(clientId, groupId, config, bootstrapServers);
        // 共享 MetadataManager，避免重复创建连接池
        this.coordinator.setMetadataManager(this.metadataManager);
        this.offsetManager.setCoordinator(this.coordinator);
        this.offsetManager.setConfig(config); // 新增：设置配置
        
        // 设置bootstrap servers变化回调，处理DNS重解析后的连接更新
        if (this.metadataManager instanceof MetadataManagerImpl) {
            ((MetadataManagerImpl) this.metadataManager).setBootstrapServersChangedCallback(() -> {
                handleBootstrapServersChanged();
            });
        }
        // coordinatorSocket在coordinator.initializeGroup()后才会被创建
    }
    
    /**
     * 解析域名为IP地址列表
     */
    private List<String> resolveDomainToIPs(String domainWithPort) {
        List<String> ips = new ArrayList<>();
        
        String[] parts = domainWithPort.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("域名格式错误，应为 domain:port，实际: " + domainWithPort);
        }
        
        String domain = parts[0];
        String port = parts[1];
        
        // 如果已经是IP地址，直接返回
        if (isIpAddress(domain)) {
            ips.add(domainWithPort);
            return ips;
        }
        
        try {
            java.net.InetAddress[] addresses = java.net.InetAddress.getAllByName(domain);
            for (java.net.InetAddress address : addresses) {
                String ip = address.getHostAddress();
                ips.add(ip + ":" + port);
                System.out.printf("[KafkaLiteConsumerImpl] DNS解析: %s -> %s:%s\n", domain, ip, port);
            }
            
            if (ips.isEmpty()) {
                throw new RuntimeException("域名解析失败，未获取到任何IP: " + domain);
            }
            
        } catch (java.net.UnknownHostException e) {
            throw new RuntimeException("域名解析失败: " + domain + ", 错误: " + e.getMessage(), e);
        }
        
        return ips;
    }
    
    /**
     * 检查是否为IP地址
     */
    private boolean isIpAddress(String host) {
        String ipPattern = "^([0-9]{1,3}\\.){3}[0-9]{1,3}$";
        return host.matches(ipPattern);
    }
    
    /**
     * 处理bootstrap servers变化（DNS重解析后）
     * 更新所有相关组件的连接，重新加入消费者组，重新获取offset
     */
    private void handleBootstrapServersChanged() {
        try {
            System.out.println("=== [集群切换] 开始处理bootstrap servers变化 ===");
            
            // 1. 获取新的bootstrap servers
            List<String> newBootstrapServers = ((MetadataManagerImpl) metadataManager).getBootstrapServers();
            System.out.printf("[集群切换] 新的bootstrap servers: %s\n", newBootstrapServers);
            
            // 2. 更新本地bootstrap servers
            this.bootstrapServers = newBootstrapServers;
            
            // 3. 清空partition leader缓存，强制重新获取
            topicPartitionLeaders.clear();
            System.out.println("[集群切换] 已清空partition leader缓存");
            
            // 4. 更新ConsumerCoordinator的bootstrap servers
            System.out.println("[集群切换] 更新ConsumerCoordinator的bootstrap servers...");
            coordinator.updateBootstrapServers(newBootstrapServers);
            
            // 5. 更新OffsetManager的bootstrap servers（这会清空本地offset缓存）
            System.out.println("[集群切换] 更新OffsetManager的bootstrap servers...");
            offsetManager.updateBootstrapServers(newBootstrapServers);
            
            // 6. 先刷新所有topic的metadata，获取新集群的partition leader信息
            System.out.println("[集群切换] 刷新topic metadata...");
            Map<String, List<Integer>> topicPartitions = new HashMap<>();
            
            boolean metadataRefreshSuccess = false;
            int maxRetries = 3;
            
            for (int retry = 0; retry < maxRetries && !metadataRefreshSuccess; retry++) {
                if (retry > 0) {
                    System.out.printf("[集群切换] 元数据刷新重试 %d/%d\n", retry, maxRetries - 1);
                    try {
                        Thread.sleep(2000); // 等待2秒后重试
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                boolean allTopicsSuccess = true;
            for (String topic : subscribedTopics) {
                try {
                        // 强制刷新元数据，跳过智能策略
                        if (metadataManager instanceof MetadataManagerImpl) {
                            ((MetadataManagerImpl) metadataManager).forceRefreshMetadata(topic);
                        } else {
                    metadataManager.refreshMetadata(topic, true, false); // error-triggered refresh
                        }
                    Map<Integer, String> leaders = metadataManager.getPartitionLeaders(topic);
                        
                        if (leaders != null && !leaders.isEmpty()) {
                    topicPartitionLeaders.put(topic, leaders);
                            topicPartitions.put(topic, new ArrayList<>(leaders.keySet()));
                            System.out.printf("[集群切换] 已更新topic %s 的partition leaders: %s\n", topic, leaders);
                        } else {
                            System.err.printf("[集群切换] topic %s 未获取到partition leader信息\n", topic);
                            allTopicsSuccess = false;
                        }
                    } catch (Exception e) {
                        System.err.printf("[集群切换] 更新topic %s metadata失败: %s\n", topic, e.getMessage());
                        allTopicsSuccess = false;
                    }
                }
                
                if (allTopicsSuccess) {
                    metadataRefreshSuccess = true;
                } else {
                    System.err.printf("[集群切换] 元数据刷新未完全成功，重试...\n");
                }
            }
            
            if (!metadataRefreshSuccess) {
                System.err.println("[集群切换] 元数据刷新失败，跳过后续操作");
                return;
            }
            
            // 7. 关键修复：触发重新加入消费者组（在元数据更新后）
            System.out.println("[集群切换] 触发重新加入消费者组...");
            coordinator.triggerRejoinGroup();
            
            // 8. 等待coordinator稳定
            waitForCoordinatorStable();
            
            // 9. 关键修复：重新从新集群获取已提交的offset
            if (!topicPartitions.isEmpty() && coordinator.isStable()) {
                System.out.println("[集群切换] 重新获取已提交的offset...");
                try {
                    offsetManager.fetchCommittedOffsets(subscribedTopics, topicPartitions);
                    System.out.println("[集群切换] 已重新获取offset信息");
                } catch (Exception e) {
                    System.err.printf("[集群切换] 重新获取offset失败: %s\n", e.getMessage());
                }
            } else {
                System.err.println("[集群切换] 跳过offset获取：分区信息为空或coordinator未稳定");
            }
            
            System.out.println("=== [集群切换] bootstrap servers变化处理完成 ===");
            
        } catch (Exception e) {
            System.err.printf("[集群切换] 处理bootstrap servers变化失败: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void subscribe(List<String> topics) {
        this.subscribedTopics = new ArrayList<>(topics);  // 创建可变副本
        
        // 刷新元数据
        for (String topic : topics) {
            // 初始化时刷新元数据 - 正常情况
            metadataManager.refreshMetadata(topic, false, false);
            topicPartitionLeaders.put(topic, metadataManager.getPartitionLeaders(topic));
        }
        
        // 初始化消费者组
        coordinator.initializeGroup(topics);
        // 新增：获取分区列表并拉取 group offset topic ->
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        for (String topic : topics) {
            Map<Integer, String> leaders = topicPartitionLeaders.get(topic);
            if (leaders != null) {
                topicPartitions.put(topic, new ArrayList<>(leaders.keySet()));
            }
        }
        offsetManager.fetchCommittedOffsets(topics, topicPartitions);
        
        // 新增：启动定期元数据刷新
        startPeriodicMetadataRefresh();
    }
    
    // 新增：启动定期元数据刷新
    private void startPeriodicMetadataRefresh() {
        if (!config.isEnablePeriodicMetadataRefresh()) {
            System.out.println("[KafkaLiteConsumerImpl] 定期元数据刷新已禁用");
            return;
        }
        
        if (metadataRefreshStarted.compareAndSet(false, true)) {
            metadataRefreshExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "metadata-refresh-" + clientId);
                t.setDaemon(true);
                return t;
            });
            
            long intervalMs = config.getMetadataRefreshIntervalMs();
            System.out.printf("[KafkaLiteConsumerImpl] 启动定期元数据刷新: 间隔=%dms, 客户端=%s\n", intervalMs, clientId);
            
            metadataRefreshExecutor.scheduleAtFixedRate(() -> {
                try {
                    if (closed.get()) {
                        return;
                    }
                    
                    System.out.printf("[KafkaLiteConsumerImpl] 执行定期元数据刷新: 客户端=%s, topics=%s\n", clientId, subscribedTopics);
                    
                    // 刷新所有订阅的topic的元数据
                    for (String topic : subscribedTopics) {
                        try {
                            // 定期刷新元数据 - 正常情况
                            metadataManager.refreshMetadata(topic, false, false);
                            Map<Integer, String> leaders = metadataManager.getPartitionLeaders(topic);
                            topicPartitionLeaders.put(topic, leaders);
                            
                            System.out.printf("[KafkaLiteConsumerImpl] 定期刷新完成: topic=%s, leaders=%s\n", topic, leaders);
                        } catch (Exception e) {
                            System.err.printf("[KafkaLiteConsumerImpl] 定期刷新元数据失败: topic=%s, 错误=%s\n", topic, e.getMessage());
                        }
                    }
                    
                } catch (Exception e) {
                    System.err.printf("[KafkaLiteConsumerImpl] 定期元数据刷新异常: 客户端=%s, 错误=%s\n", clientId, e.getMessage());
                }
            }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
            
            System.out.printf("[KafkaLiteConsumerImpl] 定期元数据刷新已启动: 客户端=%s\n", clientId);
        }
    }
    
    // 新增：停止定期元数据刷新
    private void stopPeriodicMetadataRefresh() {
        if (metadataRefreshExecutor != null) {
            System.out.printf("[KafkaLiteConsumerImpl] 停止定期元数据刷新: 客户端=%s\n", clientId);
            metadataRefreshExecutor.shutdown();
            try {
                if (!metadataRefreshExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    metadataRefreshExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                metadataRefreshExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            metadataRefreshExecutor = null;
        }
    }

    @Override
    public List<ConsumerRecord> poll(long timeoutMs) {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }
        long startTime = System.currentTimeMillis();
        List<ConsumerRecord> allRecords = new ArrayList<>();
        
        // 指标埋点: poll调用计数
        metricsCollector.incrementCounter("consumer.poll.attempt");
        
        System.out.println("[Poll] 开始拉取消息...");
        List<PartitionAssignment> assignments = coordinator.getAssignments();
        System.out.printf("[Poll] 当前分区分配: %s, coordinator.isStable()=%s, coordinator.isRejoining()=%s\n", 
            assignments, coordinator.isStable(), coordinator.isRejoining());
        System.out.printf("[DEBUG] Poll check - clientId=%s, groupId=%s, memberId=%s, generationId=%d\n", 
            clientId, groupId, coordinator.getMemberId(), coordinator.getGenerationId());
        try {
            if (assignments == null || assignments.isEmpty()) {
                System.out.println("[Poll] 当前无分区分配，等待分配变更...");
                synchronized (coordinator.assignmentLock) {
                    coordinator.assignmentLock.wait(timeoutMs > 0 ? timeoutMs : 2000);
                }
                // 再次获取分配
                assignments = coordinator.getAssignments();
                System.out.printf("[Poll] 等待后分区分配: %s\n", assignments);
                if (assignments == null || assignments.isEmpty()) {
                    System.out.println("[Poll] 等待后仍无分区分配，返回空结果");
                    return allRecords;
                }
            }
            
            // 修复：确保遍历所有分区，不要过早退出
            System.out.printf("[Poll] 开始遍历 %d 个分区分配\n", assignments.size());
            
            // 修复：计算每个分区的记录配额，确保所有分区都有机会被处理
            int maxRecordsPerPartition = Math.max(1, config.getMaxPollRecords() / assignments.size());
            int remainingGlobalQuota = config.getMaxPollRecords();
            System.out.printf("[Poll] 每个分区最大记录数: %d, 全局剩余配额: %d\n", 
                maxRecordsPerPartition, remainingGlobalQuota);
            
            for (int i = 0; i < assignments.size(); i++) {
                PartitionAssignment assignment = assignments.get(i);
                String topic = assignment.getTopic();
                int partition = assignment.getPartition();
                
                System.out.printf("[Poll] 处理分区 %d/%d: topic=%s, partition=%d, 已拉取记录数=%d, 剩余配额=%d\n", 
                    i + 1, assignments.size(), topic, partition, allRecords.size(), remainingGlobalQuota);
                
                // 修复：检查全局配额，但给每个分区至少1条消息的机会
                if (remainingGlobalQuota <= 0) {
                    System.out.printf("[Poll] 全局配额已用完，停止处理剩余 %d 个分区\n", assignments.size() - i);
                    break;
                }
                
                // 为当前分区计算可用的记录数配额
                int partitionQuota = Math.min(maxRecordsPerPartition, remainingGlobalQuota);
                // 如果是最后一个分区，可以使用所有剩余配额
                if (i == assignments.size() - 1) {
                    partitionQuota = remainingGlobalQuota;
                }
                
                System.out.printf("[Poll] 分区 %s:%d 的配额: %d 条记录\n", topic, partition, partitionQuota);
                
                Map<Integer, String> partitionLeaders = topicPartitionLeaders.get(topic);
                if (partitionLeaders == null) {
                    System.out.printf("[Poll] 分区 %s:%d 无leader信息，跳过\n", topic, partition);
                    continue;
                }
                String broker = partitionLeaders.get(partition);
                if (broker == null) {
                    System.out.printf("[Poll] 分区 %s:%d 无broker信息，跳过\n", topic, partition);
                    continue;
                }
                String[] parts = broker.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                long offset = offsetManager.getOffset(topic, partition);
                
                // 修复：将重试逻辑包装在try-catch中，避免单个分区失败影响其他分区
                boolean fetchSuccess = false;
                int retryCount = 0;
                int partitionRecordsFetched = 0;
                
                while (retryCount < config.getMaxRetries() && !fetchSuccess) {
                    try {
                        System.out.printf("[Poll] 拉取参数: topic=%s, partition=%d, offset=%d, broker=%s:%d, retry=%d, 配额=%d%n",
                            topic, partition, offset, host, port, retryCount, partitionQuota);

                        // 修复：根据分区配额调整fetchMaxBytes，避免单个分区拉取过多数据
                        int adjustedFetchMaxBytes = Math.min(config.getFetchMaxBytes(), 
                            partitionQuota * 1024); // 假设每条消息平均1KB

                        ByteBuffer fetchRequest = FetchRequestBuilder.build(
                            clientId,
                            topic,
                            partition,
                            offset,
                            adjustedFetchMaxBytes,
                            1,
                            config.getFetchMaxWaitMs()  // 使用配置的等待时间
                        );
                        ByteBuffer response = KafkaSocketClient.sendAndReceive(host, port, fetchRequest);
                        List<ConsumerRecord> records = FetchResponseParser.parse(response);
                        
                        // 修复：限制单个分区返回的记录数，确保不超过分区配额
                        if (records.size() > partitionQuota) {
                            System.out.printf("[Poll] 分区 %s:%d 返回 %d 条记录，超过配额 %d，截取前 %d 条\n", 
                                topic, partition, records.size(), partitionQuota, partitionQuota);
                            records = records.subList(0, partitionQuota);
                        }
                        
                        partitionRecordsFetched = records.size();
                        
                        if (!records.isEmpty()) {
                            long firstOffset = records.get(0).getOffset();
                            long lastOffset = records.get(records.size() - 1).getOffset();
                            System.out.printf("[Poll] 从分区 %s:%d 拉取到%d条消息 (配额:%d), offset范围: [%d, %d]\n", 
                                topic, partition, records.size(), partitionQuota, firstOffset, lastOffset);
                            System.out.printf("[DEBUG] poll调用updateOffset: topic=%s, partition=%d, offset=%d\n", topic, partition, lastOffset+1);
                            offsetManager.updateOffset(topic, partition, lastOffset + 1);
                            
                            // 指标埋点: 成功拉取消息
                            Map<String, String> labels = new HashMap<>();
                            labels.put("topic", topic);
                            labels.put("partition", String.valueOf(partition));
                            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_FETCH_SUCCESS, labels);
                            
                            // 记录拉取的消息数量
                            for (int j = 0; j < records.size(); j++) {
                                metricsCollector.incrementCounter("consumer.records.fetched");
                            }
                            
                        } else {
                            System.out.printf("[Poll] 从分区 %s:%d 拉取到0条消息\n", topic, partition);
                            
                            // 指标埋点: 空拉取
                            Map<String, String> labels = new HashMap<>();
                            labels.put("topic", topic);
                            labels.put("partition", String.valueOf(partition));
                            metricsCollector.incrementCounter("consumer.fetch.empty", labels);
                        }
                        allRecords.addAll(records);
                        remainingGlobalQuota -= partitionRecordsFetched;
                        fetchSuccess = true; // 标记拉取成功
                        
                        System.out.printf("[Poll] 分区 %s:%d 处理完成，本次拉取 %d 条，剩余全局配额 %d\n", 
                            topic, partition, partitionRecordsFetched, remainingGlobalQuota);
                        
                    } catch (Exception e) {
                        System.err.printf("[Poll] 从分区 %s:%d 拉取异常 (重试 %d/%d): %s\n", 
                            topic, partition, retryCount + 1, config.getMaxRetries(), e.getMessage());
                        
                        // 指标埋点: 拉取失败
                        Map<String, String> labels = new HashMap<>();
                        labels.put("topic", topic);
                        labels.put("partition", String.valueOf(partition));
                        labels.put("retry_count", String.valueOf(retryCount));
                        metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_FETCH_ERROR, labels);
                        
                        retryCount++;
                        if (retryCount >= config.getMaxRetries()) {
                            System.err.printf("[Poll] 分区 %s:%d 在 %d 次重试后仍然失败，跳过该分区继续处理其他分区\n", 
                                topic, partition, config.getMaxRetries());
                            // 重试失败后刷新元数据 - 错误触发
                            try {
                            metadataManager.refreshMetadata(topic, true, false);
                            topicPartitionLeaders.put(topic, metadataManager.getPartitionLeaders(topic));
                            } catch (Exception metaEx) {
                                System.err.printf("[Poll] 刷新元数据也失败: %s\n", metaEx.getMessage());
                            }
                            
                            // 指标埋点: 最终拉取失败
                            metricsCollector.incrementCounter("consumer.fetch.final_failure", labels);
                            
                            // 修复：不要抛出异常，而是跳过该分区继续处理其他分区
                            break; // 跳出重试循环，继续处理下一个分区
                        } else {
                            try {
                                Thread.sleep(config.getRetryBackoffMs());
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                System.err.printf("[Poll] 重试等待被中断，跳过分区 %s:%d\n", topic, partition);
                                break; // 跳出重试循环
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[Poll] 拉取过程中发生异常: " + e.getMessage());
            e.printStackTrace();
            // 不要重新抛出异常，而是返回已拉取到的结果，让消费者继续运行
        } finally {
            long endTime = System.currentTimeMillis();
            long pollLatency = endTime - startTime;
            
            // 指标埋点: poll完成统计（只包含拉取时间）
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_POLL);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_POLL, pollLatency);
            
            // 记录拉取的消息总数
            if (!allRecords.isEmpty()) {
                metricsCollector.setGauge("consumer.poll.records_count", allRecords.size());
                metricsCollector.incrementCounter("consumer.poll.success");
            } else {
                metricsCollector.incrementCounter("consumer.poll.empty");
            }
            
            System.out.printf("[Poll] 本次总共从 %d 个分区拉取消息数: %d\n", 
                assignments != null ? assignments.size() : 0, allRecords.size());
            System.out.printf("[DEBUG] poll finally, thread=%s, enableAutoCommit=%s\n", Thread.currentThread().getName(), config.isEnableAutoCommit());
        }
        
        // 自动提交移到finally块外面，单独计算延迟
        if (config.isEnableAutoCommit()) {
            System.out.println("[DEBUG] poll finally自动提交offset");
            performAutoCommit();
        }
        
        return allRecords;
    }

    @Override
    public void commitSync() {
        System.out.printf("[DEBUG] commitSync入口, thread=%s\n", Thread.currentThread().getName());
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }

        long startTime = System.currentTimeMillis();
        try {
            offsetManager.commitSync(coordinator.getGenerationId(), coordinator.getMemberId());
            
            // 指标埋点: 提交成功
            metricsCollector.incrementCounter("consumer.commit.success");
            
        } catch (Exception e) {
            // 指标埋点: 提交失败
            metricsCollector.incrementCounter("consumer.commit.error");
            throw e;
        }
    }

    @Override
    public void commitAsync() {
        System.out.printf("[DEBUG] commitAsync入口, thread=%s\n", Thread.currentThread().getName());
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }

        long startTime = System.currentTimeMillis();
        try {
            offsetManager.commitAsync();
        } finally {
            long endTime = System.currentTimeMillis();
            metricsCollector.incrementCounter(MetricsCollector.METRIC_CONSUMER_COMMIT);
            metricsCollector.recordLatency(MetricsCollector.METRIC_CONSUMER_COMMIT, endTime - startTime);
        }
    }

    @Override
    public void close() {
        // 先提交offset，再设置closed=true，最后关闭coordinator
        if (!closed.get()) {
            try {
                if (config.isEnableAutoCommit()) {
                    commitSync();
                }
                closed.set(true);
                
                // 新增：停止定期元数据刷新
                stopPeriodicMetadataRefresh();
                
                coordinator.close();
                offsetManager.close();
                
                // 新增：关闭MetadataManager（包括连接池）
                if (metadataManager instanceof MetadataManagerImpl) {
                    ((MetadataManagerImpl) metadataManager).close();
                }
                
            } finally {
                // 清理资源
                subscribedTopics = new ArrayList<>();
                topicPartitionLeaders.clear();
            }
        }
    }

    // 获取消费监控指标
    public double getConsumerQPS() {
        return metricsCollector.getQPS(MetricsCollector.METRIC_CONSUMER_POLL);
    }

    public double getConsumerP99Latency() {
        return metricsCollector.getP99Latency(MetricsCollector.METRIC_CONSUMER_POLL);
    }

    public double getCommitQPS() {
        return metricsCollector.getQPS(MetricsCollector.METRIC_CONSUMER_COMMIT);
    }

    public double getCommitP99Latency() {
        return metricsCollector.getP99Latency(MetricsCollector.METRIC_CONSUMER_COMMIT);
    }
    
    /**
     * 等待coordinator稳定的辅助方法
     */
    private void waitForCoordinatorStable() {
        int maxWaitTime = 15000; // 增加到15秒超时
        int waitTime = 0;
        System.out.println("[KafkaLiteConsumerImpl] 等待coordinator稳定...");
        
        while ((!coordinator.isStable() || coordinator.isRejoining()) && waitTime < maxWaitTime) {
            try {
                Thread.sleep(200); // 增加检查间隔
                waitTime += 200;
                if (waitTime % 2000 == 0) { // 每2秒输出一次进度
                    System.out.printf("[KafkaLiteConsumerImpl] 等待coordinator稳定中...%d/%dms, isStable=%s, isRejoining=%s\n", 
                        waitTime, maxWaitTime, coordinator.isStable(), coordinator.isRejoining());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("[KafkaLiteConsumerImpl] 等待coordinator稳定被中断");
                break;
            }
        }
        
        if (coordinator.isStable() && !coordinator.isRejoining()) {
            System.out.println("[KafkaLiteConsumerImpl] Coordinator已稳定");
        } else {
            System.err.printf("[KafkaLiteConsumerImpl] Coordinator等待超时: %dms, isStable=%s, isRejoining=%s\n", 
                maxWaitTime, coordinator.isStable(), coordinator.isRejoining());
        }
    }
    
    /**
     * 执行自动提交，单独记录延迟
     */
    private void performAutoCommit() {
        long startTime = System.currentTimeMillis();
        try {
            commitSync();
        } finally {
            long endTime = System.currentTimeMillis();
            long commitLatency = endTime - startTime;
            
            // 记录自动提交的延迟
            metricsCollector.incrementCounter("consumer.auto_commit");
            metricsCollector.recordLatency("consumer.auto_commit", commitLatency);
            
            System.out.printf("[AutoCommit] 自动提交延迟: %dms\n", commitLatency);
        }
    }
} 