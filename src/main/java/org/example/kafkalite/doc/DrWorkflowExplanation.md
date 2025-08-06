# Kafka DR集群切换完整流程说明

## 🎯 核心组件架构

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ DrAwareConsumer │────│ FailoverManager  │────│ ClusterConfig   │
│                 │    │                  │    │ (Primary)       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │              ┌─────────────────┐
         │              ┌──────────────────┐    │ ClusterConfig   │
         │              │ HealthChecker    │────│ (Secondary)     │
         │              └──────────────────┘    └─────────────────┘
         │                       │
┌─────────────────┐    ┌──────────────────┐
│ DynamicDns      │    │ KafkaConsumer    │
│ Resolver        │    │ Impl             │
└─────────────────┘    └──────────────────┘
```

## 📖 DR切换完整流程

### **第一阶段：初始化阶段**

#### 1.1 DNS解析器初始化
```java
// 位置: DrAwareKafkaConsumer 构造函数
this.dnsResolver = new DynamicDnsResolver(
    config.getDnsTtlMs(),           // DNS缓存TTL: 5分钟
    config.getDnsRefreshIntervalMs(), // 刷新间隔: 5分钟  
    config.getDnsMaxRetries(),      // 最大重试: 3次
    config.getDnsRetryDelayMs()     // 重试延迟: 1秒
);
```

**功能**：
- 🔄 定期刷新DNS缓存，解决官方客户端静态缓存问题
- 🎯 域名 `kafka-primary.example.com` → IP `192.168.1.100`
- 📊 缓存管理和过期清理

#### 1.2 故障转移管理器初始化
```java
// 位置: DrAwareKafkaConsumer 构造函数
this.failoverManager = new FailoverManager(
    config.getClusters(),           // 集群列表
    dnsResolver,                    // DNS解析器
    this,                          // 故障转移监听器
    config.getFailoverPolicy(),     // AUTO_WITH_FALLBACK
    config.getFailoverCooldownMs(), // 冷却时间: 5分钟
    config.getFailoverEvaluationDelayMs(), // 评估延迟: 10秒
    config.isEnableAutoFailback()   // 自动回切: true
);
```

**功能**：
- 🎛️ 选择初始活跃集群（优先级最高的PRIMARY集群）
- 📋 管理集群切换策略和冷却机制

#### 1.3 健康检查器初始化
```java
// 位置: DrAwareKafkaConsumer 构造函数  
this.healthChecker = new ClusterHealthChecker(
    config.getClusters(),           // 监控的集群列表
    dnsResolver,                   // DNS解析器
    failoverManager,               // 健康状态回调
    config.getClusterHealthCheckIntervalMs(), // 检查间隔: 30秒
    config.getClusterConnectionTimeoutMs(),   // 连接超时: 5秒
    config.getClusterSocketTimeoutMs()       // Socket超时: 10秒
);
```

**功能**：
- 🔍 独立线程池定期检查各集群连通性
- 🚨 检测集群故障并触发故障转移决策

### **第二阶段：正常运行阶段**

#### 2.1 DNS定期刷新
```java
// 位置: DynamicDnsResolver.refreshExpiredEntries()
private void refreshExpiredEntries() {
    for (Map.Entry<String, ResolvedAddress> entry : dnsCache.entrySet()) {
        String hostname = entry.getKey();
        ResolvedAddress address = entry.getValue();
        
        if (address.isExpired()) {
            // 在后台线程中重新解析DNS
            refreshExecutor.submit(() -> {
                performDnsResolution(hostname, false);
            });
        }
    }
}
```

**关键点**：
- ⏰ 每1分钟检查一次DNS缓存过期
- 🔄 后台异步刷新，不影响主业务
- 🎯 **解决痛点**：Kafka官方客户端无法更新DNS缓存

#### 2.2 集群健康检查
```java
// 位置: ClusterHealthChecker.performHealthCheck()
private void performHealthCheck(ClusterConfig cluster) {
    totalHealthChecks.incrementAndGet();
    long startTime = System.currentTimeMillis();
    
    try {
        boolean isHealthy = checkClusterConnectivity(cluster);
        
        if (isHealthy) {
            cluster.recordSuccess();
            if (listener != null) {
                listener.onClusterHealthChanged(cluster, true, "连接正常");
            }
        } else {
            cluster.recordFailure("连接失败");
            if (listener != null) {
                listener.onClusterHealthChanged(cluster, false, "连接失败");
                
                // 触发故障转移决策
                if (cluster.shouldFailover()) {
                    listener.onClusterUnreachable(cluster, "连接失败");
                }
            }
        }
    } catch (Exception e) {
        cluster.recordFailure("健康检查异常: " + e.getMessage());
    }
}
```

**检查逻辑**：
- 🔌 每30秒检查一次Socket连通性
- 📊 记录连续失败次数和成功率统计
- 🚨 连续失败达到阈值(3次)时标记为不可达

### **第三阶段：故障检测阶段**

#### 3.1 故障检测触发条件
```java
// 位置: ClusterConfig.shouldFailover()
public boolean shouldFailover() {
    return state == ClusterState.UNREACHABLE || 
           consecutiveFailures >= maxConsecutiveFailures;
}

// 位置: ClusterConfig.recordFailure()
public void recordFailure(String error) {
    this.consecutiveFailures++;
    this.lastError = error;
    
    // 连续失败达到阈值，标记为不可达
    if (consecutiveFailures >= maxConsecutiveFailures) {
        setState(ClusterState.UNREACHABLE);
    }
}
```

**触发条件**：
- ❌ 连续失败次数 ≥ 3次
- ❌ 集群状态 = UNREACHABLE
- ❌ Socket连接超时或异常

### **第四阶段：故障转移决策阶段**

#### 4.1 故障转移决策
```java
// 位置: FailoverManager.onClusterUnreachable()
@Override
public void onClusterUnreachable(ClusterConfig cluster, String reason) {
    // 只有当前活跃集群不可达时才触发故障转移
    if (cluster.equals(activeCluster.get()) && policy != FailoverPolicy.MANUAL_ONLY) {
        
        ClusterConfig nextCluster = selectNextAvailableCluster(cluster);
        if (nextCluster != null) {
            // 延迟评估，避免网络抖动误判
            new Thread(() -> {
                try {
                    Thread.sleep(evaluationDelayMs); // 等待10秒
                    // 再次确认集群确实不可达
                    if (cluster.shouldFailover() && cluster.equals(activeCluster.get())) {
                        performFailover(nextCluster, "自动故障转移: " + reason);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }
}
```

#### 4.2 选择目标集群
```java
// 位置: FailoverManager.selectNextAvailableCluster()
private ClusterConfig selectNextAvailableCluster(ClusterConfig excludeCluster) {
    return clusters.stream()
        .filter(c -> !c.equals(excludeCluster))           // 排除故障集群
        .filter(c -> c.getState() != ClusterState.UNREACHABLE) // 排除不可达集群
        .min(Comparator.comparingInt(ClusterConfig::getPriority)) // 选择优先级最高的
        .orElse(null);
}
```

**选择策略**：
- 🎯 优先级最高（数字最小）
- ✅ 状态健康（非UNREACHABLE）
- 🚫 排除当前故障的集群

### **第五阶段：故障转移执行阶段**

#### 5.1 故障转移执行
```java
// 位置: FailoverManager.performFailover()
private boolean performFailover(ClusterConfig targetCluster, String reason) {
    if (!failoverLock.tryLock()) {
        return false; // 防止并发故障转移
    }
    
    try {
        // 检查冷却时间
        if (isInCooldownPeriod()) {
            return false;
        }
        
        ClusterConfig currentCluster = activeCluster.get();
        failoverState.set(FailoverState.FAILING_OVER);
        
        // 1. 通知开始故障转移
        if (listener != null) {
            listener.onFailoverStarted(currentCluster, targetCluster, reason);
        }
        
        // 2. 刷新目标集群的DNS缓存
        refreshDnsForCluster(targetCluster);
        
        // 3. 执行切换前准备
        if (listener != null) {
            listener.beforeClusterSwitch(currentCluster, targetCluster);
        }
        
        // 4. 切换活跃集群
        if (currentCluster != null) {
            currentCluster.setState(ClusterState.INACTIVE);
        }
        targetCluster.setState(ClusterState.ACTIVE);
        activeCluster.set(targetCluster);
        
        // 5. 执行切换后清理
        if (listener != null) {
            listener.afterClusterSwitch(currentCluster, targetCluster);
        }
        
        failoverState.set(FailoverState.STABLE);
        return true;
        
    } finally {
        failoverLock.unlock();
    }
}
```

#### 5.2 DNS缓存刷新
```java
// 位置: FailoverManager.refreshDnsForCluster()
private void refreshDnsForCluster(ClusterConfig cluster) {
    for (String server : cluster.getBootstrapServers()) {
        String[] parts = server.split(":");
        String hostname = parts[0];
        // 强制刷新DNS，获取最新IP地址
        dnsResolver.forceRefresh(hostname);
    }
    Thread.sleep(1000); // 等待DNS解析完成
}
```

**关键作用**：
- 🔄 强制重新解析目标集群的域名
- 🎯 获取最新的IP地址（DR切换后IP可能变化）
- ⏱️ 等待解析完成再进行后续操作

### **第六阶段：Consumer切换阶段**

#### 6.1 切换前处理
```java
// 位置: DrAwareKafkaConsumer.beforeClusterSwitch()
@Override
public void beforeClusterSwitch(ClusterConfig fromCluster, ClusterConfig toCluster) {
    // 提交当前Consumer的offset，防止数据丢失
    try {
        KafkaLiteConsumerImpl currentConsumer = activeConsumer.get();
        if (currentConsumer != null) {
            currentConsumer.commitSync();
            System.out.printf("切换前offset提交成功\n");
        }
    } catch (Exception e) {
        System.err.printf("切换前offset提交失败: %s\n", e.getMessage());
    }
}
```

#### 6.2 切换后处理
```java
// 位置: DrAwareKafkaConsumer.afterClusterSwitch()
@Override
public void afterClusterSwitch(ClusterConfig fromCluster, ClusterConfig toCluster) {
    try {
        // 1. 关闭旧的Consumer实例
        KafkaLiteConsumerImpl oldConsumer = activeConsumer.get();
        if (oldConsumer != null) {
            oldConsumer.close();
        }
        
        // 2. 为新集群创建Consumer实例
        if (subscribedTopics != null) {
            createConsumerForCluster(toCluster, subscribedTopics);
        }
        
    } catch (Exception e) {
        throw new RuntimeException("集群切换失败", e);
    }
}
```

#### 6.3 创建新Consumer
```java
// 位置: DrAwareKafkaConsumer.createConsumerForCluster()
private void createConsumerForCluster(ClusterConfig cluster, List<String> topics) {
    List<String> bootstrapServers = cluster.getBootstrapServers();
    
    // 使用DNS解析器获取最新的IP地址
    if (dnsResolver != null) {
        bootstrapServers = dnsResolver.resolveBootstrapServers(bootstrapServers);
    }
    
    // 创建新的Consumer实例
    ConsumerConfig baseConfig = createBaseConfig();
    KafkaLiteConsumerImpl newConsumer = new KafkaLiteConsumerImpl(
        groupId, bootstrapServers, baseConfig);
    
    // 订阅topics
    newConsumer.subscribe(topics);
    
    // 更新引用
    activeConsumer.set(newConsumer);
    activeCluster.set(cluster);
}
```

### **第七阶段：自动回切阶段（可选）**

#### 7.1 集群恢复检测
```java
// 位置: FailoverManager.onClusterRecovered()
@Override
public void onClusterRecovered(ClusterConfig cluster) {
    // 如果是优先级更高的集群恢复，考虑自动failback
    if (enableAutoFailback && cluster.getPriority() < activeCluster.get().getPriority()) {
        
        // 延迟执行failback，确保集群真正稳定
        new Thread(() -> {
            try {
                Thread.sleep(evaluationDelayMs * 2); // 更长的等待时间
                if (cluster.isHealthy() && cluster.getPriority() < activeCluster.get().getPriority()) {
                    performFailover(cluster, "自动failback: 高优先级集群恢复");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
}
```

## 🔧 **测试步骤**

### 最简单的测试方法：

1. **运行SimpleDrTest**：
```bash
java -cp target/classes org.example.kafkalite.client.SimpleDrTest
```

2. **观察输出**：
```
=== 简化DR测试开始 ===
[DynamicDnsResolver] 初始化完成: TTL=300000ms, 刷新间隔=60000ms
[FailoverManager] 初始化完成: 策略=AUTO_WITH_FALLBACK, 活跃集群=主集群
[ClusterHealthChecker] 启动集群健康检查
活跃集群: 主集群 (primary)
```

3. **手动故障转移测试**：
```
尝试切换到: secondary
[FailoverManager] 开始执行故障转移: 主集群 -> 备用集群
[DrAwareKafkaConsumer] 切换前offset提交成功
[DrAwareKafkaConsumer] 新集群Consumer创建成功: 备用集群
故障转移结果: 成功
```

**不需要真实的多集群**，使用不同的broker地址就能模拟集群切换！

## 🎯 **核心价值**

1. **DNS问题解决** ✅：动态刷新，支持域名切换
2. **自动故障检测** ✅：30秒检测，3次失败触发
3. **智能切换决策** ✅：优先级+健康状态+冷却机制
4. **无缝Consumer切换** ✅：offset保护+自动重建
5. **可观测性** ✅：详细日志+统计信息

这就是完整的DR切换流程！🚀 

启动阶段：
┌─────────────────┐
│ start()         │
├─────────────────┤
│ 为每个集群创建   │
│ 独立定时任务     │ → 每30秒执行一次
└─────────────────┘

健康检查循环：
┌─────────────────┐
│ performHealthCheck() │
├─────────────────┤
│ 1. 统计计数      │
│ 2. DNS解析      │ → dnsResolver.resolveBootstrapServers()
│ 3. 连接测试      │ → testBrokerConnection()
│ 4. 状态更新      │ → cluster.recordSuccess/Failure()
│ 5. 通知监听器    │ → listener.onClusterHealthChanged()
└─────────────────┘
│
▼
集群状态判断
│
┌────▼────┐    ┌─────────────┐
│ 健康    │    │ 不健康      │
└────┬────┘    └─────┬───────┘
│               │
▼               ▼
🟢 onClusterRecovered  ❌ shouldFailover?
(如果之前不健康)            │
▼
🚨 onClusterUnreachable
(触发故障转移！)