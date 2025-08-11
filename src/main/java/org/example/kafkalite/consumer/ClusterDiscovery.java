package org.example.kafkalite.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 智能集群发现器
 * 基于单一域名自动发现多个集群并进行分组
 */
public class ClusterDiscovery {
    
    /**
     * 集群信息
     */
    public static class ClusterInfo {
        private final String clusterId;
        private final List<String> brokers;
        private final boolean isAvailable;
        
        public ClusterInfo(String clusterId, List<String> brokers, boolean isAvailable) {
            this.clusterId = clusterId;
            this.brokers = new ArrayList<>(brokers);
            this.isAvailable = isAvailable;
        }
        
        public String getClusterId() { return clusterId; }
        public List<String> getBrokers() { return new ArrayList<>(brokers); }
        public boolean isAvailable() { return isAvailable; }
        
        @Override
        public String toString() {
            return String.format("Cluster{id=%s, brokers=%s, available=%s}", 
                clusterId, brokers, isAvailable);
        }
    }
    
    /**
     * 发现结果
     */
    public static class DiscoveryResult {
        private final List<ClusterInfo> clusters;
        private final List<String> blacklistedIPs;
        
        public DiscoveryResult(List<ClusterInfo> clusters, List<String> blacklistedIPs) {
            this.clusters = new ArrayList<>(clusters);
            this.blacklistedIPs = new ArrayList<>(blacklistedIPs);
        }
        
        public List<ClusterInfo> getClusters() { return new ArrayList<>(clusters); }
        public List<String> getBlacklistedIPs() { return new ArrayList<>(blacklistedIPs); }
        
        public List<ClusterInfo> getAvailableClusters() {
            return clusters.stream()
                .filter(ClusterInfo::isAvailable)
                .collect(Collectors.toList());
        }
        
        public ClusterInfo getPrimaryCluster() {
            return getAvailableClusters().stream().findFirst().orElse(null);
        }
        
        public ClusterInfo getSecondaryCluster() {
            List<ClusterInfo> available = getAvailableClusters();
            return available.size() > 1 ? available.get(1) : null;
        }
    }
    
    /**
     * 基于单一域名发现所有集群
     * @param domainWithPort 域名:端口，如 "kafka.example.com:9092"
     * @param blacklistedIPs 黑名单IP列表
     * @return 发现结果
     */
    public static DiscoveryResult discoverClusters(String domainWithPort, List<String> blacklistedIPs) {
        String[] parts = domainWithPort.split(":");
        String domain = parts[0];
        String port = parts.length > 1 ? parts[1] : "9092";
        
        // 1. 解析域名获取所有IP
        List<String> allIPs = resolveAllIPs(domain);
        System.out.printf("[ClusterDiscovery] 域名 %s 解析到 %d 个IP: %s\n", domain, allIPs.size(), allIPs);
        
        // 2. 过滤黑名单IP
        List<String> availableIPs = allIPs.stream()
            .filter(ip -> !blacklistedIPs.contains(ip))
            .collect(Collectors.toList());
        
        System.out.printf("[ClusterDiscovery] 过滤黑名单后剩余 %d 个IP: %s\n", availableIPs.size(), availableIPs);
        
        // 3. 测试连通性并分组集群
        List<ClusterInfo> clusters = groupIntoClusters(availableIPs, port, blacklistedIPs);
        
        return new DiscoveryResult(clusters, blacklistedIPs);
    }
    
    /**
     * 解析域名获取所有IP地址
     */
    private static List<String> resolveAllIPs(String domain) {
        List<String> allIPs = new ArrayList<>();
        try {
            InetAddress[] addresses = InetAddress.getAllByName(domain);
            for (InetAddress address : addresses) {
                allIPs.add(address.getHostAddress());
            }
        } catch (UnknownHostException e) {
            System.err.printf("[ClusterDiscovery] DNS解析失败: %s, 错误: %s\n", domain, e.getMessage());
        }
        return allIPs;
    }
    
    /**
     * 将IP分组为集群
     * 基于网络段或连通性进行分组
     */
    private static List<ClusterInfo> groupIntoClusters(List<String> availableIPs, String port, List<String> blacklistedIPs) {
        List<ClusterInfo> clusters = new ArrayList<>();
        
        // 简单的网络段分组策略
        Map<String, List<String>> networkGroups = availableIPs.stream()
            .collect(Collectors.groupingBy(ClusterDiscovery::getNetworkSegment));
        
        int clusterIndex = 1;
        for (Map.Entry<String, List<String>> entry : networkGroups.entrySet()) {
            String segment = entry.getKey();
            List<String> ips = entry.getValue();
            
            // 构建broker地址列表
            List<String> brokers = ips.stream()
                .map(ip -> ip + ":" + port)
                .collect(Collectors.toList());
            
            // 检查集群可用性（这里简化为检查是否有IP被黑名单）
            boolean isAvailable = ips.stream().noneMatch(blacklistedIPs::contains);
            
            ClusterInfo cluster = new ClusterInfo(
                "cluster-" + clusterIndex + "-" + segment, 
                brokers, 
                isAvailable
            );
            
            clusters.add(cluster);
            clusterIndex++;
            
            System.out.printf("[ClusterDiscovery] 发现集群: %s\n", cluster);
        }
        
        return clusters;
    }
    
    /**
     * 获取IP的网络段（用于分组）
     * 例如：192.168.1.10 -> 192.168.1
     */
    private static String getNetworkSegment(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length >= 3) {
            return parts[0] + "." + parts[1] + "." + parts[2];
        }
        return ip;
    }
    
    /**
     * 创建增强的DR消费者装饰器
     */
    public static DrAwareConsumerDecorator createDrConsumer(String domainWithPort, String groupId, 
                                                          ConsumerConfig config, List<String> blacklistedIPs) {
        DiscoveryResult result = discoverClusters(domainWithPort, blacklistedIPs != null ? blacklistedIPs : new ArrayList<>());
        
        ClusterInfo primary = result.getPrimaryCluster();
        ClusterInfo secondary = result.getSecondaryCluster();
        
        if (primary == null) {
            throw new RuntimeException("无法发现可用的主集群");
        }
        
        List<String> primaryServers = primary.getBrokers();
        List<String> secondaryServers = secondary != null ? secondary.getBrokers() : new ArrayList<>();
        
        System.out.printf("[ClusterDiscovery] 创建DR消费者: 主集群=%s, 备集群=%s\n", primaryServers, secondaryServers);
        
        return new DrAwareConsumerDecorator(primaryServers, secondaryServers, groupId, config);
    }
} 