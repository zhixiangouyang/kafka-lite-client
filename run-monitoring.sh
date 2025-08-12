#!/bin/bash

echo "🚀 启动 Kafka-Lite-Client 监控系统"
echo "=================================="

# 检查Java环境
if ! command -v java &> /dev/null; then
    echo "❌ Java未安装，请先安装Java"
    exit 1
fi

# 检查Docker环境
if ! command -v docker &> /dev/null; then
    echo "❌ Docker未安装，请先安装Docker"
    exit 1
fi

# 检查Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 编译项目
echo "📦 编译项目..."
mvn clean compile -q

if [ $? -ne 0 ]; then
    echo "❌ 项目编译失败"
    exit 1
fi

# 启动监控栈
echo "🐳 启动监控栈 (Prometheus + Grafana + AlertManager)..."
docker-compose up -d

if [ $? -ne 0 ]; then
    echo "❌ 监控栈启动失败"
    exit 1
fi

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 10

# 检查服务状态
echo "🔍 检查服务状态..."
PROMETHEUS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/targets)
GRAFANA_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000)

if [ "$PROMETHEUS_STATUS" = "200" ]; then
    echo "✅ Prometheus已启动: http://localhost:9090"
else
    echo "❌ Prometheus启动失败"
fi

if [ "$GRAFANA_STATUS" = "200" ]; then
    echo "✅ Grafana已启动: http://localhost:3000 (admin/kafka-lite-2024)"
else
    echo "❌ Grafana启动失败"
fi

echo "✅ AlertManager已启动: http://localhost:9093"

# 启动Kafka-Lite-Client
echo ""
echo "🎯 启动 Kafka-Lite-Client 监控测试..."
echo "指标端点: http://localhost:8080/metrics"
echo "健康检查: http://localhost:8080/health"
echo ""
echo "按 Ctrl+C 停止测试"
echo "=================================="

# 启动监控集成测试
java -cp target/classes org.example.kafkalite.client.MonitoringIntegrationTest 