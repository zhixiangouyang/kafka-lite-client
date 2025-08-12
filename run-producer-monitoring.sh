#!/bin/bash

# Kafka生产者性能监控启动脚本
echo "🚀 启动Kafka生产者性能监控系统..."
echo "========================================"

# 检查Docker和Docker Compose
if ! command -v docker &> /dev/null; then
    echo "❌ Docker未安装，请先安装Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 检查Java
if ! command -v java &> /dev/null; then
    echo "❌ Java未安装，请先安装Java"
    exit 1
fi

# 编译项目
echo "🔨 编译项目..."
mvn clean compile -q
if [ $? -ne 0 ]; then
    echo "❌ 项目编译失败"
    exit 1
fi
echo "✅ 项目编译成功"

# 启动监控服务
echo "📊 启动Prometheus + Grafana监控服务..."
docker-compose up -d prometheus grafana
if [ $? -ne 0 ]; then
    echo "❌ 监控服务启动失败"
    exit 1
fi

# 等待服务启动
echo "⏳ 等待监控服务启动..."
sleep 10

# 检查服务状态
echo "🔍 检查服务状态..."
PROMETHEUS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy)
GRAFANA_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health)

if [ "$PROMETHEUS_STATUS" = "200" ]; then
    echo "✅ Prometheus服务正常 (http://localhost:9090)"
else
    echo "⚠️ Prometheus服务可能未就绪，状态码: $PROMETHEUS_STATUS"
fi

if [ "$GRAFANA_STATUS" = "200" ]; then
    echo "✅ Grafana服务正常 (http://localhost:3000)"
else
    echo "⚠️ Grafana服务可能未就绪，状态码: $GRAFANA_STATUS"
fi

echo ""
echo "========================================"
echo "🎯 监控服务已启动！"
echo "========================================"
echo "📊 Prometheus: http://localhost:9090"
echo "📈 Grafana:    http://localhost:3000"
echo "   用户名: admin"
echo "   密码: admin"
echo ""
echo "🚀 现在可以运行生产者性能测试:"
echo "   java -cp target/classes org.example.kafkalite.client.KafkaProducerMonitorTest"
echo ""
echo "📋 测试指标将在以下端点可见:"
echo "   • 原始指标: http://localhost:8083/metrics"
echo "   • Grafana Dashboard: http://localhost:3000/d/kafka-producer-perf"
echo ""
echo "⚠️  注意: 请确保您的生产者测试正在运行，否则Grafana中看不到数据"
echo ""
echo "🛑 停止监控服务: docker-compose down"
echo "========================================"

# 可选：自动打开浏览器
if command -v open &> /dev/null; then
    echo "🌐 正在打开Grafana..."
    sleep 3
    open http://localhost:3000
elif command -v xdg-open &> /dev/null; then
    echo "🌐 正在打开Grafana..."
    sleep 3
    xdg-open http://localhost:3000
fi 