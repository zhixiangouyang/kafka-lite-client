#!/bin/bash

# 统一版Kafka生产者性能监控启动脚本
echo "🚀 启动统一版Kafka生产者性能监控..."
echo "========================================"

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

# 获取当前目录的绝对路径
CURRENT_DIR=$(pwd)
HTML_FILE="$CURRENT_DIR/simple-monitor.html"

echo ""
echo "========================================"
echo "🎯 统一版监控系统已准备就绪！"
echo "========================================"
echo "📊 监控页面: file://$HTML_FILE"
echo ""
echo "🚀 推荐使用方式:"
echo "1. 运行标准生产者测试 (推荐):"
echo "   java -cp target/classes org.example.kafkalite.client.KafkaProducerMonitorTest"
echo "   📡 监控端点: http://localhost:8083/metrics"
echo ""
echo "2. 运行性能测试:"
echo "   java -cp target/classes org.example.kafkalite.client.ProducerPerformanceMonitorTest"
echo "   📡 监控端点: http://localhost:8082/metrics"
echo ""
echo "3. 运行集成测试:"
echo "   java -cp target/classes org.example.kafkalite.client.MonitoringIntegrationTest"
echo "   📡 监控端点: http://localhost:8081/metrics"
echo ""
echo "📋 配置对比:"
echo "  • 标准测试: 批次大小=900, 端口=8083 ⭐"
echo "  • 性能测试: 批次大小=102, 端口=8082"
echo "  • 集成测试: 批次大小=100, 端口=8081"
echo ""
echo "💡 提示: HTML监控页面默认连接8083端口"
echo "========================================"

# 选择测试类型
echo ""
echo "🤔 请选择要启动的测试类型:"
echo "1) 标准监控测试 (KafkaProducerMonitorTest) - 推荐"
echo "2) 性能监控测试 (ProducerPerformanceMonitorTest)"  
echo "3) 集成监控测试 (MonitoringIntegrationTest)"
echo "4) 仅启动监控页面"
read -p "请输入选择 (1-4): " choice

case $choice in
    1)
        echo "🚀 启动标准监控测试..."
        # 尝试自动打开监控页面
        if command -v open &> /dev/null; then
            open "$HTML_FILE"
        fi
        echo "▶️ 运行中... (按Ctrl+C停止)"
        java -cp target/classes org.example.kafkalite.client.KafkaProducerMonitorTest
        ;;
    2)
        echo "🚀 启动性能监控测试..."
        echo "⚠️ 注意：此测试使用较小的批次大小(102)，可能影响性能"
        # 更新HTML页面端口到8082
        sed -i.bak 's/localhost:8083/localhost:8082/g' "$HTML_FILE" 2>/dev/null || \
        sed -i '' 's/localhost:8083/localhost:8082/g' "$HTML_FILE" 2>/dev/null
        if command -v open &> /dev/null; then
            open "$HTML_FILE"
        fi
        echo "▶️ 运行中... (按Ctrl+C停止)"
        java -cp target/classes org.example.kafkalite.client.ProducerPerformanceMonitorTest
        # 恢复HTML页面端口
        sed -i.bak 's/localhost:8082/localhost:8083/g' "$HTML_FILE" 2>/dev/null || \
        sed -i '' 's/localhost:8082/localhost:8083/g' "$HTML_FILE" 2>/dev/null
        ;;
    3)
        echo "🚀 启动集成监控测试..."
        echo "⚠️ 注意：此测试使用最小的批次大小(100)，适合功能测试"
        # 更新HTML页面端口到8081
        sed -i.bak 's/localhost:8083/localhost:8081/g' "$HTML_FILE" 2>/dev/null || \
        sed -i '' 's/localhost:8083/localhost:8081/g' "$HTML_FILE" 2>/dev/null
        if command -v open &> /dev/null; then
            open "$HTML_FILE"
        fi
        echo "▶️ 运行中... (按Ctrl+C停止)"
        java -cp target/classes org.example.kafkalite.client.MonitoringIntegrationTest
        # 恢复HTML页面端口
        sed -i.bak 's/localhost:8081/localhost:8083/g' "$HTML_FILE" 2>/dev/null || \
        sed -i '' 's/localhost:8081/localhost:8083/g' "$HTML_FILE" 2>/dev/null
        ;;
    4)
        echo "🌐 仅启动监控页面..."
        if command -v open &> /dev/null; then
            open "$HTML_FILE"
        fi
        echo "📋 监控页面已打开，请手动运行测试类"
        ;;
    *)
        echo "❌ 无效选择，默认启动标准监控测试"
        if command -v open &> /dev/null; then
            open "$HTML_FILE"
        fi
        echo "▶️ 运行中... (按Ctrl+C停止)"
        java -cp target/classes org.example.kafkalite.client.KafkaProducerMonitorTest
        ;;
esac

echo ""
echo "🎉 监控系统已退出" 