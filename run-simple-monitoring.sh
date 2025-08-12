#!/bin/bash

# 简单版Kafka生产者性能监控启动脚本（无需Docker）
echo "🚀 启动简单版Kafka生产者性能监控..."
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
echo "🎯 简单版监控系统已准备就绪！"
echo "========================================"
echo "📊 监控页面: file://$HTML_FILE"
echo ""
echo "🚀 使用步骤:"
echo "1. 运行生产者测试:"
echo "   java -cp target/classes org.example.kafkalite.client.KafkaProducerMonitorTest"
echo ""
echo "2. 打开监控页面:"
echo "   在浏览器中打开: file://$HTML_FILE"
echo "   或者运行: open simple-monitor.html"
echo ""
echo "📋 监控功能:"
echo "   • 📈 实时QPS和吞吐量图表"
echo "   • 📊 6个关键性能指标卡片"
echo "   • ⏱️  每3秒自动刷新"
echo "   • 🎨 美观的渐变界面"
echo ""
echo "📡 数据来源: http://localhost:8083/metrics"
echo "========================================"

# 尝试自动打开监控页面
if command -v open &> /dev/null; then
    echo "🌐 正在打开监控页面..."
    open "$HTML_FILE"
elif command -v xdg-open &> /dev/null; then
    echo "🌐 正在打开监控页面..."
    xdg-open "$HTML_FILE"
else
    echo "💡 请手动在浏览器中打开: $HTML_FILE"
fi

echo ""
echo "🎉 准备完成！现在请运行生产者测试，然后查看监控页面" 