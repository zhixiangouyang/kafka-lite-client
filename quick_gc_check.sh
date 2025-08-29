#!/bin/bash

# 请修改这里的PID为您的实际进程ID
PID=35439

echo "🔍 快速GC模式检测 (PID: $PID)"
echo "采样20次，每次间隔1秒..."

# 检查进程是否存在
if ! ps -p $PID > /dev/null 2>&1; then
    echo "❌ 进程 $PID 不存在，请检查PID"
    echo "💡 使用以下命令查找您的Kafka客户端进程："
    echo "   ps aux | grep KafkaProducerMonitorTest"
    echo "   ps aux | grep kafkalite"
    exit 1
fi

# 清理之前的临时文件
rm -f /tmp/gc_pattern.log

echo "📊 开始采样..."
for i in {1..20}; do
    # 获取CPU和内存
    PROC_INFO=$(ps -p $PID -o pcpu,rss --no-headers 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "❌ 进程 $PID 已退出"
        exit 1
    fi
    
    CPU=$(echo $PROC_INFO | awk '{print $1}')
    RSS=$(echo $PROC_INFO | awk '{print $2}')
    
    echo "$i $(date +%s) $CPU $RSS" >> /tmp/gc_pattern.log
    printf "."
    sleep 1
done

echo ""
echo ""
echo "📈 分析GC模式:"
echo "时间点  CPU%   内存MB   分析"
echo "----   ----   ------   ----"

while read line; do
    INDEX=$(echo $line | awk '{print $1}')
    TIMESTAMP=$(echo $line | awk '{print $2}')
    CPU=$(echo $line | awk '{print $3}')
    RSS=$(echo $line | awk '{print $4}')
    RSS_MB=$((RSS / 1024))
    
    ANALYSIS=""
    CPU_INT=${CPU%.*}
    
    if [ "$CPU_INT" -gt 80 ]; then
        ANALYSIS="$ANALYSIS [高CPU]"
    fi
    
    if [ "$INDEX" -gt 1 ]; then
        PREV_LINE=$(sed -n "$((INDEX-1))p" /tmp/gc_pattern.log)
        PREV_RSS=$(echo $PREV_LINE | awk '{print $4}')
        RSS_DIFF=$((RSS - PREV_RSS))
        
        if [ "$RSS_DIFF" -lt -10000 ]; then
            ANALYSIS="$ANALYSIS [内存下降:可能GC]"
        elif [ "$RSS_DIFF" -gt 20000 ]; then
            ANALYSIS="$ANALYSIS [内存激增:对象积累]"
        fi
    fi
    
    printf "%2d     %s%%    %4dMB    %s\n" "$INDEX" "$CPU" "$RSS_MB" "$ANALYSIS"
    
done < /tmp/gc_pattern.log

echo ""
echo "🎯 快速诊断结果:"

# 计算统计信息
AVG_CPU=$(awk '{sum+=$3; count++} END {printf "%.1f", sum/count}' /tmp/gc_pattern.log)
MAX_CPU=$(awk 'BEGIN{max=0} {if($3>max) max=$3} END {print max}' /tmp/gc_pattern.log)
MIN_RSS=$(awk 'BEGIN{min=999999999} {if($4<min) min=$4} END {print min}' /tmp/gc_pattern.log)
MAX_RSS=$(awk 'BEGIN{max=0} {if($4>max) max=$4} END {print max}' /tmp/gc_pattern.log)
RSS_RANGE=$((MAX_RSS - MIN_RSS))

echo "• 平均CPU使用率: $AVG_CPU%"
echo "• 最高CPU使用率: $MAX_CPU%"
echo "• 内存使用范围: $((MIN_RSS/1024))MB - $((MAX_RSS/1024))MB (变化: $((RSS_RANGE/1024))MB)"

# 诊断建议
if (( $(echo "$MAX_CPU > 80" | bc -l 2>/dev/null || echo "0") )); then
    echo "🚨 检测到高CPU使用率，可能存在GC压力"
fi

if [ "$RSS_RANGE" -gt 100000 ]; then
    echo "⚠️ 内存变化较大，可能存在频繁的GC活动"
elif [ "$RSS_RANGE" -gt 50000 ]; then
    echo "💡 内存有一定波动，建议继续观察"
else
    echo "✅ 内存变化较小，GC压力不大"
fi

# 清理临时文件
rm -f /tmp/gc_pattern.log

echo ""
echo "💡 如需持续监控，请运行: ./gc_analysis.sh" 