#!/bin/bash

# 请修改这里的PID为您的实际进程ID
PID=35439
LOG_FILE="/tmp/gc_analysis_$PID.log"

echo "🔍 开始GC行为分析 (PID: $PID)"
echo "监控文件: $LOG_FILE"
echo "========================================"

# 检查进程是否存在
if ! ps -p $PID > /dev/null 2>&1; then
    echo "❌ 进程 $PID 不存在，请检查PID"
    echo "💡 使用以下命令查找您的Kafka客户端进程："
    echo "   ps aux | grep KafkaProducerMonitorTest"
    echo "   ps aux | grep kafkalite"
    exit 1
fi

# 记录初始状态
LAST_CPU=0
LAST_RSS=0
LAST_TIME=$(date +%s)

echo "🚀 开始监控进程 $PID..."
echo "按 Ctrl+C 停止监控"
echo ""

while true; do
    CURRENT_TIME=$(date +%s)
    TIME_DIFF=$((CURRENT_TIME - LAST_TIME))
    
    # 获取进程信息
    PROC_INFO=$(ps -p $PID -o pid,pcpu,pmem,rss,vsz --no-headers 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "❌ 进程 $PID 已退出"
        break
    fi
    
    CPU=$(echo $PROC_INFO | awk '{print $2}')
    MEM_PERCENT=$(echo $PROC_INFO | awk '{print $3}')
    RSS=$(echo $PROC_INFO | awk '{print $4}')
    VSZ=$(echo $PROC_INFO | awk '{print $5}')
    
    # 计算变化量
    RSS_DIFF=$((RSS - LAST_RSS))
    CPU_INT=${CPU%.*}  # 去掉小数点
    
    # 记录到日志
    echo "$(date '+%H:%M:%S') CPU:${CPU}% RSS:${RSS}KB DIFF:${RSS_DIFF}KB VSZ:${VSZ}KB" >> $LOG_FILE
    
    # GC行为推断
    GC_SIGNAL=""
    
    # 判断1：CPU突然升高但RSS下降 = 可能的GC
    if [ "$CPU_INT" -gt 80 ] && [ "$RSS_DIFF" -lt -10000 ]; then
        GC_SIGNAL="🔄 [可能GC] CPU高+内存下降"
    fi
    
    # 判断2：CPU很高但RSS基本不变 = 可能的Full GC
    if [ "$CPU_INT" -gt 90 ] && [ "$RSS_DIFF" -gt -5000 ] && [ "$RSS_DIFF" -lt 5000 ]; then
        GC_SIGNAL="🚨 [可能Full GC] CPU极高+内存无变化"
    fi
    
    # 判断3：RSS突然大幅下降 = GC完成
    if [ "$RSS_DIFF" -lt -50000 ]; then
        GC_SIGNAL="✅ [GC完成] 内存大幅释放"
    fi
    
    # 判断4：RSS快速增长 = 对象积累
    if [ "$RSS_DIFF" -gt 50000 ]; then
        GC_SIGNAL="⚠️ [对象积累] 内存快速增长"
    fi
    
    # 输出实时信息
    printf "🔍 [%s] CPU:%s%% RSS:%sMB 变化:%+dMB %s\n" \
           "$(date '+%H:%M:%S')" "$CPU" "$((RSS/1024))" "$((RSS_DIFF/1024))" "$GC_SIGNAL"
    
    # 更新last值
    LAST_CPU=$CPU_INT
    LAST_RSS=$RSS
    LAST_TIME=$CURRENT_TIME
    
    sleep 2
done

echo ""
echo "📊 分析完成，日志保存在: $LOG_FILE"
echo "💡 您可以使用 'cat $LOG_FILE' 查看完整日志" 