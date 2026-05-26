#!/bin/bash
# 把 pump-monitor 的日志打印到任务输出, 方便从 DolphinScheduler 复制下来用于本地回测.
#
# 在 DolphinScheduler 创建一个新的 Shell 任务, 把这段贴进去就行.
# 注意: 必须把任务的 Worker 分组也设成 worker_22 (跟监控任务一致).

LOG_FILE=/home/user_asset/pump-monitor/dolphin/logs/monitor.log

if [ ! -f "$LOG_FILE" ]; then
    echo "ERROR: log file not found at $LOG_FILE"
    exit 1
fi

TOTAL_LINES=$(wc -l < $LOG_FILE)
TOTAL_SIZE=$(du -h $LOG_FILE | cut -f1)
ALERT_COUNT=$(grep -c "log_alert_json" $LOG_FILE)
FIRST_ALERT=$(grep "log_alert_json" $LOG_FILE | head -1 | grep -oP '"ts": "\K[^"]+' || echo "N/A")
LAST_ALERT=$(grep "log_alert_json" $LOG_FILE | tail -1 | grep -oP '"ts": "\K[^"]+' || echo "N/A")

echo "==================== LOG INFO ===================="
echo "Path:        $LOG_FILE"
echo "Size:        $TOTAL_SIZE"
echo "Total lines: $TOTAL_LINES"
echo "Alert lines: $ALERT_COUNT"
echo "First alert: $FIRST_ALERT"
echo "Last alert:  $LAST_ALERT"
echo "==================================================="
echo ""
echo "==================== LOG_ALERTS_BEGIN ===================="
# 只导出告警 JSON 行, 过滤掉 INFO / DEBUG, 减少输出量
grep "log_alert_json" $LOG_FILE
echo "==================== LOG_ALERTS_END ===================="
