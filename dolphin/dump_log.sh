#!/bin/bash
# 把 pump-monitor 的日志打印到任务输出, 方便从 DolphinScheduler 复制下来用于本地回测.
#
# 在 DolphinScheduler 创建一个新的 Shell 任务, 把这段贴进去就行.
# 注意: 必须把任务的 Worker 分组也设成 worker_22 (跟监控任务一致).

LOG_DIR=/home/user_asset/pump-monitor/dolphin/logs

if [ ! -d "$LOG_DIR" ]; then
    echo "ERROR: log dir not found at $LOG_DIR"
    exit 1
fi

# 注意: 日志按天切分, monitor.log 只有今天; 历史在 monitor.YYYY-... 文件里.
# 所以要 cat 所有 monitor*.log, 不能只看 monitor.log.

echo "==================== FILE LIST (确认监控没停 + 文件时间跨度) ===================="
ls -la $LOG_DIR
echo ""

ALL_ALERTS=$(cat $LOG_DIR/monitor*.log 2>/dev/null | grep "log_alert_json")
ALERT_COUNT=$(echo "$ALL_ALERTS" | grep -c "log_alert_json")
FIRST_ALERT=$(echo "$ALL_ALERTS" | head -1 | grep -oP '"ts": "\K[^"]+' || echo "N/A")
LAST_ALERT=$(echo "$ALL_ALERTS" | tail -1 | grep -oP '"ts": "\K[^"]+' || echo "N/A")

echo "==================== LOG INFO ===================="
echo "Dir:         $LOG_DIR"
echo "Alert lines: $ALERT_COUNT"
echo "First alert: $FIRST_ALERT"
echo "Last alert:  $LAST_ALERT"
echo "==================================================="
echo ""
echo "==================== LOG_ALERTS_B64_BEGIN ===================="
# 全部告警 JSON 行 → gzip 压缩 → base64 单行, 规避 DolphinScheduler 输出截断.
# 本地用 decode_log.py 解码.
echo "$ALL_ALERTS" | gzip | base64 -w 0
echo ""
echo "==================== LOG_ALERTS_B64_END ===================="
