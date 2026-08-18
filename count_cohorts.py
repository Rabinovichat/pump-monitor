"""快速统计各 cohort 在 48h 去重口径下的信号量, 判断手操可行性 (不联网)."""
import sys

from backtest import parse_log_file
from funding_backtest import build_cohort

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

recs = parse_log_file("logs/monitor_30d.log")
span_days = (recs[-1]["ts_dt"] - recs[0]["ts_dt"]).total_seconds() / 86400
print(f"日志跨度 {span_days:.1f} 天, 原始告警 {len(recs)}\n")
print(f"{'cohort':<18}{'48h去重信号':>12}{'个/天':>8}{'平均并发仓':>12}")
print("-" * 52)
for ch in ["r1_r3_fused", "r1_r3", "r1_r2_r3_fused", "score20", "score25", "r4"]:
    try:
        c = [x for x in build_cohort(recs, ch, cooldown_hours=48) if x.get("price_now")]
    except Exception as e:
        print(f"{ch:<18} ERR {e}")
        continue
    per_day = len(c) / span_days
    print(f"{ch:<18}{len(c):>12}{per_day:>8.1f}{per_day * 2:>12.1f}")
