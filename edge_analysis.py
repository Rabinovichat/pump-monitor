"""
信号 edge 分析: 不预设止盈止损, 直接测信号出现后的"未来收益分布".

对每个候选信号, 拉 48h 的 15m K 线, 计算:
  - 各时间点的收益: +1h / +4h / +12h / +24h / +48h (相对入场价)
  - MFE (最大有利涨幅, 做多视角能吃到的最大上涨)
  - MAE (最大不利跌幅, 做多视角最大回撤)

汇总: 均值/中位数/正收益占比. 用来判断:
  - 若各时点收益系统性为负 → 应做空而非做多
  - 若 MFE 远大于 |MAE| → 值得放宽止盈
  - 若收益围绕 0 对称 → 无方向性 edge, 这个信号不能拿来交易

用法:
    python edge_analysis.py --cohort r3_solo
    python edge_analysis.py --cohort r1_r3
"""
import argparse
import asyncio
import statistics
import sys
from collections import defaultdict
from datetime import timedelta

from backtest import parse_log_file, find_candidates, fetch_klines, INTERVAL_MIN

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

HORIZONS_H = [1, 4, 12, 24, 48]
MAX_HOLD_H = 48


def build_cohort(records, cohort):
    """按 cohort 类型筛选候选信号."""
    if cohort == "r3_solo":
        # 复用 Strategy 1 的筛选 (R3 + 6h无R1/R5 + 4h冷却)
        return find_candidates(records, quiet_hours=6, cooldown_hours=4)

    # 其它 cohort: 简单按 current_round_hits 组合筛, 带4h同币冷却
    def simple(pred):
        last = {}
        out = []
        for r in records:
            if not pred(r.get("current_round_hits", [])):
                continue
            sym = r["symbol"]
            if sym in last and (r["ts_dt"] - last[sym]).total_seconds() < 4 * 3600:
                continue
            out.append(r)
            last[sym] = r["ts_dt"]
        return out

    if cohort == "r1_r3":
        return simple(lambda h: "R1" in h and "R3" in h)
    if cohort == "r1_solo":
        return simple(lambda h: h == ["R1"])
    if cohort == "r4":
        return simple(lambda h: "R4" in h)
    if cohort == "r2_r3":
        return simple(lambda h: "R2" in h and "R3" in h)
    raise ValueError(f"unknown cohort {cohort}")


def pct(vals, p):
    if not vals:
        return 0.0
    s = sorted(vals)
    k = int(len(s) * p)
    return s[min(k, len(s) - 1)]


async def analyze(cohort, log_file):
    records = parse_log_file(log_file)
    print(f"日志记录: {len(records):,}")
    cands = [c for c in build_cohort(records, cohort) if c.get("price_now")]
    print(f"Cohort '{cohort}' 候选(有入场价): {len(cands):,}")
    if not cands:
        return

    import httpx
    sem = asyncio.Semaphore(4)
    done = 0

    async def fetch_one(c, http):
        nonlocal done
        start = c["ts_dt"]
        end = start + timedelta(hours=MAX_HOLD_H)
        async with sem:
            kl = await fetch_klines(c["symbol"], start, end, http)
        done += 1
        if done % 50 == 0 or done == len(cands):
            print(f"  拉取 {done}/{len(cands)}", flush=True)
        return c, kl

    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
        fetched = await asyncio.gather(*[fetch_one(c, http) for c in cands])

    # 计算每个信号的未来收益 / MFE / MAE
    rows = []
    fail = 0
    for c, kl in fetched:
        if not kl or len(kl) < 2:
            fail += 1
            continue
        entry = c["price_now"]
        closes = [float(k[4]) for k in kl]
        highs = [float(k[2]) for k in kl]
        lows = [float(k[3]) for k in kl]
        row = {"symbol": c["symbol"], "ts": c["ts_dt"]}
        # 各时间点收益
        for h in HORIZONS_H:
            idx = min(int(h * 60 / INTERVAL_MIN), len(closes) - 1)
            row[f"r{h}h"] = (closes[idx] - entry) / entry
        # MFE / MAE (整段48h)
        row["mfe"] = (max(highs) - entry) / entry
        row["mae"] = (min(lows) - entry) / entry
        rows.append(row)

    print(f"成功 {len(rows)} / 失败 {fail} (失败多为币安无此合约)")
    if not rows:
        return

    print()
    print("=" * 70)
    print(f"📈 Edge 分析: cohort={cohort}, N={len(rows)}")
    print("=" * 70)
    print(f"{'时间点':>8} | {'均值':>9} | {'中位数':>9} | {'正收益%':>8} | {'p25':>8} | {'p75':>8}")
    print("-" * 70)
    for h in HORIZONS_H:
        vals = [r[f"r{h}h"] for r in rows]
        posp = sum(1 for v in vals if v > 0) / len(vals)
        print(f"{h:>6}h | {statistics.mean(vals):>+8.2%} | {statistics.median(vals):>+8.2%} "
              f"| {posp:>7.1%} | {pct(vals,0.25):>+7.2%} | {pct(vals,0.75):>+7.2%}")

    mfes = [r["mfe"] for r in rows]
    maes = [r["mae"] for r in rows]
    print("-" * 70)
    print(f"MFE 最大有利涨幅  均值 {statistics.mean(mfes):+.2%}  中位数 {statistics.median(mfes):+.2%}")
    print(f"MAE 最大不利跌幅  均值 {statistics.mean(maes):+.2%}  中位数 {statistics.median(maes):+.2%}")
    print()
    # 方向性结论
    r48 = [r["r48h"] for r in rows]
    m = statistics.mean(r48)
    print(f"→ 48h 平均收益 {m:+.2%}. ", end="")
    if m < -0.005:
        print("系统性下跌 → 若有 edge, 方向应是【做空】")
    elif m > 0.005:
        print("系统性上涨 → 方向应是【做多】")
    else:
        print("围绕0, 无明显方向性 edge")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", default="r3_solo",
                    choices=["r3_solo", "r1_r3", "r1_solo", "r4", "r2_r3"])
    ap.add_argument("--log-file", default="logs/monitor_30d.log")
    args = ap.parse_args()
    asyncio.run(analyze(args.cohort, args.log_file))


if __name__ == "__main__":
    main()
