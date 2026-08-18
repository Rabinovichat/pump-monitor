"""
入场迟到容忍度测试: 手操的核心问题 —— 报警后隔多久入场, 边际还在吗?

回测原本按报警瞬间价 (price_now) 入场, 那是机器人才做得到的.
人工看到 Slack 可能已经晚 30 分钟甚至几小时. 若边际 15 分钟就衰减完, 手操判死;
若 2-4 小时内还在, 手操可行.

做法: 对每个信号, 分别在 signal+delay 处按 15m K 线收盘价入场, 持 48h 后收盘平仓.
    delay = 0/15m/30m/1h/2h/4h/8h. 同一批信号横向对比, 隔离"入场时点"这一个变量.

注: 表中只扣手续费 (双边 0.08%), 未扣资金费. 做空负费率币资金费拖累 ≈ -0.8%/笔,
    对所有 delay 近似相同, 不影响横向比较 (看绝对值时手动减).

用法:
    python entry_delay.py --cohort r1_r3 --side short --cooldown 48
"""
import argparse
import asyncio
import statistics
import sys
from datetime import timedelta

import httpx

from backtest import parse_log_file, fetch_klines
from funding_backtest import build_cohort
from grid_sltp import slice_period

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

HOLD_H = 48
TAKER_FEE = 0.0004
DELAYS_MIN = [0, 15, 30, 60, 120, 240, 480]   # 入场延迟(分钟)
FETCH_H = HOLD_H + 10                          # 多拉 10h 覆盖最大延迟(8h)


def bar_at_or_after(klines, ts_ms):
    """返回第一根 closeTime >= ts_ms 的 K 线索引, 找不到返回 None."""
    for i, k in enumerate(klines):
        if int(k[6]) >= ts_ms:
            return i
    return None


def sim_delayed(klines, signal_ms, delay_min, side):
    """在 signal+delay 处入场, 持 HOLD_H 后平仓. 返回净收益(扣手续费), 失败返回 None."""
    ei = bar_at_or_after(klines, signal_ms + delay_min * 60_000)
    if ei is None:
        return None
    entry = float(klines[ei][4])
    if entry <= 0:
        return None
    xi = bar_at_or_after(klines, int(klines[ei][6]) + HOLD_H * 3600_000)
    if xi is None:
        xi = len(klines) - 1          # 数据不足则用最后一根
    if xi <= ei:
        return None
    exit_p = float(klines[xi][4])
    ret = (exit_p - entry) / entry
    if side == "short":
        ret = -ret
    return ret - 2 * TAKER_FEE


async def run(cohort, log_file, side, cooldown, period):
    records = slice_period(parse_log_file(log_file), period)
    cands = [c for c in build_cohort(records, cohort, cooldown_hours=cooldown)
             if c.get("price_now")]
    print(f"Cohort '{cohort}' 候选: {len(cands)}  [方向={side}, 冷却={cooldown}h, 时段={period}]")

    sem = asyncio.Semaphore(4)
    done = 0

    async def one(c, http):
        nonlocal done
        start = c["ts_dt"]
        end = start + timedelta(hours=FETCH_H)
        async with sem:
            kl = await fetch_klines(c["symbol"], start, end, http)
        done += 1
        if done % 50 == 0 or done == len(cands):
            print(f"  拉取 {done}/{len(cands)}", flush=True)
        return c, kl

    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
        fetched = await asyncio.gather(*[one(c, http) for c in cands])

    valid = [(c, kl) for c, kl in fetched if kl and len(kl) >= 5]
    print(f"有效样本: {len(valid)}\n")
    if not valid:
        return

    # 理想基准: 报警瞬间价入场 (机器人才做得到)
    ideal = []
    for c, kl in valid:
        sig_ms = int(c["ts_dt"].timestamp() * 1000)
        xi = bar_at_or_after(kl, sig_ms + HOLD_H * 3600_000) or (len(kl) - 1)
        r = (float(kl[xi][4]) - c["price_now"]) / c["price_now"]
        if side == "short":
            r = -r
        ideal.append(r - 2 * TAKER_FEE)

    print("=" * 72)
    print(f"入场迟到容忍度 — cohort={cohort}, 方向={side}, 持有{HOLD_H}h, N={len(valid)}")
    print("(只扣手续费; 做空还需再减资金费 ≈ -0.8%/笔)")
    print("=" * 72)
    print(f"{'入场时点':<16}{'均值净收益':>12}{'中位数':>12}{'胜率':>8}{'样本':>7}")
    print("-" * 72)
    print(f"{'报警瞬间价(理想)':<14}{statistics.mean(ideal):>+11.2%}"
          f"{statistics.median(ideal):>+12.2%}"
          f"{sum(1 for x in ideal if x > 0) / len(ideal):>8.0%}{len(ideal):>7}")

    for d in DELAYS_MIN:
        nets = [v for c, kl in valid
                if (v := sim_delayed(kl, int(c["ts_dt"].timestamp() * 1000), d, side)) is not None]
        if not nets:
            print(f"{'+' + str(d) + 'min':<16}{'无样本':>12}")
            continue
        lab = "当根K线收盘" if d == 0 else f"+{d}min" if d < 60 else f"+{d // 60}h"
        w = sum(1 for x in nets if x > 0) / len(nets)
        print(f"{lab:<16}{statistics.mean(nets):>+11.2%}"
              f"{statistics.median(nets):>+12.2%}{w:>8.0%}{len(nets):>7}")
    print("-" * 72)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", default="r1_r3")
    ap.add_argument("--side", default="short", choices=["long", "short"])
    ap.add_argument("--cooldown", type=int, default=48)
    ap.add_argument("--period", default="all", choices=["all", "first", "second"])
    ap.add_argument("--log-file", default="logs/monitor_30d.log")
    args = ap.parse_args()
    asyncio.run(run(args.cohort, args.log_file, args.side, args.cooldown, args.period))


if __name__ == "__main__":
    main()
