"""
最优持仓时长回测: 48h 平仓是不是最好? 还是更短/更长更优?

两个关键点(否则结论会错):
1. 资金费随持仓时间累积. 做空负费率币是【付】资金费的, 持越久付越多.
   所以必须按实际结算时点累加, 不能只看价格.
2. 真正该优化的是【单位时间收益】, 不是单笔收益. 24h 赚 1.5% 优于 96h 赚 2%,
   因为同样的资金能周转 4 倍次数. 表里给出 %/天 和非复利年化.

口径: 同一批信号横向对比, 只变出场时点 (入场固定为报警价), 保证 apples-to-apples.
      样本要求持有到最长时长的 K 线都齐全, 避免不同时长样本数不同造成偏差.

做空净收益 = (entry-exit)/entry + sum(窗口内fundingRate) - 手续费
其中 sum(fundingRate) 对空头: 费率为负 → 空头付钱 → 该项为负.

用法:
    python hold_period.py --cohort r1_r3 --cooldown 48
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

TAKER_FEE = 0.0004
HOLDS_H = [6, 12, 24, 36, 48, 72, 96, 120]
BUFFER_H = 3


async def fetch_funding_ts(symbol, start_dt, end_dt, http):
    """拉历史资金费, 返回 [(fundingTime_ms, rate), ...] —— 需要时间戳才能按持仓窗口累加."""
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {
        "symbol": f"{symbol}USDT",
        "startTime": int(start_dt.timestamp() * 1000),
        "endTime": int(end_dt.timestamp() * 1000),
        "limit": 1000,
    }
    for attempt in range(4):
        try:
            r = await http.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return [(int(x["fundingTime"]), float(x["fundingRate"])) for x in r.json()]
            if r.status_code in (418, 429):
                await asyncio.sleep(min(int(r.headers.get("Retry-After", 2 ** attempt * 3)), 30))
                continue
            return None
        except Exception:
            await asyncio.sleep(1 + attempt)
    return None


def bar_at_or_after(klines, ts_ms):
    for i, k in enumerate(klines):
        if int(k[6]) >= ts_ms:
            return i
    return None


async def run(cohort, log_file, cooldown, period, side, max_hold, holds=None):
    holds = [h for h in (holds or HOLDS_H) if h <= max_hold]
    records = slice_period(parse_log_file(log_file), period)
    cands = [c for c in build_cohort(records, cohort, cooldown_hours=cooldown)
             if c.get("price_now")]
    print(f"Cohort '{cohort}' 候选: {len(cands)}  [方向={side}, 冷却={cooldown}h, 时段={period}]")
    print(f"测试持仓时长: {holds} 小时\n")

    sem = asyncio.Semaphore(4)
    done = 0

    async def one(c, http):
        nonlocal done
        start = c["ts_dt"]
        end = start + timedelta(hours=max_hold + BUFFER_H)
        async with sem:
            kl = await fetch_klines(c["symbol"], start, end, http)
            fr = await fetch_funding_ts(c["symbol"], start, end, http)
        done += 1
        if done % 25 == 0 or done == len(cands):
            print(f"  拉取 {done}/{len(cands)}", flush=True)
        return c, kl, fr

    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
        fetched = await asyncio.gather(*[one(c, http) for c in cands])

    # 只保留"持到最长时长数据齐全"的样本, 保证各时长同一批样本
    # 分类统计剔除原因: 否则一旦被 WAF 限流(资金费全拿不到), 只会看到"有效样本 0",
    # 会被误读成"数据不够", 实际是拉取失败 —— 两者的处理方式完全不同.
    valid = []
    no_kl = no_fr = short_kl = 0
    for c, kl, fr in fetched:
        if not kl:
            no_kl += 1
            continue
        if fr is None:
            no_fr += 1
            continue
        sig_ms = int(c["ts_dt"].timestamp() * 1000)
        if bar_at_or_after(kl, sig_ms + max_hold * 3600_000) is None:
            short_kl += 1         # 数据不够持到 max_hold (信号太靠近日志末尾)
            continue
        valid.append((c, kl, fr))
    print(f"有效样本: {len(valid)}  (剔除: K线拉取失败 {no_kl}, "
          f"资金费拉取失败 {no_fr}, 不够持到{max_hold}h {short_kl})\n")
    if no_fr:
        print(f"  ⚠ 有 {no_fr} 条资金费拉取失败(可能被限流 403/429), 结果不可信, 建议等几分钟重跑\n")
    if not valid:
        return

    print("=" * 92)
    print(f"最优持仓时长 — cohort={cohort}, 方向={side}, N={len(valid)} (同一批样本, 只变出场时点)")
    print("=" * 92)
    print(f"{'持仓':>6}{'价格收益':>11}{'资金费':>10}{'净收益均值':>12}"
          f"{'净中位':>10}{'胜率':>7}{'结算次':>7}{'%/天':>9}{'年化(非复利)':>13}")
    print("-" * 92)

    rows = []
    for H in holds:
        nets, prices, fundings, nsettle = [], [], [], []
        for c, kl, fr in valid:
            sig_ms = int(c["ts_dt"].timestamp() * 1000)
            entry = c["price_now"]
            xi = bar_at_or_after(kl, sig_ms + H * 3600_000)
            if xi is None:
                continue
            exit_p = float(kl[xi][4])
            exit_ms = int(kl[xi][6])
            pr = (exit_p - entry) / entry
            if side == "short":
                pr = -pr
            # 持仓窗口内的资金费结算. 空头 P&L = +sum(rate) (费率为负→付钱)
            in_win = [rate for t, rate in fr if sig_ms < t <= exit_ms]
            f_pnl = sum(in_win) if side == "short" else -sum(in_win)
            prices.append(pr)
            fundings.append(f_pnl)
            nsettle.append(len(in_win))
            nets.append(pr + f_pnl - 2 * TAKER_FEE)
        if not nets:
            continue
        m = statistics.mean(nets)
        per_day = m / (H / 24)
        apr = m * (365 * 24 / H)
        rows.append((H, m, per_day, apr, statistics.median(nets)))
        print(f"{H:>5}h{statistics.mean(prices):>+10.2%}{statistics.mean(fundings):>+10.2%}"
              f"{m:>+12.2%}{statistics.median(nets):>+10.2%}"
              f"{sum(1 for x in nets if x > 0) / len(nets):>7.0%}"
              f"{statistics.mean(nsettle):>7.1f}{per_day:>+9.2%}{apr:>+13.0%}")
    print("-" * 92)

    if rows:
        best_trade = max(rows, key=lambda r: r[1])
        best_rate = max(rows, key=lambda r: r[2])
        print(f"\n单笔收益最高 : {best_trade[0]}h  → 净 {best_trade[1]:+.2%}/笔")
        print(f"资金效率最高 : {best_rate[0]}h  → {best_rate[2]:+.2%}/天  (年化 {best_rate[3]:+.0%} 非复利)")
        if best_rate[0] != best_trade[0]:
            print("  ⇒ 两者不同: 持越久单笔赚越多, 但资金周转变慢. 按资金效率应选前者.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", default="r1_r3")
    ap.add_argument("--side", default="short", choices=["long", "short"])
    ap.add_argument("--cooldown", type=int, default=48)
    ap.add_argument("--period", default="all", choices=["all", "first", "second"])
    ap.add_argument("--max-hold", type=int, default=120)
    ap.add_argument("--holds", default="",
                    help="自定义持仓时长(小时), 逗号分隔, 如 '8,16,24,32'. 默认用 HOLDS_H")
    ap.add_argument("--log-file", default="logs/monitor_30d.log")
    args = ap.parse_args()
    holds = [int(x) for x in args.holds.split(",") if x.strip()] or None
    asyncio.run(run(args.cohort, args.log_file, args.cooldown, args.period,
                    args.side, args.max_hold, holds))


if __name__ == "__main__":
    main()
