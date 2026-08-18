"""
止盈止损网格搜索: 对某 cohort 的做多信号, 拉 48h 的 15m K 线,
然后在内存里把一堆 (止损%, 止盈%) 组合全部模拟一遍, 看有没有任何一组能盈利.

核心问题: 一个"未来收益系统性为负"的信号, 靠"涨X%止盈/跌Y%止损"能不能变盈利?
理论上: 止盈止损只是对收益分布做截断变换, 不改变漂移方向. 若 MAE(最大回撤) > MFE(最大涨幅),
截断反而更容易被下方止损扫掉. 这里用真实K线验证.

模拟规则 (保守): 逐根15m K线, 同一根内若同时触及止损/止盈, 认定止损先成交(最坏情况).
到 48h 未触发则按末根收盘价平仓. 单边手续费 0.04%, 来回 0.08%.

用法:
    python grid_sltp.py --cohort r1_r3
    python grid_sltp.py --cohort r3_solo
    python grid_sltp.py --cohort r4
"""
import argparse
import asyncio
import statistics
import sys
from datetime import timedelta

import httpx

from backtest import parse_log_file, fetch_klines, INTERVAL_MIN
from funding_backtest import build_cohort

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

HOLD_H = 48
TAKER_FEE = 0.0004  # 单边

# 网格: 做多时 STOPS 为负(跌到止损); 做空时按幅度取正(涨到止损)
STOPS = [-0.02, -0.03, -0.05, -0.08, -0.10]
TAKES = [0.03, 0.05, 0.08, 0.10, 0.15, 0.20]


def sim_one(klines, entry, sl_pct, tp_pct, side="long"):
    """返回该组合下这笔交易的价格收益% (未扣手续费).
    long : sl_pct<0(跌到止损), tp_pct>0(涨到止盈).
    short: sl_pct>0(涨到止损, 收益=-sl_pct), tp_pct>0(跌到止盈, 收益=+tp_pct).
    """
    last = entry
    if side == "long":
        sl = entry * (1 + sl_pct); tp = entry * (1 + tp_pct)
        for k in klines:
            high, low, close = float(k[2]), float(k[3]), float(k[4])
            last = close
            if low <= sl:              # 保守: 先判止损
                return sl_pct
            if high >= tp:
                return tp_pct
        return (last - entry) / entry
    else:  # short
        sl = entry * (1 + sl_pct)      # 价格上涨到此 → 止损
        tp = entry * (1 - tp_pct)      # 价格下跌到此 → 止盈
        for k in klines:
            high, low, close = float(k[2]), float(k[3]), float(k[4])
            last = close
            if high >= sl:             # 保守: 做空先判止损(价格上冲)
                return -sl_pct
            if low <= tp:
                return tp_pct
        return (entry - last) / entry  # 超时平仓(做空收益)


async def run(cohort, log_file, side="long", cooldown=4):
    records = parse_log_file(log_file)
    cands = [c for c in build_cohort(records, cohort, cooldown_hours=cooldown) if c.get("price_now")]
    print(f"Cohort '{cohort}' 候选(有入场价): {len(cands)}  [方向={side}, 冷却={cooldown}h]")

    sem = asyncio.Semaphore(4)
    done = 0

    async def one(c, http):
        nonlocal done
        start = c["ts_dt"]; end = start + timedelta(hours=HOLD_H)
        async with sem:
            kl = await fetch_klines(c["symbol"], start, end, http)
        done += 1
        if done % 50 == 0 or done == len(cands):
            print(f"  拉取 {done}/{len(cands)}", flush=True)
        return c, kl

    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
        fetched = await asyncio.gather(*[one(c, http) for c in cands])

    valid = [(c, kl) for c, kl in fetched if kl and len(kl) >= 2]
    print(f"有效样本: {len(valid)}")
    if not valid:
        return

    # 基准: 纯持有48h, 不设止盈止损
    base = []
    for c, kl in valid:
        e = c["price_now"]
        idx = min(int(HOLD_H * 60 / INTERVAL_MIN), len(kl) - 1)
        ret = (float(kl[idx][4]) - e) / e
        if side == "short":
            ret = -ret
        base.append(ret - 2 * TAKER_FEE)
    print(f"\n基准(裸{'空' if side=='short' else '多'}持48h, 无止盈止损): 均值净收益 {statistics.mean(base):+.2%} | "
          f"中位 {statistics.median(base):+.2%} | 胜率 {sum(1 for x in base if x>0)/len(base):.0%}")

    # 网格. 做空时把止损幅度取正(价格上涨触发)
    stops = STOPS if side == "long" else [abs(s) for s in STOPS]

    print("\n" + "=" * 78)
    print(f"止盈止损网格 — cohort={cohort}, 方向={side}, N={len(valid)}  (单元格=均值净收益% / 胜率%)")
    print("=" * 78)
    header = "止损\\止盈 |" + "".join(f"{tp:>+8.0%}" for tp in TAKES)
    print(header)
    print("-" * len(header))

    best = None
    for sl in stops:
        cells = []
        for tp in TAKES:
            nets = []
            for c, kl in valid:
                r = sim_one(kl, c["price_now"], sl, tp, side) - 2 * TAKER_FEE
                nets.append(r)
            m = statistics.mean(nets)
            w = sum(1 for x in nets if x > 0) / len(nets)
            cells.append((m, w))
            if best is None or m > best[0]:
                best = (m, w, sl, tp)
        sl_disp = sl if side == "long" else -sl
        row = f"{sl_disp:>+7.0%} |" + "".join(f"{m:>+5.1%}/{w:>2.0%}" for m, w in cells)
        print(row)

    print("-" * len(header))
    bm, bw, bsl, btp = best
    bsl_disp = bsl if side == "long" else -bsl
    print(f"\n最优组合: 止损 {bsl_disp:+.0%} / 止盈 {btp:+.0%}  →  均值净收益 {bm:+.2%} | 胜率 {bw:.0%}")
    if bm > 0:
        print("  ⇒ 存在盈利组合 (需再验证是否稳健/是否过拟合)")
    else:
        print("  ⇒ 所有组合均为负期望: 止盈止损无法把这个信号救成盈利")
    if side == "short":
        print("  注: 做空负费率币需向多头【支付】资金费, 上表未计. R1/R3 为负费率, 实际做空收益还要再减资金费.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", default="r1_r3",
                    choices=["r3_solo", "r1_r3", "r2_r3", "r4",
                             "r1_r3_fused", "r1_r2_r3_fused", "score20", "score25"])
    ap.add_argument("--side", default="long", choices=["long", "short"])
    ap.add_argument("--cooldown", type=int, default=4, help="同币冷却小时数(去重). 设为48=每个持仓窗口只算首次播报")
    ap.add_argument("--log-file", default="logs/monitor_30d.log")
    args = ap.parse_args()
    asyncio.run(run(args.cohort, args.log_file, args.side, args.cooldown))


if __name__ == "__main__":
    main()
