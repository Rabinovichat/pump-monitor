"""
资金费收割回测: 做多 R3 极端负费率币, 净收益 = 价格变动 + 实际收取的资金费.

关键修正: 之前只算价格 (漏了做多方收取资金费). 负费率下做多是【收】资金费的.
这里拉币安历史资金费接口, 把持仓窗口内每次实际结算的费率加总, 得到真实收入.

做多净收益 = 价格收益(48h) + 累计资金费收入 - 手续费
其中 资金费收入 = -sum(fundingRate)  (费率为负时, 多头收取, 收入为正)

用法:
    python funding_backtest.py --cohort r3_solo
"""
import argparse
import asyncio
import statistics
import sys
from datetime import timedelta

import httpx

from backtest import parse_log_file, find_candidates, fetch_klines, INTERVAL_MIN

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

HOLD_H = 48
TAKER_FEE = 0.0004  # 单边 0.04%


def build_cohort(records, cohort, cooldown_hours=4):
    if cohort == "r3_solo":
        return find_candidates(records, quiet_hours=6, cooldown_hours=cooldown_hours)
    def simple(pred, cd=cooldown_hours):
        last = {}; out = []
        for r in records:
            if not pred(r.get("current_round_hits", [])):
                continue
            sym = r["symbol"]
            if sym in last and (r["ts_dt"] - last[sym]).total_seconds() < cd * 3600:
                continue
            out.append(r); last[sym] = r["ts_dt"]
        return out
    if cohort == "r1_r3":
        return simple(lambda h: "R1" in h and "R3" in h)
    if cohort == "r2_r3":
        return simple(lambda h: "R2" in h and "R3" in h)
    if cohort == "r4":
        return simple(lambda h: "R4" in h)

    # --- 融合口径: 用 recent_rules(2h窗口) + score, 更贴近现行规则 ---
    def simple_recent(pred, cd=cooldown_hours):
        last = {}; out = []
        for r in records:
            if not pred(set(r.get("recent_rules", [])), r.get("score", 0)):
                continue
            sym = r["symbol"]
            if sym in last and (r["ts_dt"] - last[sym]).total_seconds() < cd * 3600:
                continue
            out.append(r); last[sym] = r["ts_dt"]
        return out

    if cohort == "r1_r3_fused":
        return simple_recent(lambda h, s: {"R1", "R3"} <= h)
    if cohort == "r1_r2_r3_fused":
        return simple_recent(lambda h, s: {"R1", "R2", "R3"} <= h)
    if cohort == "score20":
        return simple_recent(lambda h, s: s >= 20)
    if cohort == "score25":
        return simple_recent(lambda h, s: s >= 25)
    raise ValueError(cohort)


async def fetch_funding(symbol, start_dt, end_dt, http):
    """拉历史资金费, 返回窗口内的 fundingRate 列表 (float)."""
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
                return [float(x["fundingRate"]) for x in r.json()]
            if r.status_code in (418, 429):
                await asyncio.sleep(min(int(r.headers.get("Retry-After", 2 ** attempt * 3)), 30))
                continue
            return None
        except Exception:
            await asyncio.sleep(1 + attempt)
    return None


async def run(cohort, log_file, cooldown=4):
    records = parse_log_file(log_file)
    cands = [c for c in build_cohort(records, cohort, cooldown_hours=cooldown) if c.get("price_now")]
    print(f"Cohort '{cohort}' 候选: {len(cands)}  [冷却={cooldown}h]")

    sem = asyncio.Semaphore(4)
    done = 0

    async def one(c, http):
        nonlocal done
        start = c["ts_dt"]; end = start + timedelta(hours=HOLD_H)
        async with sem:
            kl = await fetch_klines(c["symbol"], start, end, http)
            fr = await fetch_funding(c["symbol"], start, end, http)
        done += 1
        if done % 50 == 0 or done == len(cands):
            print(f"  {done}/{len(cands)}", flush=True)
        return c, kl, fr

    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
        fetched = await asyncio.gather(*[one(c, http) for c in cands])

    rows = []
    for c, kl, fr in fetched:
        if not kl or len(kl) < 2 or fr is None:
            continue
        entry = c["price_now"]
        idx = min(int(HOLD_H * 60 / INTERVAL_MIN), len(kl) - 1)
        price_ret = (float(kl[idx][4]) - entry) / entry
        funding_income = -sum(fr)                       # 多头收取
        n_settle = len(fr)
        long_net = price_ret + funding_income - 2 * TAKER_FEE
        rows.append({
            "symbol": c["symbol"], "price_ret": price_ret,
            "funding_income": funding_income, "n_settle": n_settle,
            "long_net": long_net, "entry_fr": c.get("bn_fr_cur"),
        })

    print(f"有效样本: {len(rows)}")
    if not rows:
        return

    def stats(key):
        v = [r[key] for r in rows]
        return statistics.mean(v), statistics.median(v)

    pr_m, pr_med = stats("price_ret")
    fi_m, fi_med = stats("funding_income")
    ln_m, ln_med = stats("long_net")
    win = sum(1 for r in rows if r["long_net"] > 0) / len(rows)
    avg_settle = statistics.mean(r["n_settle"] for r in rows)

    print("\n" + "=" * 66)
    print(f"💰 资金费收割做多回测 (持有{HOLD_H}h) — cohort={cohort}, N={len(rows)}")
    print("=" * 66)
    print(f"平均结算次数/{HOLD_H}h : {avg_settle:.1f}  (推断结算周期 ≈ {HOLD_H/avg_settle:.1f}h)")
    print(f"{'项目':<20}{'均值':>12}{'中位数':>12}")
    print("-" * 66)
    print(f"{'价格收益':<20}{pr_m:>+11.2%}{pr_med:>+12.2%}")
    print(f"{'资金费收入(多头收)':<18}{fi_m:>+11.2%}{fi_med:>+12.2%}")
    print(f"{'做多净收益':<20}{ln_m:>+11.2%}{ln_med:>+12.2%}")
    print("-" * 66)
    print(f"做多净胜率(净>0): {win:.1%}")

    # 按资金费收入分档: 极端费率是否真能覆盖价格下跌?
    print("\n按资金费收入分档 (看极端负费率币是否更赚):")
    buckets = [(0, 0.005), (0.005, 0.02), (0.02, 0.05), (0.05, 1.0)]
    labels = ["<0.5%", "0.5~2%", "2~5%", ">5%"]
    for (lo, hi), lab in zip(buckets, labels):
        sub = [r for r in rows if lo <= r["funding_income"] < hi]
        if not sub:
            print(f"  资金费收入 {lab:>8}: 0 样本")
            continue
        ln = [r["long_net"] for r in sub]
        w = sum(1 for x in ln if x > 0) / len(sub)
        print(f"  资金费收入 {lab:>8}: {len(sub):4d}样本 | 做多净收益 均值 {statistics.mean(ln):+.2%} 中位 {statistics.median(ln):+.2%} | 净胜率 {w:.0%}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", default="r3_solo",
                    choices=["r3_solo", "r1_r3", "r2_r3", "r4",
                             "r1_r3_fused", "r1_r2_r3_fused", "score20", "score25"])
    ap.add_argument("--cooldown", type=int, default=4, help="同币冷却小时数. 48=每个持仓窗口只算首次播报")
    ap.add_argument("--log-file", default="logs/monitor_30d.log")
    args = ap.parse_args()
    asyncio.run(run(args.cohort, args.log_file, args.cooldown))


if __name__ == "__main__":
    main()
