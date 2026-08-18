"""
Strategy 1 回测脚本 - R3 单独触发的轧空交易

策略:
- 入场: 某币 R3 触发 + 该币近 N 小时内无 R1/R5 触发 (说明价格还没启动)
- 止损: -3%
- 止盈: +5% 平 50%, +10% 平剩下 50%
- 最长持仓: 48h, 否则按当前价平仓

数据源:
- 信号: 本地 logs/monitor.log (loguru WARNING 级别的 JSON 行)
- 价格: Binance Futures /fapi/v1/klines (1分钟K线)

用法:
    python backtest.py
    python backtest.py --log-file dolphin/logs/monitor.log
    python backtest.py --quiet-hours 12 --max-hold 24
"""
import argparse
import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

# Windows 控制台 UTF-8 输出
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


# ============ Default Config (可以用 CLI 覆盖) ============
DEFAULTS = {
    "log_file": "logs/monitor.log",
    "quiet_hours": 6,            # R3 触发前 N 小时内不能有 R1/R5
    "cooldown_hours": 4,         # 同一个币重复触发的冷却期
    "stop_loss_pct": -0.03,      # -3%
    "tp1_pct": 0.05,             # +5%
    "tp2_pct": 0.10,             # +10%
    "max_hold_hours": 48,        # 超时强平
    "initial_capital": 10000,    # 初始资金 (USD, 仅用于复利曲线)
    "position_pct": 0.02,        # 单笔仓位 = 资金 × 2%
    "leverage": 3,               # 杠杆倍数
}


# ============ Log Parsing ============
def parse_log_file(path):
    """解析日志文件, 返回告警记录列表 (按时间排序)."""
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = re.search(r'\{.*\}\s*$', line)
            if not m:
                continue
            try:
                rec = json.loads(m.group())
                if "symbol" in rec and "ts" in rec:
                    rec["ts_dt"] = datetime.fromisoformat(rec["ts"])
                    records.append(rec)
            except (json.JSONDecodeError, ValueError):
                continue
    records.sort(key=lambda r: r["ts_dt"])
    return records


def find_candidates(records, quiet_hours, cooldown_hours):
    """找出符合 Strategy 1 入场条件的信号."""
    by_symbol = defaultdict(list)
    last_entry = {}
    candidates = []

    for rec in records:
        sym = rec["symbol"]
        current_hits = rec.get("current_round_hits", [])

        # 条件1: 本轮 R3 触发
        if "R3" not in current_hits:
            by_symbol[sym].append(rec)
            continue

        # 条件2: 过去 quiet_hours 内同币无 R1/R5
        cutoff = rec["ts_dt"] - timedelta(hours=quiet_hours)
        had_r1_r5 = any(
            ("R1" in p.get("current_round_hits", [])
             or "R5" in p.get("current_round_hits", []))
            for p in by_symbol[sym] if p["ts_dt"] >= cutoff
        )
        if had_r1_r5:
            by_symbol[sym].append(rec)
            continue

        # 条件3: 冷却期 (避免同一波信号重复入场)
        if sym in last_entry:
            elapsed = (rec["ts_dt"] - last_entry[sym]).total_seconds()
            if elapsed < cooldown_hours * 3600:
                by_symbol[sym].append(rec)
                continue

        candidates.append(rec)
        last_entry[sym] = rec["ts_dt"]
        by_symbol[sym].append(rec)

    return candidates


# ============ Price Fetching ============
INTERVAL = "15m"          # 15分钟K线: 权重是1m的1/10, 一次请求覆盖48h, 不会被限流
INTERVAL_MIN = 15


async def fetch_klines(symbol, start_dt, end_dt, http):
    """从 Binance Futures 拉 K 线 (默认15m, 带 429/418 退避)."""
    url = "https://fapi.binance.com/fapi/v1/klines"
    span_min = (end_dt - start_dt).total_seconds() / 60
    limit = min(1500, int(span_min / INTERVAL_MIN) + 5)
    params = {
        "symbol": f"{symbol}USDT",
        "interval": INTERVAL,
        "startTime": int(start_dt.timestamp() * 1000),
        "endTime": int(end_dt.timestamp() * 1000),
        "limit": limit,
    }
    for attempt in range(4):
        try:
            r = await http.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):
                # 被限流/封禁: 读 Retry-After 或指数退避
                wait = int(r.headers.get("Retry-After", 2 ** attempt * 3))
                await asyncio.sleep(min(wait, 30))
                continue
            return None  # 400 等 (币安无此合约) 直接放弃
        except Exception:
            await asyncio.sleep(1 + attempt)
    return None


# ============ Trade Simulation ============
def simulate_trade(klines, entry_price, cfg):
    """
    用 1 分钟 K 线模拟一次交易.

    关键规则: 当一根 K 线同时碰到 SL 和 TP, 保守起见认为 SL 先触发 (最坏情况).
    """
    sl_price = entry_price * (1 + cfg["stop_loss_pct"])
    tp1_price = entry_price * (1 + cfg["tp1_pct"])
    tp2_price = entry_price * (1 + cfg["tp2_pct"])

    tp1_hit = False
    last_close = entry_price

    for k in klines:
        ts_ms, _, high, low, close = k[0], k[1], float(k[2]), float(k[3]), float(k[4])
        last_close = close

        # 保守: 先判断止损
        if low <= sl_price:
            if tp1_hit:
                # 50% 已在 TP1 平掉, 剩下 50% 在 SL 平
                pnl = (cfg["tp1_pct"] + cfg["stop_loss_pct"]) / 2
                return {"outcome": "SL_after_TP1", "pnl_pct": pnl,
                        "exit_ts": ts_ms}
            return {"outcome": "SL", "pnl_pct": cfg["stop_loss_pct"],
                    "exit_ts": ts_ms}

        # 检查止盈
        if high >= tp2_price:
            if tp1_hit:
                pnl = (cfg["tp1_pct"] + cfg["tp2_pct"]) / 2
                return {"outcome": "TP2", "pnl_pct": pnl, "exit_ts": ts_ms}
            # 跳空直接到 TP2 (罕见)
            pnl = (cfg["tp1_pct"] + cfg["tp2_pct"]) / 2
            return {"outcome": "TP2_direct", "pnl_pct": pnl, "exit_ts": ts_ms}

        if high >= tp1_price and not tp1_hit:
            tp1_hit = True

    # 超时强平
    timeout_pct = (last_close - entry_price) / entry_price
    if tp1_hit:
        timeout_pct = (cfg["tp1_pct"] + timeout_pct) / 2
        return {"outcome": "TIMEOUT_after_TP1", "pnl_pct": timeout_pct,
                "exit_ts": klines[-1][0] if klines else None}
    return {"outcome": "TIMEOUT", "pnl_pct": timeout_pct,
            "exit_ts": klines[-1][0] if klines else None}


# ============ Aggregation ============
def aggregate_results(trades, cfg):
    """汇总统计."""
    total = len(trades)
    if total == 0:
        return None

    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    losses = total - wins

    pnls = [t["pnl_pct"] for t in trades]
    avg_pnl = sum(pnls) / total
    avg_win = sum(p for p in pnls if p > 0) / wins if wins else 0
    avg_loss = sum(p for p in pnls if p <= 0) / losses if losses else 0

    # 复利曲线 (按时间顺序)
    sorted_trades = sorted(trades, key=lambda t: t["candidate"]["ts_dt"])
    equity = cfg["initial_capital"]
    peak = equity
    max_dd_pct = 0
    curve = [equity]

    for t in sorted_trades:
        # 单笔有效收益 = 价格变动% × 杠杆 × 仓位占比
        effective = t["pnl_pct"] * cfg["leverage"] * cfg["position_pct"]
        equity *= (1 + effective)
        peak = max(peak, equity)
        dd = (equity - peak) / peak
        max_dd_pct = min(max_dd_pct, dd)
        curve.append(equity)

    # 收益分布
    outcomes = defaultdict(int)
    for t in trades:
        outcomes[t["outcome"]] += 1

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / total,
        "avg_pnl_pct": avg_pnl,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "expectancy": avg_pnl,
        "rr_ratio": -avg_win / avg_loss if avg_loss else 0,
        "final_equity": equity,
        "total_return": (equity / cfg["initial_capital"]) - 1,
        "max_drawdown_pct": max_dd_pct,
        "outcomes": dict(outcomes),
    }


def print_report(stats, candidates, cfg, trades):
    """打印报告."""
    print()
    print("=" * 64)
    print("📊  Strategy 1 回测报告 (R3 单独触发轧空)")
    print("=" * 64)
    print(f"参数:")
    print(f"  • 沉默期(无R1/R5)        {cfg['quiet_hours']}h")
    print(f"  • 冷却期(同币)            {cfg['cooldown_hours']}h")
    print(f"  • 止损                    {cfg['stop_loss_pct']:+.1%}")
    print(f"  • 止盈1 (平50%)          {cfg['tp1_pct']:+.1%}")
    print(f"  • 止盈2 (平50%)          {cfg['tp2_pct']:+.1%}")
    print(f"  • 最长持仓                {cfg['max_hold_hours']}h")
    print(f"  • 杠杆 / 单笔仓位         {cfg['leverage']}x / {cfg['position_pct']:.0%}")
    print()
    print(f"信号筛选:")
    print(f"  • 候选信号                {len(candidates)} 条")
    print(f"  • 实际成交                {stats['total']} 条 ({stats['total']-len(candidates)} 条因数据缺失被丢弃)" if stats['total'] != len(candidates) else f"  • 实际成交                {stats['total']} 条")
    print()
    print(f"交易表现:")
    print(f"  • 胜率                    {stats['win_rate']:.1%}  ({stats['wins']} 赢 / {stats['losses']} 输)")
    print(f"  • 平均单笔收益            {stats['avg_pnl_pct']:+.2%}")
    print(f"  • 平均盈利单              {stats['avg_win_pct']:+.2%}")
    print(f"  • 平均亏损单              {stats['avg_loss_pct']:+.2%}")
    print(f"  • 赢损比 (R:R)            {stats['rr_ratio']:.2f}")
    print()
    print(f"复利模拟 (初始资金 ${cfg['initial_capital']:,.0f}):")
    print(f"  • 最终资金                ${stats['final_equity']:,.2f}")
    print(f"  • 累计收益                {stats['total_return']:+.1%}")
    print(f"  • 最大回撤                {stats['max_drawdown_pct']:.1%}")
    print()
    print(f"结果分布:")
    for k, v in sorted(stats["outcomes"].items(), key=lambda x: -x[1]):
        pct = v / stats["total"]
        bar = "█" * int(pct * 30)
        print(f"  {k:22s} {v:3d}  {pct:5.1%}  {bar}")
    print()
    # 列出每笔交易
    print(f"逐笔明细:")
    for t in sorted(trades, key=lambda x: x["candidate"]["ts_dt"]):
        c = t["candidate"]
        ts = c["ts_dt"].strftime("%m-%d %H:%M")
        sign = "+" if t["pnl_pct"] >= 0 else ""
        print(f"  {ts}  {c['symbol']:8s}  {t['outcome']:22s}  {sign}{t['pnl_pct']:.2%}")


# ============ Main ============
async def run(cfg):
    log_path = Path(cfg["log_file"])
    if not log_path.exists():
        print(f"❌ 找不到日志文件: {log_path}")
        sys.exit(1)

    print(f"读取 {log_path}...")
    records = parse_log_file(log_path)
    if not records:
        print("❌ 日志中没有有效记录")
        return
    start_t = records[0]["ts_dt"]
    end_t = records[-1]["ts_dt"]
    print(f"加载 {len(records)} 条告警, 时间范围: {start_t} ~ {end_t}")

    candidates = find_candidates(records, cfg["quiet_hours"], cfg["cooldown_hours"])
    print(f"符合 Strategy 1 入场条件: {len(candidates)} 条")
    if not candidates:
        return

    # 只保留有入场价的候选
    valid = [c for c in candidates if c.get("price_now")]
    skipped_no_price = len(candidates) - len(valid)
    if skipped_no_price:
        print(f"跳过 {skipped_no_price} 条 (无 price_now)")

    # 并发拉取 K 线 (信号量限流, 避免币安 429). 拉取是纯 IO, 模拟是纯 CPU, 分两阶段.
    concurrency = cfg.get("concurrency", 4)
    sem = asyncio.Semaphore(concurrency)
    done = 0

    async def fetch_one(c, http):
        nonlocal done
        start_dt = c["ts_dt"]
        end_dt = start_dt + timedelta(hours=cfg["max_hold_hours"])
        async with sem:
            klines = await fetch_klines(c["symbol"], start_dt, end_dt, http)
        done += 1
        if done % 25 == 0 or done == len(valid):
            print(f"  拉取进度 {done}/{len(valid)}", flush=True)
        return c, klines

    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
        print(f"并发拉取 K 线 (concurrency={concurrency})...", flush=True)
        fetched = await asyncio.gather(*[fetch_one(c, http) for c in valid])

    # 模拟交易 (无网络)
    trades = []
    fail = 0
    for c, klines in fetched:
        if not klines:
            fail += 1
            continue
        t = simulate_trade(klines, c["price_now"], cfg)
        t["candidate"] = c
        trades.append(t)
    if fail:
        print(f"K线获取失败 {fail} 条 (已跳过)")
    print(f"成功模拟 {len(trades)} 笔交易")

    stats = aggregate_results(trades, cfg)
    if stats:
        print_report(stats, candidates, cfg, trades)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-file", default=DEFAULTS["log_file"])
    parser.add_argument("--quiet-hours", type=int, default=DEFAULTS["quiet_hours"])
    parser.add_argument("--cooldown-hours", type=int, default=DEFAULTS["cooldown_hours"])
    parser.add_argument("--max-hold", type=int, default=DEFAULTS["max_hold_hours"])
    parser.add_argument("--stop-loss", type=float, default=DEFAULTS["stop_loss_pct"])
    parser.add_argument("--tp1", type=float, default=DEFAULTS["tp1_pct"])
    parser.add_argument("--tp2", type=float, default=DEFAULTS["tp2_pct"])
    parser.add_argument("--leverage", type=float, default=DEFAULTS["leverage"])
    parser.add_argument("--position", type=float, default=DEFAULTS["position_pct"])
    parser.add_argument("--capital", type=float, default=DEFAULTS["initial_capital"])
    args = parser.parse_args()

    cfg = {
        "log_file": args.log_file,
        "quiet_hours": args.quiet_hours,
        "cooldown_hours": args.cooldown_hours,
        "max_hold_hours": args.max_hold,
        "stop_loss_pct": args.stop_loss,
        "tp1_pct": args.tp1,
        "tp2_pct": args.tp2,
        "leverage": args.leverage,
        "position_pct": args.position,
        "initial_capital": args.capital,
    }

    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
