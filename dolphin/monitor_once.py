"""
DolphinScheduler 版: 单次运行 + 状态持久化
==========================================

与 monitor.py 核心逻辑完全一致,区别:
- 每次只跑一轮,跑完退出
- 状态 (signal_memory, netflow_history, etc.) 保存到 state.json
- 下次调度启动时自动读取上次状态

DolphinScheduler 配置:
    任务类型: Shell
    命令: cd /path/to/pump-monitor/dolphin && /path/to/.venv/bin/python monitor_once.py
    定时: 每 30 分钟 (cron: 0 */30 * * * ?)
"""

import asyncio
import json
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

# Add parent dir to path so we can import from monitor.py if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from dotenv import load_dotenv
from loguru import logger

# Load .env from parent dir or current dir
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# ============ CONFIG ============
CONFIG = {
    "loop_interval_seconds": 1800,
    "netflow_window_count": 2,
    "netflow_min_exchanges": 4,
    "scoring": {
        "memory_window_rounds": 8,
        "push_min_rules": 2,
        "push_override_levels": {"🔴"},
        "rule_base_scores": {
            "R1": 3, "R2": 2, "R3": 5, "R4": 2, "R5": 3,
        },
        "multi_rule_multiplier": 1.5,
    },
    "rules": {
        "r1_oi_vs_price_ratio": 3.0,
        "r1_oi_growth_min_pct": 0.033,
        "r1_oi_growth_min_usd": 50_000,
        "r1_oi_growth_zero_price_usd": 150_000,
        "r2_negative_funding_periods": 3,
        "r3_funding_rate_threshold": -0.0005,
        "r4_netflow_threshold_usd": 500_000,
        "r5_oi_growth_min_pct": 0.033,
        "r5_oi_growth_min_usd": 50_000,
        "r5_positive_funding_threshold": 0.0005,
    },
    "excluded_symbols": {
        "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "TRX", "TON",
        "DOT", "LINK", "MATIC", "LTC", "BCH", "UNI", "ATOM", "ETC", "FIL", "APT",
        "USDC", "FDUSD", "DAI", "TUSD",
    },
}

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_SUMMARY_TOKEN = os.getenv("TELEGRAM_SUMMARY_BOT_TOKEN", "")
TG_SUMMARY_CHAT_ID = os.getenv("TELEGRAM_SUMMARY_CHAT_ID", "")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_SUMMARY_WEBHOOK_URL = os.getenv("SLACK_SUMMARY_WEBHOOK_URL", "") or SLACK_WEBHOOK_URL

# ============ Logging ============
logger.remove()
logger.add(
    sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"),
    format="<green>{time:HH:mm:ss}</green> | <level>{level:8}</level> | {message}",
)
SCRIPT_DIR = Path(__file__).parent
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger.add(
    str(LOG_DIR / "monitor.log"), rotation="1 day", retention="30 days",
    level="DEBUG", encoding="utf-8",
)

# ============ State File ============
STATE_FILE = SCRIPT_DIR / "state.json"


# ============ Global State (loaded from file) ============
netflow_history = defaultdict(
    lambda: defaultdict(lambda: deque(maxlen=CONFIG["netflow_window_count"]))
)
last_levels = {}
round_count = 0
summary_alerts = []
last_summary_hour = -1
signal_memory = defaultdict(
    lambda: deque(maxlen=CONFIG["scoring"]["memory_window_rounds"])
)


# ====================================================================
#  State Persistence
# ====================================================================
def save_state():
    """Save all mutable global state to JSON file."""
    state = {
        "round_count": round_count,
        "last_summary_hour": last_summary_hour,
        "last_levels": last_levels,
        "summary_alerts": summary_alerts,
        "netflow_history": {
            sym: {ex: list(dq) for ex, dq in exchanges.items()}
            for sym, exchanges in netflow_history.items()
        },
        "signal_memory": {
            sym: [(rnd, list(tags), reasons) for rnd, tags, reasons in dq]
            for sym, dq in signal_memory.items()
        },
    }
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"State saved: round={round_count}, symbols_tracked={len(signal_memory)}")


def load_state():
    """Load state from JSON file, or start fresh if not exists."""
    global round_count, last_summary_hour, last_levels, summary_alerts

    if not STATE_FILE.exists():
        logger.info("No state file found, starting fresh")
        return

    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        round_count = raw.get("round_count", 0)
        last_summary_hour = raw.get("last_summary_hour", -1)
        last_levels.update(raw.get("last_levels", {}))
        summary_alerts.extend(raw.get("summary_alerts", []))

        # Restore netflow_history
        for sym, exchanges in raw.get("netflow_history", {}).items():
            for ex, values in exchanges.items():
                dq = netflow_history[sym][ex]
                for v in values:
                    dq.append(v)

        # Restore signal_memory
        for sym, entries in raw.get("signal_memory", {}).items():
            dq = signal_memory[sym]
            for rnd, tags, reasons in entries:
                dq.append((rnd, frozenset(tags), reasons))

        logger.info(
            f"State loaded: round={round_count}, "
            f"symbols={len(signal_memory)}, "
            f"netflow_symbols={len(netflow_history)}"
        )
    except Exception as e:
        logger.error(f"Failed to load state: {e}, starting fresh")


# ====================================================================
#  以下逻辑与 monitor.py 完全一致 (HTTP, Exchanges, Rules, etc.)
#  为避免维护两份代码,直接从 monitor.py 导入
# ====================================================================
from monitor import (
    BinanceFutures, OkxSwap,
    BinanceSpot, OkxSpot, BybitSpot, BitgetSpot,
    HtxSpot, GateSpot, MexcSpot, KucoinSpot,
    get_universe, update_netflow_history, fetch_symbol_data,
    rule_r1, rule_r2, rule_r3, rule_r4, rule_r5,
    detect_level_change, format_tg_message,
    send_slack_alert, send_slack_summary,
    log_alert_json, _fmt_usd,
    LEVEL_ORDER, LEVEL_LABELS,
)

# Override the global state references in monitor module
import monitor
monitor.netflow_history = netflow_history
monitor.signal_memory = signal_memory
monitor.last_levels = last_levels
monitor.summary_alerts = summary_alerts
monitor.last_summary_hour = last_summary_hour


# ====================================================================
#  evaluate() — same logic, uses our local state
# ====================================================================
def evaluate(base, data):
    global round_count
    r1_hit, r1_reason, r1_extra = rule_r1(data)
    r2_hit, r2_reason, r2_extra = rule_r2(data)
    r3_hit, r3_reason, r3_extra = rule_r3(data)
    r4_hit, r4_reason, r4_extra = rule_r4(base)
    r5_hit, r5_reason, r5_extra = rule_r5(data)

    hits = []
    current_round_tags = set()
    if r3_hit:
        hits.append(("R3", r3_reason))
        current_round_tags.add("R3")
    if r1_hit:
        hits.append(("R1", r1_reason))
        current_round_tags.add("R1")
    if r2_hit:
        hits.append(("R2", r2_reason))
        current_round_tags.add("R2")
    if r4_hit:
        hits.append(("R4", r4_reason))
        current_round_tags.add("R4")
    if r5_hit:
        hits.append(("R5", r5_reason))
        current_round_tags.add("R5")

    current_reasons = {tag: reason for tag, reason in hits}
    if current_round_tags:
        signal_memory[base].append((round_count, frozenset(current_round_tags), current_reasons))

    recent_rules = set()
    recent_reasons = {}
    for _rnd, tags, reasons in signal_memory[base]:
        recent_rules.update(tags)
        for tag, reason in reasons.items():
            recent_reasons[tag] = reason

    scoring_cfg = CONFIG["scoring"]
    base_scores = scoring_cfg["rule_base_scores"]
    score = sum(base_scores.get(tag, 0) for tag in recent_rules)
    if len(recent_rules) >= 2:
        score *= scoring_cfg["multi_rule_multiplier"] ** (len(recent_rules) - 1)

    if not recent_rules:
        level = "🟢"
    elif "R3" in recent_rules:
        level = "🔴"
    elif recent_rules & {"R1", "R2", "R5"}:
        level = "🟠"
    else:
        level = "🟡"

    should_push = False
    if current_round_tags:
        if level in scoring_cfg["push_override_levels"]:
            should_push = True
        elif len(recent_rules) >= scoring_cfg["push_min_rules"]:
            should_push = True

    total_1h_nf = sum(
        sum(w) for w in netflow_history[base].values()
        if len(w) >= CONFIG["netflow_window_count"]
    )

    return {
        "level": level,
        "hits": hits,
        "r1_extra": r1_extra,
        "r5_extra": r5_extra,
        "r4_extra": r4_extra,
        "total_1h_netflow": total_1h_nf,
        "score": round(score, 1),
        "recent_rules": sorted(recent_rules),
        "recent_reasons": recent_reasons,
        "current_round_hits": sorted(current_round_tags),
        "should_push": should_push,
    }


# ====================================================================
#  maybe_send_summary — same logic, uses our local state
# ====================================================================
async def maybe_send_summary(http):
    global last_summary_hour, summary_alerts
    now = datetime.now(timezone.utc)
    current_hour = now.hour
    summary_boundary = (current_hour // 6) * 6
    if summary_boundary == last_summary_hour:
        return
    if last_summary_hour == -1:
        last_summary_hour = summary_boundary
        return

    if not summary_alerts:
        msg = (
            f"📋 <b>6h 数据总结</b> ({last_summary_hour:02d}:00 ~ {summary_boundary:02d}:00 UTC)\n"
            "━━━━━━━━━━━━━━━━━\n"
            "本时段无告警触发。"
        )
    else:
        level_counts = {"🔴": 0, "🟠": 0, "🟡": 0}
        sym_data = {}
        for alert in summary_alerts:
            lv = alert["level"]
            level_counts[lv] = level_counts.get(lv, 0) + 1
            sym = alert["symbol"]
            if sym not in sym_data:
                sym_data[sym] = {
                    "distinct_rules": set(),
                    "max_score": 0,
                    "push_count": 0,
                    "total_count": 0,
                    "max_level": "🟡",
                }
            sd = sym_data[sym]
            sd["total_count"] += 1
            for tag, _ in alert["hits"]:
                sd["distinct_rules"].add(tag)
            sd["max_score"] = max(sd["max_score"], alert.get("score", 0))
            if alert.get("should_push"):
                sd["push_count"] += 1
            if LEVEL_ORDER.get(lv, 0) > LEVEL_ORDER.get(sd["max_level"], 0):
                sd["max_level"] = lv

        ranked = sorted(
            sym_data.items(),
            key=lambda x: (
                len(x[1]["distinct_rules"]),
                x[1]["max_score"],
                x[1]["push_count"],
            ),
            reverse=True,
        )

        total_alerts = len(summary_alerts)
        total_pushed = sum(1 for a in summary_alerts if a.get("should_push"))

        lines = [
            f"📋 <b>6h 数据总结</b> ({last_summary_hour:02d}:00 ~ {summary_boundary:02d}:00 UTC)",
            "━━━━━━━━━━━━━━━━━",
            f"📊 告警: {total_alerts} 条 (推送 {total_pushed}, 仅记录 {total_alerts - total_pushed})",
            f"   🔴 {level_counts['🔴']} | 🟠 {level_counts['🟠']} | 🟡 {level_counts['🟡']}",
            "",
            "🏆 <b>高价值币种 (Top 10)</b>",
        ]
        for sym, sd in ranked[:10]:
            rules_str = ", ".join(sorted(sd["distinct_rules"]))
            push_str = f"{sd['push_count']}推" if sd["push_count"] > 0 else "无推送"
            lines.append(
                f"  • {sd['max_level']} <b>{sym}</b> "
                f"| {rules_str} | {push_str}/{sd['total_count']}次 "
                f"| ⚡{sd['max_score']}"
            )
        lines.append("")
        lines.append(f"<i>{now.strftime('%Y-%m-%d %H:%M UTC')}</i>")
        msg = "\n".join(lines)

    await send_slack_summary(msg, http)
    last_summary_hour = summary_boundary
    summary_alerts.clear()


# ====================================================================
#  Main: single run
# ====================================================================
async def run_once():
    global round_count
    round_count += 1

    http = httpx.AsyncClient(
        timeout=httpx.Timeout(20, connect=10),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        headers={"User-Agent": "PumpMonitor/4.0-dolphin"},
    )

    try:
        bf = BinanceFutures(http)
        okx_swap = OkxSwap(http)
        spot_clients = [
            BinanceSpot(http), OkxSpot(http), BybitSpot(http), BitgetSpot(http),
            HtxSpot(http), GateSpot(http), MexcSpot(http), KucoinSpot(http),
        ]

        # Load spot symbols
        print("Loading spot symbol lists from 8 exchanges...")
        results = await asyncio.gather(
            *[c.load_spot_symbols() for c in spot_clients],
            return_exceptions=True,
        )
        for c, r in zip(spot_clients, results):
            if isinstance(r, Exception):
                logger.error(f"{c.name} symbol load failed: {r}")

        # Get universe
        t0 = time.time()
        bn_spot = spot_clients[0]
        universe = await get_universe(bf, okx_swap)
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] 第 {round_count} 轮 | 币种池 {len(universe)}")
        logger.info(f"Round {round_count} start: {len(universe)} symbols")

        # Update netflow
        nf_statuses = await update_netflow_history(universe, spot_clients)
        status_parts = []
        for ex in spot_clients:
            ok_c, fail_c = nf_statuses.get(ex.name, (0, 0))
            mark = "✓" if fail_c == 0 else f"✗({fail_c})"
            status_parts.append(f"{ex.name} {mark}")
        print(f"  {' | '.join(status_parts)}")

        # Evaluate all symbols
        sem = asyncio.Semaphore(20)
        push_alerts_by_level = {"🔴": [], "🟠": [], "🟡": []}
        log_only_count = 0

        async def process_one(base):
            nonlocal log_only_count
            async with sem:
                try:
                    data = await fetch_symbol_data(base, bf, okx_swap, bn_spot)
                    result = evaluate(base, data)
                    if result["current_round_hits"]:
                        level_change = detect_level_change(base, result["level"])
                        log_alert_json(base, result, data)
                        last_levels[base] = result["level"]
                        summary_alerts.append({
                            "symbol": base,
                            "level": result["level"],
                            "hits": result["hits"],
                            "score": result["score"],
                            "recent_rules": result["recent_rules"],
                            "current_round_hits": result["current_round_hits"],
                            "should_push": result["should_push"],
                        })
                        if result["should_push"]:
                            push_alerts_by_level[result["level"]].append(
                                (base, result, data, level_change)
                            )
                        else:
                            log_only_count += 1
                            logger.info(
                                f"{base} 单规则 {result['current_round_hits']} "
                                f"score={result['score']} → 仅记录"
                            )
                except Exception as e:
                    logger.exception(f"Processing {base} failed: {e}")

        await asyncio.gather(*[process_one(b) for b in universe])

        # Push to TG
        total_pushed = 0
        for level in ["🔴", "🟠", "🟡"]:
            push_alerts_by_level[level].sort(key=lambda x: x[1]["score"], reverse=True)
            for base, result, data, level_change in push_alerts_by_level[level]:
                msg = format_tg_message(base, result, data, level_change)
                await send_slack_alert(msg, http)
                total_pushed += 1
                if total_pushed < 20:
                    await asyncio.sleep(3.5)
                else:
                    logger.warning("TG rate limit: >20 alerts, stopping")
                    break
            else:
                continue
            break

        # 6h summary check
        await maybe_send_summary(http)

        # Status
        elapsed = time.time() - t0
        parts = []
        for lv in ["🔴", "🟠", "🟡"]:
            n = len(push_alerts_by_level[lv])
            if n > 0:
                names = " ".join(a[0] for a in push_alerts_by_level[lv][:3])
                parts.append(f"{lv}{n}({names})")
        alert_summary = " | ".join(parts) if parts else "无告警"
        print(
            f"  完成 | {elapsed:.0f}s | {alert_summary} | "
            f"推送 {total_pushed}, 记录 {log_only_count}"
        )
        logger.info(f"Round {round_count} done: {total_pushed} pushed, {log_only_count} log-only")

    finally:
        await http.aclose()


def main():
    # Load previous state
    load_state()

    # Sync module-level state after load
    import monitor
    monitor.netflow_history = netflow_history
    monitor.signal_memory = signal_memory
    monitor.last_levels = last_levels
    monitor.round_count = round_count

    # Run one round
    asyncio.run(run_once())

    # Save state for next run
    save_state()


if __name__ == "__main__":
    main()
