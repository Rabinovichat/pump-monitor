"""
对 30 天告警日志做零网络的快速统计分析.
帮助判断: 哪些规则常触发, 多规则共振多不多, 各策略能筛出多少候选.

用法:
    python analyze_log.py logs/monitor_30d.log
"""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def parse(path):
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


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "logs/monitor_30d.log"
    recs = parse(path)
    n = len(recs)
    print(f"总告警记录: {n:,}")
    if not n:
        return
    span = recs[-1]["ts_dt"] - recs[0]["ts_dt"]
    print(f"时间跨度: {recs[0]['ts_dt']}  ~  {recs[-1]['ts_dt']}  ({span.days}天{span.seconds//3600}时)")

    # 每个 current_round_hits 组合出现次数
    combo = Counter()
    single_rule = Counter()
    any_rule = Counter()
    push_count = 0
    level_count = Counter()
    for r in recs:
        hits = tuple(sorted(r.get("current_round_hits", [])))
        combo[hits] += 1
        if len(hits) == 1:
            single_rule[hits[0]] += 1
        for tag in hits:
            any_rule[tag] += 1
        if r.get("should_push"):
            push_count += 1
        level_count[r.get("level", "?")] += 1

    print(f"\n应推送(should_push=true): {push_count:,}  ({push_count/n:.1%})")
    print(f"仅记录: {n-push_count:,}")

    print("\n=== 各规则单独出现次数 (在任意组合里) ===")
    for tag, c in sorted(any_rule.items()):
        print(f"  {tag}: {c:,}")

    print("\n=== 本轮命中组合 Top 15 ===")
    for hits, c in combo.most_common(15):
        combo_str = "+".join(hits) if hits else "(空)"
        print(f"  {combo_str:20s} {c:6,}  ({c/n:.1%})")

    print("\n=== 告警级别分布 ===")
    for lv, c in level_count.most_common():
        print(f"  {lv}  {c:,}  ({c/n:.1%})")

    # 多规则共振 (同一轮 >=2 条规则)
    multi = sum(c for hits, c in combo.items() if len(hits) >= 2)
    print(f"\n多规则共振(本轮>=2条): {multi:,}  ({multi/n:.1%})")

    # R3 分析 (Strategy 1 基础)
    r3_events = [r for r in recs if "R3" in r.get("current_round_hits", [])]
    r3_solo = [r for r in r3_events if r.get("current_round_hits") == ["R3"]]
    print(f"\n=== R3 分析 ===")
    print(f"  含 R3 的告警: {len(r3_events):,}")
    print(f"  R3 单独触发: {len(r3_solo):,}")

    # Strategy 1 候选: R3 + 过去6h无R1/R5 + 4h冷却
    def s1_candidates(records, quiet_hours=6, cooldown_hours=4):
        by_symbol = defaultdict(list)
        last_entry = {}
        cands = []
        for rec in records:
            sym = rec["symbol"]
            hits = rec.get("current_round_hits", [])
            if "R3" not in hits:
                by_symbol[sym].append(rec)
                continue
            cutoff = rec["ts_dt"] - timedelta(hours=quiet_hours)
            had = any(("R1" in p.get("current_round_hits", []) or "R5" in p.get("current_round_hits", []))
                      for p in by_symbol[sym] if p["ts_dt"] >= cutoff)
            if had:
                by_symbol[sym].append(rec); continue
            if sym in last_entry and (rec["ts_dt"]-last_entry[sym]).total_seconds() < cooldown_hours*3600:
                by_symbol[sym].append(rec); continue
            cands.append(rec); last_entry[sym] = rec["ts_dt"]; by_symbol[sym].append(rec)
        return cands

    s1 = s1_candidates(recs)
    print(f"  Strategy1 候选(quiet6h+cd4h): {len(s1):,}")

    # 有入场价的比例
    with_price = sum(1 for c in s1 if c.get("price_now"))
    print(f"  其中有 price_now: {with_price:,}")

    # 币种分布
    sym_count = Counter(r["symbol"] for r in recs)
    print(f"\n=== 触发最频繁的币种 Top 15 ===")
    for sym, c in sym_count.most_common(15):
        print(f"  {sym:12s} {c:,}")
    print(f"\n涉及币种总数: {len(sym_count)}")


if __name__ == "__main__":
    main()
