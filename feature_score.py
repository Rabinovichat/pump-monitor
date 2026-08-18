"""
R1+R3 信号的强度分级: 用规则内部的连续量预测收益, 替代已退化的 score.

为什么要做:
    推送收窄到「本轮 R1+R3」之后, score 在这个池子里几乎是常量 —— 实测 108 个
    信号中 83% 取同一个值, 而且历史上的差异几乎全部来自【已下线的 R2】.
    换句话说 score 现在只回答"有没有别的规则碰巧也响了", 完全不回答
    "这个信号有多强". 真正的强度信息在规则内部的连续量里:
    OI 涨了多少 / 资金费偏离多深 / OI 相对价格的异常倍数 / 仓位绝对规模.

方法论约束 (N≈108, 必须克制, 否则一定过拟合):
    1. 先做【单变量】分层, 只接受"单调" + "前后两个半段都成立"的特征.
    2. 不做多元回归拟合权重 —— 108 个样本拟合出的系数就是噪音.
       要合成就用等权 rank 复合: 参数越少, 越不容易骗自己.
    3. 分层用三分位(每层~36), 不用五分位(每层~21, 一个尾部样本就能翻盘).
    4. 资金费率这个特征有陷阱: 它【机械地】决定了做空的资金费成本, 所以它
       和净收益天然相关. 必须同时看"纯价格收益", 才能区分
       "真的预测了价格" 还是 "只是把成本项换了个说法".

用法:
    python feature_score.py                    # 全样本
    python feature_score.py --period first     # 前半(样本内)
    python feature_score.py --period second    # 后半(样本外)
    python feature_score.py --refresh          # 忽略缓存重新拉数据
"""
import argparse
import asyncio
import json
import re
import statistics
import sys
from datetime import timedelta
from pathlib import Path

import httpx

from backtest import parse_log_file, fetch_klines
from funding_backtest import build_cohort
from grid_sltp import slice_period
from hold_period import fetch_funding_ts, bar_at_or_after

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

TAKER_FEE = 0.0004
HOLD_H = 24                     # 用 hold_period.py 验证出的最优持仓
COOLDOWN_H = 24                 # 与持仓周期对齐
FETCH_H = HOLD_H + 6
CACHE = Path(__file__).parent / ".cache_feature.json"


# ====================================================================
#  特征提取 —— 全部来自日志里已有的字段/reason 字符串
# ====================================================================
RE_OI = re.compile(r"OI 1h \+([\d.]+)% \(\+\$([\d,]+)\)")
RE_PRICE = re.compile(r"价格 ([+-][\d.]+)%")
RE_MULT = re.compile(r"\(([\d.]+)×\)")


def extract_features(rec):
    """从一条告警记录里抽出所有连续量. 缺失返回 None(该条会被剔除)."""
    hits = dict((h[0], h[1]) for h in rec.get("hits", []) if h)
    r1 = hits.get("R1", "")

    m_oi = RE_OI.search(r1)
    if not m_oi:
        return None
    oi_growth_usd = float(m_oi.group(2).replace(",", ""))

    # OI 增幅优先用美元额反推(全精度), reason 里的百分比只有 0.1% 精度.
    # oi_1h = oi_now - oi_growth_usd
    oi_now = sum(v for v in (rec.get("bn_oi_now"), rec.get("ok_oi_now")) if v)
    oi_growth = None
    if oi_now and oi_now > oi_growth_usd:
        oi_growth = oi_growth_usd / (oi_now - oi_growth_usd)
    if oi_growth is None or not (0 < oi_growth < 5):
        oi_growth = float(m_oi.group(1)) / 100      # 回退到字符串值

    m_price = RE_PRICE.search(r1)
    price_growth = float(m_price.group(1)) / 100 if m_price else 0.0

    m_mult = RE_MULT.search(r1)
    if m_mult:
        mult = float(m_mult.group(1))
    elif price_growth != 0:
        mult = oi_growth / abs(price_growth)
    else:
        mult = 100.0                                 # "价格不变 (∞×)"

    frs = [f for f in (rec.get("bn_fr_cur"), rec.get("ok_fr_cur")) if f is not None]
    if not frs:
        return None
    thr = -0.0005
    return {
        "fr_min": min(frs),                          # 最负的资金费率(偏离越深越负)
        "fr_both": 1.0 if sum(1 for f in frs if f <= thr) >= 2 else 0.0,
        "oi_growth": oi_growth,                      # OI 1h 增幅 %
        "oi_growth_usd": oi_growth_usd,              # OI 1h 增幅 $ (绝对建仓规模)
        "price_growth": price_growth,                # 同期价格变动
        "mult": mult,                                # OI/价格 异常倍数
        "oi_total": oi_now or 0.0,                   # 总持仓量 $ (流动性/体量代理)
        "netflow_1h": rec.get("netflow_1h") or 0.0,
        "score": rec.get("score", 0),                # 对照组: 现有 score
    }


FEATURE_LABELS = [
    ("fr_min",        "资金费率(越负越拥挤)"),
    ("fr_both",       "两所同时命中R3"),
    ("oi_growth",     "OI增幅 %"),
    ("oi_growth_usd", "OI增幅 $"),
    ("price_growth",  "同期价格变动"),
    ("mult",          "OI/价格 异常倍数"),
    ("oi_total",      "总持仓量 $"),
    ("netflow_1h",    "现货净流入 $"),
    ("score",         "现有 score (对照)"),
]


# ====================================================================
#  统计工具 (纯标准库)
# ====================================================================
def ranks(xs):
    """平均秩 (处理并值)."""
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    out = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            out[order[k]] = avg
        i = j + 1
    return out


def spearman(xs, ys):
    """秩相关 —— 对肥尾稳健, 比 Pearson 适合这里的收益分布."""
    if len(xs) < 3:
        return 0.0
    rx, ry = ranks(xs), ranks(ys)
    mx, my = statistics.mean(rx), statistics.mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = sum((a - mx) ** 2 for a in rx) ** 0.5
    dy = sum((b - my) ** 2 for b in ry) ** 0.5
    return num / (dx * dy) if dx and dy else 0.0


def fmt_val(key, v):
    if key in ("oi_growth_usd", "oi_total", "netflow_1h"):
        return f"{v / 1000:,.0f}k"
    if key in ("fr_min", "oi_growth", "price_growth"):
        return f"{v:+.2%}"
    return f"{v:.1f}"


# ====================================================================
#  数据获取 + 缓存
# ====================================================================
async def load_returns(cands, refresh):
    """返回 {key: (price_ret, funding_pnl)} —— 做空 HOLD_H 小时的收益分解."""
    cache = {}
    if CACHE.exists() and not refresh:
        try:
            cache = json.loads(CACHE.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    need = [c for c in cands if f"{c['symbol']}_{int(c['ts_dt'].timestamp())}" not in cache]
    print(f"缓存命中 {len(cands) - len(need)}/{len(cands)}, 需拉取 {len(need)}")

    if need:
        sem = asyncio.Semaphore(3)
        done = 0

        async def one(c, http):
            nonlocal done
            start, end = c["ts_dt"], c["ts_dt"] + timedelta(hours=FETCH_H)
            async with sem:
                kl = await fetch_klines(c["symbol"], start, end, http)
                fr = await fetch_funding_ts(c["symbol"], start, end, http)
            done += 1
            if done % 25 == 0 or done == len(need):
                print(f"  拉取 {done}/{len(need)}", flush=True)
            if not kl or fr is None:
                return None
            # 只存需要的字段, 缓存体积小一个数量级
            return (f"{c['symbol']}_{int(c['ts_dt'].timestamp())}",
                    {"kl": [[int(k[6]), float(k[4])] for k in kl], "fr": fr})

        async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as http:
            for r in await asyncio.gather(*[one(c, http) for c in need]):
                if r:
                    cache[r[0]] = r[1]
        CACHE.write_text(json.dumps(cache), encoding="utf-8")

    out = {}
    for c in cands:
        key = f"{c['symbol']}_{int(c['ts_dt'].timestamp())}"
        d = cache.get(key)
        if not d:
            continue
        sig_ms = int(c["ts_dt"].timestamp() * 1000)
        kl = d["kl"]
        xi = next((i for i, k in enumerate(kl) if k[0] >= sig_ms + HOLD_H * 3600_000), None)
        if xi is None:
            continue
        entry, exit_p, exit_ms = c["price_now"], kl[xi][1], kl[xi][0]
        price_ret = -(exit_p - entry) / entry                       # 做空
        f_pnl = sum(r for t, r in d["fr"] if sig_ms < t <= exit_ms)  # 空头付负费率
        out[key] = (price_ret, f_pnl)
    return out


# ====================================================================
#  主流程
# ====================================================================
async def build_rows(period, refresh, log_file):
    """→ [(features, price_ret, net_ret), ...]"""
    records = slice_period(parse_log_file(log_file), period)
    cands = [c for c in build_cohort(records, "r1_r3", cooldown_hours=COOLDOWN_H)
             if c.get("price_now")]
    rets = await load_returns(cands, refresh)

    rows = []
    for c in cands:
        key = f"{c['symbol']}_{int(c['ts_dt'].timestamp())}"
        if key not in rets:
            continue
        f = extract_features(c)
        if f is None:
            continue
        pr, fp = rets[key]
        rows.append((f, pr, pr + fp - 2 * TAKER_FEE))
    return rows


async def stability(refresh, log_file):
    """把前后两个半段的秩相关并排放 —— 判断特征是真信号还是噪音的最强证据.

    噪音地板: 秩相关的标准误 ≈ 1/sqrt(N-1). 若 |rho| 没超过 ~2 个标准误,
    它和 0 没有区别; 更要命的是【符号翻转】—— 真实的因子不会在两个
    相邻时段里换方向, 噪音会.
    """
    halves = {}
    for p in ("first", "second"):
        halves[p] = await build_rows(p, refresh, log_file)

    n1, n2 = len(halves["first"]), len(halves["second"])
    se1, se2 = 1 / max(n1 - 1, 1) ** 0.5, 1 / max(n2 - 1, 1) ** 0.5
    print(f"\n{'=' * 96}")
    print(f"特征稳定性检验 — 前半 N={n1} vs 后半 N={n2}")
    print(f"噪音地板: 秩相关标准误 ≈ ±{se1:.2f} / ±{se2:.2f}, "
          f"故 |秩相关| < ~{2 * se1:.2f} 与 0 无区别")
    print(f"另外这里一共测了 {len(FEATURE_LABELS)} 个特征, 多重比较下"
          f"总会有 1~2 个纯靠运气显著 —— 所以【必须】看两段是否一致")
    print("=" * 96)
    print(f"{'特征':<22}{'前半 rho':>12}{'后半 rho':>12}{'':>4}{'判定':<40}")
    print("-" * 96)

    for key, label in FEATURE_LABELS:
        v1 = [r[0][key] for r in halves["first"]]
        v2 = [r[0][key] for r in halves["second"]]
        if len(set(v1)) < 3 or len(set(v2)) < 3:
            print(f"{label:<20}{'—':>14}{'—':>12}    取值不足, 无法评估")
            continue
        r1 = spearman(v1, [r[2] for r in halves["first"]])
        r2 = spearman(v2, [r[2] for r in halves["second"]])
        if r1 * r2 < 0:
            verdict = "✗ 符号翻转 → 噪音"
        elif abs(r1) < 2 * se1 and abs(r2) < 2 * se2:
            verdict = "✗ 两段都在噪音地板内"
        elif abs(r2) < se2:
            verdict = "✗ 样本外衰减到噪音内"
        elif abs(r1) >= 2 * se1 and abs(r2) >= 2 * se2:
            verdict = "✓ 两段都显著且同向"
        else:
            verdict = "~ 方向一致但样本外偏弱"
        print(f"{label:<20}{r1:>+14.2f}{r2:>+12.2f}    {verdict:<40}")
    print("-" * 96)

    # 最终裁决: 用前半选出的方向, 在后半做一次真正的样本外分档
    print("\n【样本外实战检验】用前半选出的 OI 增幅方向, 在后半分档:")
    for key, label in (("oi_growth", "OI增幅 %"), ("oi_growth_usd", "OI增幅 $")):
        rows2 = halves["second"]
        vals = [r[0][key] for r in rows2]
        idx = sorted(range(len(rows2)), key=lambda i: vals[i])
        k = len(rows2) // 3
        lo = [rows2[i][2] for i in idx[:k]]
        hi = [rows2[i][2] for i in idx[-k:]]
        gap = statistics.mean(hi) - statistics.mean(lo)
        mark = "✓ 方向保持" if gap > 0 else "✗ 方向反了"
        print(f"  {label:<14} 高档 {statistics.mean(hi):+.2%} vs 低档 "
              f"{statistics.mean(lo):+.2%}  → 差 {gap * 100:+.2f}pp  {mark}")


async def run(period, refresh, log_file):
    rows = await build_rows(period, refresh, log_file)

    if len(rows) < 15:
        print(f"样本太少 ({len(rows)}), 放弃")
        return
    nets = [r[2] for r in rows]
    se = 1 / max(len(rows) - 1, 1) ** 0.5
    print(f"\n{'=' * 96}")
    print(f"R1+R3 特征强度分析 — 做空{HOLD_H}h, 去重{COOLDOWN_H}h, 时段={period}, N={len(rows)}")
    print(f"基准: 净收益均值 {statistics.mean(nets):+.2%}, "
          f"中位 {statistics.median(nets):+.2%}, "
          f"胜率 {sum(1 for x in nets if x > 0) / len(nets):.0%}")
    print(f"噪音地板: 秩相关标准误 ≈ ±{se:.2f} → |秩相关| < {2 * se:.2f} 基本等于 0")
    print("=" * 96)

    n3 = len(rows) // 3
    print(f"\n{'特征':<22}{'秩相关':>8}  "
          f"{'低三分位':>22}{'中三分位':>22}{'高三分位':>22}")
    print(f"{'':<22}{'(净收益)':>8}  " + "".join(f"{'均值/胜率':>22}" for _ in range(3)))
    print("-" * 96)

    findings = []
    for key, label in FEATURE_LABELS:
        vals = [r[0][key] for r in rows]
        if len(set(vals)) < 3:
            print(f"{label:<20}{'—':>10}  (取值不足 3 种: {sorted(set(vals))[:4]})")
            continue
        rho = spearman(vals, nets)
        idx = sorted(range(len(rows)), key=lambda i: vals[i])
        buckets = [idx[:n3], idx[n3:2 * n3], idx[2 * n3:]]
        cells, means, wrs = [], [], []
        for b in buckets:
            bn = [rows[i][2] for i in b]
            m = statistics.mean(bn)
            means.append(m)
            wr = sum(1 for x in bn if x > 0) / len(bn)
            wrs.append(wr)
            # b 里已经是原始行号(来自 idx), 直接取值; 早先写成 vals[idx[b[0]]] 是双重索引
            lo, hi = vals[b[0]], vals[b[-1]]
            cells.append(f"{m:+.2%}/{wr:.0%} [{fmt_val(key, lo)}~{fmt_val(key, hi)}]")

        def is_mono(seq):
            return seq[0] < seq[1] < seq[2] or seq[0] > seq[1] > seq[2]

        # 均值单调易被单个肥尾样本打断; 胜率单调更稳健, 两者分开标注
        mono_m, mono_w = is_mono(means), is_mono(wrs)
        flag = ("★均值单调" if mono_m else "") + ("☆胜率单调" if mono_w else "")
        print(f"{label:<20}{rho:>+10.2f}  " + "".join(f"{c:>26}" for c in cells) + " " + flag)
        findings.append((key, label, rho, means, wrs, mono_m, mono_w))

    # 资金费率的陷阱: 它机械地决定做空成本, 必须看纯价格收益才知道是否真有预测力
    prices = [r[1] for r in rows]
    fr_vals = [r[0]["fr_min"] for r in rows]
    print("-" * 96)
    print(f"\n【陷阱核查】资金费率 vs 收益: 秩相关(净收益)={spearman(fr_vals, nets):+.2f}, "
          f"秩相关(纯价格收益)={spearman(fr_vals, prices):+.2f}")
    print("  若两者差异大 → 资金费率的'预测力'主要是成本项的机械效应, 不是真预测价格.")

    print("\n【候选特征汇总】(单调 + |秩相关|≥0.15 才值得进一步验证)")
    mono_f = [f for f in findings if (f[5] or f[6]) and abs(f[2]) >= 0.15]
    if not mono_f:
        print("  无 —— 没有任何特征既单调又有 |秩相关|≥0.15. "
              "在这个样本量下, 这意味着不该做分级仓位.")
    else:
        for key, label, rho, means, wrs, mm, mw in sorted(mono_f, key=lambda x: -abs(x[2])):
            direction = "越大越好" if means[2] > means[0] else "越小越好"
            kind = "均值+胜率均单调" if (mm and mw) else ("仅均值单调" if mm else "仅胜率单调")
            print(f"  • {label:<22} 秩相关{rho:+.2f}  {direction}  ({kind})")
            print(f"      净收益 低→高: {means[0]:+.2%} → {means[1]:+.2%} → {means[2]:+.2%}")
            print(f"      胜率   低→高: {wrs[0]:.0%} → {wrs[1]:.0%} → {wrs[2]:.0%}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", default="all", choices=["all", "first", "second"])
    ap.add_argument("--stability", action="store_true",
                    help="前后半段秩相关并排对比 + 样本外分档检验")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--log-file", default="logs/monitor_30d.log")
    args = ap.parse_args()
    if args.stability:
        asyncio.run(stability(args.refresh, args.log_file))
    else:
        asyncio.run(run(args.period, args.refresh, args.log_file))


if __name__ == "__main__":
    main()
