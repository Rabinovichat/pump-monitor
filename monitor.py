"""
加密货币拉升预警监控 v4
========================

识别拉升信号:OI 异常增长、资金费率持续为负/极值、现货净流入异常、OI+正费率多头信号。
8 家交易所聚合数据,Telegram 群实时推送。

v4 改进:
    - 评分机制: 多规则联合触发的信号才推送 TG,单规则仅记日志,大幅降低噪音
    - 2h 信号记忆窗口: 不同轮次触发的规则会被关联,形成综合评分
    - 新增 R5: OI 增长 + 正费率 (≥+0.05%) = 多头主导拉升信号
    - 6h 总结按规则多样性排序,而非简单计数

安装依赖:
    pip install -r requirements.txt

创建 Telegram Bot 并获取 Chat ID:
    1. 在 TG 搜索 @BotFather,发送 /newbot
    2. 按提示起名(如 "Pump Alert Bot"),拿到 Bot Token
       格式: 1234567890:ABCdefGHI...
    3. 建一个群(如 "拉升预警"),把 Bot 拉进群并设为管理员
    4. 在群里随便发一条消息,浏览器访问:
       https://api.telegram.org/bot<TOKEN>/getUpdates
       找到 "chat":{"id":-100...} 那个负数就是 Chat ID
    5. 复制 .env.example 为 .env,填入 Token 和 Chat ID:
       TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHI...
       TELEGRAM_CHAT_ID=-1001234567890

启动:
    python monitor.py

五条规则:
    R1 OI 异常增长       1h OI 增长率 >= 3x 价格涨幅           🟠 预警
    R2 资金费率持续负     过去 24h(3期) 所有结算均 < 0          🟠 预警
    R3 资金费率极值       当前资金费率 <= -0.05%                🔴 行动
    R4 现货净流异常       8 家聚合 1h 净流入/出 >= $500,000     🟡 关注
    R5 OI增长+正费率     OI 异常增长且正费率 >= +0.05%         🟠 预警

推送规则:
    🔴 级别(R3): 单条即推送
    其余: 2h 窗口内 ≥2 条不同规则触发才推送,单条仅记日志

R4 预热期: 启动后需 45~60 分钟累积 4 个 15min 窗口后才开始触发。

日志: logs/monitor.log (日级轮转,保留 30 天)
    告警级别 WARNING(JSON 格式),可用 jq / pandas 查询。

调阈值: 修改脚本内 CONFIG 字典的 rules / scoring 部分。
"""

import asyncio
import json
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ============ CONFIG ============
CONFIG = {
    "loop_interval_seconds": 1800,
    "netflow_window_count": 4,
    "netflow_min_exchanges": 4,
    "scoring": {
        "memory_window_rounds": 8,          # 2h = 8 × 15min 信号记忆窗口
        "push_min_rules": 2,                # ≥2 条不同规则才推送 TG
        "push_override_levels": {"🔴"},     # 🔴 级别单条也推送
        "rule_base_scores": {
            "R1": 3, "R2": 2, "R3": 5, "R4": 2, "R5": 3,
        },
        "multi_rule_multiplier": 1.5,       # 每多一条规则 score ×1.5
    },
    "rules": {
        "r1_oi_vs_price_ratio": 3.0,
        "r1_oi_growth_min_pct": 0.033,         # OI 增长率不低于 3.3%
        "r1_oi_growth_min_usd": 50_000,         # OI 绝对增长不低于 $50k
        "r1_oi_growth_zero_price_usd": 150_000, # 价格不变时 OI 增长 >= $150k
        "r2_negative_funding_periods": 3,
        "r3_funding_rate_threshold": -0.0005,
        "r4_netflow_threshold_usd": 500_000,
        "r5_oi_growth_min_pct": 0.033,         # R5: OI 增长率门槛 (同 R1)
        "r5_oi_growth_min_usd": 50_000,         # R5: OI 绝对增长门槛 (同 R1)
        "r5_positive_funding_threshold": 0.0005, # R5: 正 funding ≥ +0.05%
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

# ============ Logging ============
logger.remove()
logger.add(
    sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"),
    format="<green>{time:HH:mm:ss}</green> | <level>{level:8}</level> | {message}",
)
os.makedirs("logs", exist_ok=True)
logger.add(
    "logs/monitor.log", rotation="1 day", retention="30 days",
    level="DEBUG", encoding="utf-8",
)

# ============ Global State ============
netflow_history: dict[str, dict[str, deque]] = defaultdict(
    lambda: defaultdict(lambda: deque(maxlen=CONFIG["netflow_window_count"]))
)
last_levels: dict[str, str] = {}
round_count = 0
# 6h summary accumulator: list of alert records per summary window
summary_alerts: list[dict] = []
last_summary_hour: int = -1
# 2h signal memory: {symbol: deque of (round_number, frozenset_of_rule_tags)}
signal_memory: dict[str, deque] = defaultdict(
    lambda: deque(maxlen=CONFIG["scoring"]["memory_window_rounds"])
)


# ====================================================================
#  HTTP Helper
# ====================================================================
async def http_get(client, url, params=None, sem=None, retries=3):
    _sem = sem or asyncio.Semaphore(9999)
    async with _sem:
        for attempt in range(retries):
            try:
                r = await client.get(url, params=params, timeout=15)
                if r.status_code == 429:
                    logger.warning(f"429 rate limit: {url}")
                    await asyncio.sleep(60)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise


# ====================================================================
#  Binance Futures (OI + Funding Rate)
# ====================================================================
class BinanceFutures:
    BASE = "https://fapi.binance.com"

    def __init__(self, client):
        self.client = client
        self.sem = asyncio.Semaphore(8)

    @staticmethod
    def _sym(base):
        return f"{base.upper()}USDT"

    async def get_perp_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/fapi/v1/exchangeInfo")
        return {
            s["baseAsset"].upper()
            for s in data["symbols"]
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        }

    async def get_oi_history(self, base):
        """(oi_now_usd, oi_1h_ago_usd) or (None, None)"""
        try:
            data = await http_get(
                self.client,
                f"{self.BASE}/futures/data/openInterestHist",
                params={"symbol": self._sym(base), "period": "1h", "limit": 2},
                sem=self.sem,
            )
            if data and len(data) >= 2:
                # ascending by ts: [older, newer]
                return float(data[-1]["sumOpenInterestValue"]), float(data[0]["sumOpenInterestValue"])
        except Exception as e:
            logger.warning(f"Binance OI hist {base}: {e}")
        return None, None

    async def get_funding_current(self, base):
        try:
            data = await http_get(
                self.client,
                f"{self.BASE}/fapi/v1/premiumIndex",
                params={"symbol": self._sym(base)},
                sem=self.sem,
            )
            return float(data["lastFundingRate"])
        except Exception as e:
            logger.warning(f"Binance FR current {base}: {e}")
            return None

    async def get_funding_history(self, base, limit=3):
        try:
            data = await http_get(
                self.client,
                f"{self.BASE}/fapi/v1/fundingRate",
                params={"symbol": self._sym(base), "limit": limit},
                sem=self.sem,
            )
            return [float(d["fundingRate"]) for d in data]
        except Exception as e:
            logger.warning(f"Binance FR hist {base}: {e}")
            return []


# ====================================================================
#  OKX Swap (OI + Funding Rate)
# ====================================================================
class OkxSwap:
    BASE = "https://www.okx.com"

    def __init__(self, client):
        self.client = client
        self.sem = asyncio.Semaphore(5)

    @staticmethod
    def _inst(base):
        return f"{base.upper()}-USDT-SWAP"

    async def get_perp_symbols(self):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v5/public/instruments",
            params={"instType": "SWAP"},
        )
        symbols = set()
        for inst in data.get("data", []):
            if inst.get("settleCcy") == "USDT" and inst.get("state") == "live":
                parts = inst["instId"].split("-")
                if len(parts) >= 3:
                    symbols.add(parts[0].upper())
        return symbols

    async def get_oi_history(self, base):
        """(oi_now_usd, oi_1h_ago_usd) via rubik endpoint. Arrays: [ts, oi, oiCcy, oiUsd]"""
        try:
            data = await http_get(
                self.client,
                f"{self.BASE}/api/v5/rubik/stat/contracts/open-interest-history",
                params={"instId": self._inst(base), "period": "1H", "limit": "2"},
                sem=self.sem,
            )
            recs = data.get("data", [])
            if len(recs) >= 2:
                # recs[0] = newer, recs[1] = older; each is [ts, oi, oiCcy, oiUsd]
                return float(recs[0][3]), float(recs[1][3])
        except Exception as e:
            logger.warning(f"OKX OI hist {base}: {e}")
        return None, None

    async def get_funding_current(self, base):
        try:
            data = await http_get(
                self.client,
                f"{self.BASE}/api/v5/public/funding-rate",
                params={"instId": self._inst(base)},
                sem=self.sem,
            )
            recs = data.get("data", [])
            if recs:
                return float(recs[0]["fundingRate"])
        except Exception as e:
            logger.warning(f"OKX FR current {base}: {e}")
        return None

    async def get_funding_history(self, base, limit=3):
        try:
            data = await http_get(
                self.client,
                f"{self.BASE}/api/v5/public/funding-rate-history",
                params={"instId": self._inst(base), "limit": str(limit)},
                sem=self.sem,
            )
            return [float(d["fundingRate"]) for d in data.get("data", [])]
        except Exception as e:
            logger.warning(f"OKX FR hist {base}: {e}")
            return []


# ====================================================================
#  Spot Exchanges — Base
# ====================================================================
class BaseSpotExchange:
    name: str = ""
    _spot_bases: set
    _sem: asyncio.Semaphore

    def __init__(self, client):
        self.client = client
        self._spot_bases = set()
        self._sem = asyncio.Semaphore(5)

    async def load_spot_symbols(self):
        raise NotImplementedError

    def has_symbol(self, base):
        return base.upper() in self._spot_bases

    def format_symbol(self, base):
        raise NotImplementedError

    async def get_15min_netflow_usd(self, base):
        raise NotImplementedError


class TradesSpotExchange(BaseSpotExchange):
    """Base for Tier-B exchanges (trades → netflow)."""

    async def _fetch_raw_trades(self, base):
        """Return list of dicts: {ts_ms, price, qty, side} ('buy'/'sell')."""
        raise NotImplementedError

    async def get_15min_netflow_usd(self, base):
        trades = await self._fetch_raw_trades(base)
        now_ms = time.time() * 1000
        cutoff = now_ms - 15 * 60 * 1000
        buy_usd = sell_usd = 0.0
        for t in trades:
            if t["ts_ms"] < cutoff:
                continue
            val = t["price"] * t["qty"]
            if t["side"] == "buy":
                buy_usd += val
            else:
                sell_usd += val
        return buy_usd - sell_usd


# ====================================================================
#  Binance Spot — Tier A (kline with taker buy volume)
# ====================================================================
class BinanceSpot(BaseSpotExchange):
    name = "Binance"
    BASE = "https://api.binance.com"

    async def load_spot_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/api/v3/exchangeInfo")
        self._spot_bases = {
            s["baseAsset"].upper()
            for s in data["symbols"]
            if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
        }
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}USDT"

    async def get_15min_netflow_usd(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v3/klines",
            params={"symbol": self.format_symbol(base), "interval": "15m", "limit": "2"},
            sem=self._sem,
        )
        # data[0]=completed candle, data[1]=current incomplete
        candle = data[0]
        total_quote = float(candle[7])
        taker_buy_quote = float(candle[10])
        return 2 * taker_buy_quote - total_quote

    async def get_price_1h(self, base):
        """(price_now, price_1h_ago)"""
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v3/klines",
            params={"symbol": self.format_symbol(base), "interval": "15m", "limit": "5"},
            sem=self._sem,
        )
        if not data or len(data) < 2:
            return None, None
        return float(data[-1][4]), float(data[0][4])


# ====================================================================
#  OKX Spot — Tier B (trades, limit 500)
# ====================================================================
class OkxSpot(TradesSpotExchange):
    name = "OKX"
    BASE = "https://www.okx.com"

    async def load_spot_symbols(self):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v5/public/instruments",
            params={"instType": "SPOT"},
        )
        self._spot_bases = set()
        for inst in data.get("data", []):
            if inst.get("state") == "live" and inst.get("quoteCcy") == "USDT":
                self._spot_bases.add(inst["baseCcy"].upper())
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}-USDT"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v5/market/trades",
            params={"instId": self.format_symbol(base), "limit": "500"},
            sem=self._sem,
        )
        return [
            {
                "ts_ms": float(t["ts"]),
                "price": float(t["px"]),
                "qty": float(t["sz"]),
                "side": t["side"],  # "buy"/"sell"
            }
            for t in data.get("data", [])
        ]


# ====================================================================
#  Bybit Spot — Tier B (trades, hardcap 60)
# ====================================================================
class BybitSpot(TradesSpotExchange):
    name = "Bybit"
    BASE = "https://api.bybit.com"

    async def load_spot_symbols(self):
        data = await http_get(
            self.client,
            f"{self.BASE}/v5/market/instruments-info",
            params={"category": "spot"},
        )
        self._spot_bases = {
            s["baseCoin"].upper()
            for s in data["result"]["list"]
            if s["status"] == "Trading" and s["quoteCoin"] == "USDT"
        }
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}USDT"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/v5/market/recent-trade",
            params={"category": "spot", "symbol": self.format_symbol(base), "limit": "1000"},
            sem=self._sem,
        )
        return [
            {
                "ts_ms": float(t["time"]),
                "price": float(t["price"]),
                "qty": float(t["size"]),
                "side": t["side"].lower(),  # "Buy"→"buy"
            }
            for t in data["result"]["list"]
        ]


# ====================================================================
#  Bitget Spot — Tier B (trades, max 100)
# ====================================================================
class BitgetSpot(TradesSpotExchange):
    name = "Bitget"
    BASE = "https://api.bitget.com"

    async def load_spot_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/api/v2/spot/public/symbols")
        self._spot_bases = {
            s["baseCoin"].upper()
            for s in data["data"]
            if s["status"] == "online" and s["quoteCoin"] == "USDT"
        }
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}USDT"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v2/spot/market/fills",
            params={"symbol": self.format_symbol(base), "limit": "100"},
            sem=self._sem,
        )
        return [
            {
                "ts_ms": float(t["ts"]),
                "price": float(t["price"]),
                "qty": float(t["size"]),
                "side": t["side"],  # "buy"/"sell"
            }
            for t in data["data"]
        ]


# ====================================================================
#  HTX Spot — Tier B (trades, size=2000, nested batches)
# ====================================================================
class HtxSpot(TradesSpotExchange):
    name = "HTX"
    BASE = "https://api.huobi.pro"

    async def load_spot_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/v2/settings/common/symbols")
        self._spot_bases = set()
        for s in data["data"]:
            if s.get("state") == "online" and s.get("qc") == "usdt":
                self._spot_bases.add(s["bcdn"].upper())
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.lower()}usdt"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/market/history/trade",
            params={"symbol": self.format_symbol(base), "size": 2000},
            sem=self._sem,
        )
        results = []
        for batch in data.get("data", []):
            for t in batch.get("data", []):
                results.append({
                    "ts_ms": float(t["ts"]),
                    "price": float(t["price"]),
                    "qty": float(t["amount"]),
                    "side": t["direction"],  # "buy"/"sell"
                })
        return results


# ====================================================================
#  Gate Spot — Tier B (trades, limit=1000)
# ====================================================================
class GateSpot(TradesSpotExchange):
    name = "Gate"
    BASE = "https://api.gateio.ws"

    async def load_spot_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/api/v4/spot/currency_pairs")
        self._spot_bases = set()
        for p in data:
            if p.get("trade_status") == "tradable" and p.get("quote") == "USDT":
                self._spot_bases.add(p["base"].upper())
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}_USDT"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v4/spot/trades",
            params={"currency_pair": self.format_symbol(base), "limit": 1000},
            sem=self._sem,
        )
        return [
            {
                "ts_ms": float(t["create_time_ms"]),
                "price": float(t["price"]),
                "qty": float(t["amount"]),
                "side": t["side"],  # "buy"/"sell"
            }
            for t in data
        ]


# ====================================================================
#  MEXC Spot — Tier B (trades, max 201, isBuyerMaker)
# ====================================================================
class MexcSpot(TradesSpotExchange):
    name = "MEXC"
    BASE = "https://api.mexc.com"

    async def load_spot_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/api/v3/exchangeInfo")
        self._spot_bases = {
            s["baseAsset"].upper()
            for s in data["symbols"]
            if str(s.get("status")) == "1" and s.get("quoteAsset") == "USDT"
            and s.get("isSpotTradingAllowed")
        }
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}USDT"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v3/trades",
            params={"symbol": self.format_symbol(base), "limit": 1000},
            sem=self._sem,
        )
        return [
            {
                "ts_ms": float(t["time"]),
                "price": float(t["price"]),
                "qty": float(t["qty"]),
                # isBuyerMaker=false → taker is buy; true → taker is sell
                "side": "sell" if t.get("isBuyerMaker") else "buy",
            }
            for t in data
        ]


# ====================================================================
#  KuCoin Spot — Tier B (trades, fixed 100, nanosecond timestamps)
# ====================================================================
class KucoinSpot(TradesSpotExchange):
    name = "KuCoin"
    BASE = "https://api.kucoin.com"

    async def load_spot_symbols(self):
        data = await http_get(self.client, f"{self.BASE}/api/v1/symbols")
        self._spot_bases = set()
        for s in data.get("data", []):
            if s.get("enableTrading") and s.get("quoteCurrency") == "USDT":
                self._spot_bases.add(s["baseCurrency"].upper())
        logger.info(f"{self.name} Spot: {len(self._spot_bases)} USDT pairs")

    def format_symbol(self, base):
        return f"{base.upper()}-USDT"

    async def _fetch_raw_trades(self, base):
        data = await http_get(
            self.client,
            f"{self.BASE}/api/v1/market/histories",
            params={"symbol": self.format_symbol(base)},
            sem=self._sem,
        )
        return [
            {
                "ts_ms": float(t["time"]) / 1_000_000,  # nanoseconds → ms
                "price": float(t["price"]),
                "qty": float(t["size"]),
                "side": t["side"],  # "buy"/"sell"
            }
            for t in data.get("data", [])
        ]


# ====================================================================
#  Universe: Binance ∩ OKX perpetual intersection − excluded
# ====================================================================
async def get_universe(bf: BinanceFutures, okx: OkxSwap):
    b_syms, o_syms = await asyncio.gather(bf.get_perp_symbols(), okx.get_perp_symbols())
    universe = (b_syms & o_syms) - CONFIG["excluded_symbols"]
    return sorted(universe)


# ====================================================================
#  Netflow history update (one round)
# ====================================================================
async def update_netflow_history(universe, spot_clients):
    """Fetch 15min netflow for all symbols from all exchanges, update deques."""
    statuses = {}

    async def fetch_one_exchange(exchange):
        ok_count = fail_count = 0
        eligible = [b for b in universe if exchange.has_symbol(b)]
        if not eligible:
            statuses[exchange.name] = (0, 0)
            return

        async def fetch_one(base):
            nonlocal ok_count, fail_count
            try:
                nf = await exchange.get_15min_netflow_usd(base)
                netflow_history[base][exchange.name].append(nf)
                ok_count += 1
            except Exception as e:
                fail_count += 1
                logger.warning(f"{exchange.name} netflow {base}: {e}")

        await asyncio.gather(*[fetch_one(b) for b in eligible])
        statuses[exchange.name] = (ok_count, fail_count)

    await asyncio.gather(*[fetch_one_exchange(ex) for ex in spot_clients])
    return statuses


# ====================================================================
#  Per-symbol data fetch (OI, funding, price)
# ====================================================================
async def fetch_symbol_data(base, bf: BinanceFutures, okx: OkxSwap, bn_spot: BinanceSpot):
    results = await asyncio.gather(
        bf.get_oi_history(base),
        bf.get_funding_current(base),
        bf.get_funding_history(base, limit=3),
        okx.get_oi_history(base),
        okx.get_funding_current(base),
        okx.get_funding_history(base, limit=3),
        bn_spot.get_price_1h(base),
        return_exceptions=True,
    )
    # Unpack, replacing exceptions with defaults
    def safe(idx, default):
        v = results[idx]
        return default if isinstance(v, Exception) else v

    bn_oi_now, bn_oi_1h = safe(0, (None, None))
    bn_fr_cur = safe(1, None)
    bn_fr_hist = safe(2, [])
    ok_oi_now, ok_oi_1h = safe(3, (None, None))
    ok_fr_cur = safe(4, None)
    ok_fr_hist = safe(5, [])
    price_now, price_1h = safe(6, (None, None))

    return {
        "bn_oi_now": bn_oi_now, "bn_oi_1h": bn_oi_1h,
        "ok_oi_now": ok_oi_now, "ok_oi_1h": ok_oi_1h,
        "bn_fr_cur": bn_fr_cur, "bn_fr_hist": bn_fr_hist,
        "ok_fr_cur": ok_fr_cur, "ok_fr_hist": ok_fr_hist,
        "price_now": price_now, "price_1h": price_1h,
    }


# ====================================================================
#  Rule Engine
# ====================================================================
def rule_r1(d):
    """OI 异常增长: oi_growth >= 3 * |price_growth|, with minimum thresholds."""
    # Aggregate OI
    oi_now_parts = [v for v in [d["bn_oi_now"], d["ok_oi_now"]] if v is not None]
    oi_1h_parts = [v for v in [d["bn_oi_1h"], d["ok_oi_1h"]] if v is not None]
    if not oi_now_parts or not oi_1h_parts or len(oi_now_parts) != len(oi_1h_parts):
        return False, "", {}
    oi_now = sum(oi_now_parts)
    oi_1h = sum(oi_1h_parts)
    if oi_1h <= 0 or d["price_1h"] is None or d["price_1h"] <= 0:
        return False, "", {}

    oi_growth = (oi_now - oi_1h) / oi_1h
    oi_growth_usd = oi_now - oi_1h
    price_growth = (d["price_now"] - d["price_1h"]) / d["price_1h"]

    if oi_growth <= 0:
        return False, "", {}

    rules = CONFIG["rules"]
    ratio = rules["r1_oi_vs_price_ratio"]
    min_pct = rules["r1_oi_growth_min_pct"]
    min_usd = rules["r1_oi_growth_min_usd"]
    zero_price_usd = rules["r1_oi_growth_zero_price_usd"]

    triggered = False
    reason = ""

    if abs(price_growth) == 0:
        # 价格不变: 只看 OI 绝对增长是否 >= $150k
        if oi_growth_usd >= zero_price_usd:
            triggered = True
            reason = f"OI 1h +{oi_growth:.1%} (+${oi_growth_usd:,.0f}), 价格不变 (∞×)"
    elif oi_growth >= ratio * abs(price_growth):
        # 原比率逻辑 + 新增门槛: OI 增长率 >= 3.3% 且 OI 绝对增长 >= $50k
        if oi_growth >= min_pct and oi_growth_usd >= min_usd:
            mult = oi_growth / abs(price_growth)
            reason = (
                f"OI 1h +{oi_growth:.1%} (+${oi_growth_usd:,.0f}), "
                f"价格 {price_growth:+.1%} ({mult:.1f}×)"
            )
            triggered = True

    if not triggered:
        return False, "", {}

    extra = {
        "oi_now": oi_now, "oi_1h": oi_1h, "oi_growth": oi_growth,
        "oi_growth_usd": oi_growth_usd, "price_growth": price_growth,
        "bn_oi_now": d["bn_oi_now"], "ok_oi_now": d["ok_oi_now"],
        "bn_oi_1h": d["bn_oi_1h"], "ok_oi_1h": d["ok_oi_1h"],
    }
    return triggered, reason, extra


def rule_r2(d):
    """资金费率持续为负: 过去 3 期 (24h) 全部 < 0"""
    n = CONFIG["rules"]["r2_negative_funding_periods"]
    sources = []
    if len(d["bn_fr_hist"]) >= n and all(fr < 0 for fr in d["bn_fr_hist"][:n]):
        sources.append("Binance")
    if len(d["ok_fr_hist"]) >= n and all(fr < 0 for fr in d["ok_fr_hist"][:n]):
        sources.append("OKX")
    if not sources:
        return False, "", {}
    reason = f"过去 {n} 期资金费率全负 ({', '.join(sources)})"
    return True, reason, {"sources": sources}


def rule_r3(d):
    """资金费率极值: 当前 FR <= threshold"""
    threshold = CONFIG["rules"]["r3_funding_rate_threshold"]
    hits = []
    if d["bn_fr_cur"] is not None and d["bn_fr_cur"] <= threshold:
        hits.append(("Binance", d["bn_fr_cur"]))
    if d["ok_fr_cur"] is not None and d["ok_fr_cur"] <= threshold:
        hits.append(("OKX", d["ok_fr_cur"]))
    if not hits:
        return False, "", {}
    parts = [f"{name} {fr:.3%}" for name, fr in hits]
    reason = f"资金费率 {' / '.join(parts)} ≤ {threshold:.2%}"
    return True, reason, {"hits": hits}


def rule_r4(base):
    """现货净流异常: 8 家聚合 1h |netflow| >= threshold"""
    threshold = CONFIG["rules"]["r4_netflow_threshold_usd"]
    min_ex = CONFIG["netflow_min_exchanges"]
    per_exchange_1h = {}
    for ex_name, windows in netflow_history[base].items():
        if len(windows) < CONFIG["netflow_window_count"]:
            continue
        per_exchange_1h[ex_name] = sum(windows)
    if len(per_exchange_1h) < min_ex:
        return False, f"数据不足({len(per_exchange_1h)}/{min_ex}家)", per_exchange_1h
    total_1h = sum(per_exchange_1h.values())
    if abs(total_1h) >= threshold:
        direction = "净流入" if total_1h > 0 else "净流出"
        reason = f"8家聚合{direction} ${abs(total_1h):,.0f}"
        return True, reason, per_exchange_1h
    return False, "", per_exchange_1h


def rule_r5(d):
    """OI 异常增长 + 正资金费率: 多头主导的积极拉升信号。"""
    # --- OI growth check (same logic as R1) ---
    oi_now_parts = [v for v in [d["bn_oi_now"], d["ok_oi_now"]] if v is not None]
    oi_1h_parts = [v for v in [d["bn_oi_1h"], d["ok_oi_1h"]] if v is not None]
    if not oi_now_parts or not oi_1h_parts or len(oi_now_parts) != len(oi_1h_parts):
        return False, "", {}
    oi_now = sum(oi_now_parts)
    oi_1h = sum(oi_1h_parts)
    if oi_1h <= 0:
        return False, "", {}

    oi_growth = (oi_now - oi_1h) / oi_1h
    oi_growth_usd = oi_now - oi_1h

    if oi_growth <= 0:
        return False, "", {}

    rules = CONFIG["rules"]
    min_pct = rules["r5_oi_growth_min_pct"]
    min_usd = rules["r5_oi_growth_min_usd"]
    fr_threshold = rules["r5_positive_funding_threshold"]

    if oi_growth < min_pct or oi_growth_usd < min_usd:
        return False, "", {}

    # --- Positive funding check ---
    fr_values = [v for v in [d["bn_fr_cur"], d["ok_fr_cur"]] if v is not None]
    if not fr_values:
        return False, "", {}
    avg_fr = sum(fr_values) / len(fr_values)
    if avg_fr < fr_threshold:
        return False, "", {}

    reason = (
        f"OI 1h +{oi_growth:.1%} (+${oi_growth_usd:,.0f}), "
        f"正费率 {avg_fr:.3%} (多头主导)"
    )
    extra = {
        "oi_now": oi_now, "oi_1h": oi_1h, "oi_growth": oi_growth,
        "oi_growth_usd": oi_growth_usd, "avg_funding_rate": avg_fr,
    }
    return True, reason, extra


def evaluate(base, data):
    r1_hit, r1_reason, r1_extra = rule_r1(data)
    r2_hit, r2_reason, r2_extra = rule_r2(data)
    r3_hit, r3_reason, r3_extra = rule_r3(data)
    r4_hit, r4_reason, r4_extra = rule_r4(base)
    r5_hit, r5_reason, r5_extra = rule_r5(data)

    # Current round hits
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

    # --- Signal memory: 2h sliding window ---
    if current_round_tags:
        signal_memory[base].append((round_count, frozenset(current_round_tags)))

    # Compute recent_rules: union of all rule tags in the memory window
    recent_rules = set()
    for _rnd, tags in signal_memory[base]:
        recent_rules.update(tags)

    # --- Scoring ---
    scoring_cfg = CONFIG["scoring"]
    base_scores = scoring_cfg["rule_base_scores"]
    score = sum(base_scores.get(tag, 0) for tag in recent_rules)
    if len(recent_rules) >= 2:
        score *= scoring_cfg["multi_rule_multiplier"] ** (len(recent_rules) - 1)

    # --- Level (based on highest-severity in recent_rules) ---
    if not recent_rules:
        level = "🟢"
    elif "R3" in recent_rules:
        level = "🔴"
    elif recent_rules & {"R1", "R2", "R5"}:
        level = "🟠"
    else:
        level = "🟡"

    # --- Should push? ---
    # Only push if this round has at least one active hit (avoid re-pushing
    # purely from memory when nothing new triggered this round)
    should_push = False
    if current_round_tags:
        if level in scoring_cfg["push_override_levels"]:
            should_push = True       # 🔴 always pushes
        elif len(recent_rules) >= scoring_cfg["push_min_rules"]:
            should_push = True       # multi-rule fusion → push

    # Compute netflow total for display
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
        "current_round_hits": sorted(current_round_tags),
        "should_push": should_push,
    }


# ====================================================================
#  Level change detection
# ====================================================================
LEVEL_ORDER = {"🟢": 0, "🟡": 1, "🟠": 2, "🔴": 3}


def detect_level_change(base, new_level):
    old = last_levels.get(base, "🟢")
    old_val = LEVEL_ORDER.get(old, 0)
    new_val = LEVEL_ORDER.get(new_level, 0)
    if new_val > old_val:
        return "⬆️⬆️"
    elif new_val < old_val:
        return "⬇️"
    return ""


# ====================================================================
#  Telegram push
# ====================================================================
LEVEL_LABELS = {"🔴": "行动信号", "🟠": "预警信号", "🟡": "关注信号"}


def _fmt_usd(val):
    if val is None:
        return "N/A"
    abs_v = abs(val)
    sign = "+" if val >= 0 else "-"
    if abs_v >= 1e9:
        return f"{sign}${abs_v / 1e9:.1f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v / 1e6:.0f}M"
    if abs_v >= 1e3:
        return f"{sign}${abs_v / 1e3:.0f}k"
    return f"{sign}${abs_v:.0f}"


def format_tg_message(base, result, data, level_change):
    lv = result["level"]
    label = LEVEL_LABELS.get(lv, "信号")
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    score = result.get("score", 0)
    recent_rules = result.get("recent_rules", [])
    current_hits = result.get("current_round_hits", [])

    price_str = f"${data['price_now']:.4f}" if data["price_now"] else "N/A"
    price_pct = ""
    if data["price_now"] and data["price_1h"]:
        pct = (data["price_now"] - data["price_1h"]) / data["price_1h"]
        price_pct = f"  1h: {pct:+.1%}"

    # Header with score
    lines = [
        f"{lv} <b>{label}</b> | <b>{base}</b> | ⚡{score}分",
        "━━━━━━━━━━━━━━━━━",
        f"💰 现价: <code>{price_str}</code>{price_pct}",
    ]

    # Signal fusion info
    if recent_rules:
        recent_str = ", ".join(recent_rules)
        current_str = ", ".join(current_hits) if current_hits else "—"
        lines.append(f"📡 2h 内触发: {recent_str} (本轮: {current_str})")
    lines.append("")

    # Rules hit
    if result["hits"]:
        lines.append("🎯 <b>命中规则</b>")
        for tag, reason in result["hits"]:
            lines.append(f"- {tag}: {reason}")
        lines.append("")

    # OI (from R1 or R5)
    r1x = result.get("r1_extra", {})
    r5x = result.get("r5_extra", {})
    oi_extra = r1x if r1x.get("oi_now") is not None else r5x
    if oi_extra.get("oi_now") is not None:
        lines.append("📊 <b>OI</b>")
        lines.append(f"- 总: {_fmt_usd(oi_extra.get('oi_now'))} ← {_fmt_usd(oi_extra.get('oi_1h'))} (1h 前)")
        bn_oi = _fmt_usd(oi_extra.get("bn_oi_now"))
        ok_oi = _fmt_usd(oi_extra.get("ok_oi_now"))
        if bn_oi != "N/A" or ok_oi != "N/A":
            lines.append(f"- Binance: {bn_oi} | OKX: {ok_oi}")
        lines.append("")

    # Funding rates
    bn_fr = data.get("bn_fr_cur")
    ok_fr = data.get("ok_fr_cur")
    if bn_fr is not None or ok_fr is not None:
        lines.append("📈 <b>资金费率</b>")
        parts = []
        if bn_fr is not None:
            parts.append(f"Binance: {bn_fr:.3%}")
        if ok_fr is not None:
            parts.append(f"OKX: {ok_fr:.3%}")
        lines.append(f"- {' | '.join(parts)}")
        # R5: show positive funding note
        if r5x.get("avg_funding_rate") is not None:
            lines.append(f"- 🟢 正费率均值: {r5x['avg_funding_rate']:.3%} (多头主导)")
        lines.append("")

    # Netflow table
    r4x = result.get("r4_extra", {})
    if r4x:
        lines.append("💵 <b>现货净流(1h · USD)</b>")
        ex_order = ["Binance", "OKX", "Bybit", "Bitget", "HTX", "Gate", "MEXC", "KuCoin"]
        rows = []
        for i in range(0, len(ex_order), 2):
            left_name = ex_order[i]
            right_name = ex_order[i + 1] if i + 1 < len(ex_order) else ""
            left_val = _fmt_usd(r4x.get(left_name)) if left_name in r4x else "  -  "
            right_val = _fmt_usd(r4x.get(right_name)) if right_name in r4x else "  -  "
            rows.append(f" {left_name:<9}{left_val:>10}   {right_name:<8}{right_val:>10}")
        rows.append(" ─────────────────────────────────")
        total = _fmt_usd(result["total_1h_netflow"])
        rows.append(f" 合计 {total:>10}")
        lines.append("<pre>" + "\n".join(rows) + "</pre>")
        lines.append("")

    # Links
    lines.append(
        f'🔗 <a href="https://www.binance.com/en/futures/{base}USDT">Binance 永续</a>'
        f' | <a href="https://www.coinglass.com/currencies/{base}">Coinglass</a>'
    )

    # Timestamp + level change
    suffix = f"· 等级升级 {level_change}" if "⬆" in level_change else (
        f"· 等级降级 {level_change}" if "⬇" in level_change else ""
    )
    lines.append(f"<i>{now_str} {suffix}</i>")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "..."
    return msg


async def send_tg_alert(message, http):
    if not TG_TOKEN or not TG_CHAT_ID:
        logger.warning("TG credentials not configured, skip push")
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            r = await http.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return True
            logger.warning(f"TG push HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logger.warning(f"TG push error (attempt {attempt + 1}): {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error("TG push final failure")
    return False


# ====================================================================
#  Log alert as JSON (for post-analysis)
# ====================================================================
def log_alert_json(base, result, data):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": base,
        "level": result["level"],
        "hits": result["hits"],
        "score": result.get("score", 0),
        "recent_rules": result.get("recent_rules", []),
        "current_round_hits": result.get("current_round_hits", []),
        "should_push": result.get("should_push", False),
        "price_now": data.get("price_now"),
        "price_1h": data.get("price_1h"),
        "bn_fr_cur": data.get("bn_fr_cur"),
        "ok_fr_cur": data.get("ok_fr_cur"),
        "bn_oi_now": data.get("bn_oi_now"),
        "ok_oi_now": data.get("ok_oi_now"),
        "netflow_1h": result["total_1h_netflow"],
        "netflow_per_exchange": result.get("r4_extra", {}),
    }
    logger.warning(json.dumps(record, ensure_ascii=False))


# ====================================================================
#  Main loop
# ====================================================================
async def monitor_once(http, bf, okx_swap, spot_clients):
    global round_count
    round_count += 1
    t0 = time.time()
    bn_spot = spot_clients[0]  # BinanceSpot is always first

    # 1. Universe
    universe = await get_universe(bf, okx_swap)
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] 第 {round_count} 轮开始 | 币种池 {len(universe)}")
    logger.info(f"Round {round_count} start: {len(universe)} symbols")

    # 2. Update netflow
    nf_statuses = await update_netflow_history(universe, spot_clients)
    status_parts = []
    for ex in spot_clients:
        ok_c, fail_c = nf_statuses.get(ex.name, (0, 0))
        mark = "✓" if fail_c == 0 else f"✗({fail_c})"
        status_parts.append(f"{ex.name} {mark}")
    ts2 = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts2}] {' | '.join(status_parts)}")

    # 3. Fetch symbol data & evaluate (parallel, bounded concurrency)
    sem = asyncio.Semaphore(20)
    push_alerts_by_level = {"🔴": [], "🟠": [], "🟡": []}
    log_only_count = 0

    async def process_one(base):
        nonlocal log_only_count
        async with sem:
            try:
                data = await fetch_symbol_data(base, bf, okx_swap, bn_spot)
                result = evaluate(base, data)
                # Only act on coins with current-round hits (not purely from memory)
                if result["current_round_hits"]:
                    level_change = detect_level_change(base, result["level"])
                    # Always log to file
                    log_alert_json(base, result, data)
                    last_levels[base] = result["level"]
                    # Accumulate for 6h summary (richer info)
                    summary_alerts.append({
                        "symbol": base,
                        "level": result["level"],
                        "hits": result["hits"],
                        "score": result["score"],
                        "recent_rules": result["recent_rules"],
                        "current_round_hits": result["current_round_hits"],
                        "should_push": result["should_push"],
                    })
                    # Split: push to TG only if should_push
                    if result["should_push"]:
                        push_alerts_by_level[result["level"]].append(
                            (base, result, data, level_change)
                        )
                    else:
                        log_only_count += 1
                        logger.info(
                            f"{base} 单规则触发 {result['current_round_hits']} "
                            f"score={result['score']} → 仅记录"
                        )
            except Exception as e:
                logger.exception(f"Processing {base} failed: {e}")

    await asyncio.gather(*[process_one(b) for b in universe])

    # 4. Push to TG (sorted by score within each level, 3.5s between messages)
    total_pushed = 0
    for level in ["🔴", "🟠", "🟡"]:
        # Sort by score descending within each level
        push_alerts_by_level[level].sort(key=lambda x: x[1]["score"], reverse=True)
        for base, result, data, level_change in push_alerts_by_level[level]:
            msg = format_tg_message(base, result, data, level_change)
            await send_tg_alert(msg, http)
            total_pushed += 1
            if total_pushed < 20:
                await asyncio.sleep(3.5)
            else:
                logger.warning("TG rate limit: >20 alerts this round, stopping push")
                break
        else:
            continue
        break

    # 5. Status line
    elapsed = time.time() - t0
    parts = []
    for lv in ["🔴", "🟠", "🟡"]:
        n = len(push_alerts_by_level[lv])
        if n > 0:
            names = " ".join(a[0] for a in push_alerts_by_level[lv][:3])
            lvl_change_marks = " ".join(a[3] for a in push_alerts_by_level[lv][:3] if a[3])
            parts.append(f"{lv}{n}({names}{' ' + lvl_change_marks if lvl_change_marks else ''})")
    alert_summary = " | ".join(parts) if parts else "无告警"
    ts3 = datetime.now().strftime("%H:%M:%S")
    print(
        f"[{ts3}] 本轮结束 | 耗时 {elapsed:.0f}s | {alert_summary} | "
        f"TG 推送 {total_pushed} 条, 仅记录 {log_only_count} 条"
    )
    logger.info(
        f"Round {round_count} done: {total_pushed} pushed, "
        f"{log_only_count} log-only, {elapsed:.1f}s"
    )


# ====================================================================
#  Clock-aligned sleep (snap to 00/15/30/45 minute boundaries)
# ====================================================================
async def sleep_until_next_boundary():
    """Sleep until the next 15-min clock boundary (xx:00, xx:15, xx:30, xx:45)."""
    now = time.time()
    interval = CONFIG["loop_interval_seconds"]
    # Next boundary = ceiling of current time to interval
    next_boundary = (int(now) // interval + 1) * interval
    wait = next_boundary - now
    if wait < 10:
        # Already very close to boundary, skip to next one
        wait += interval
    ts = datetime.now().strftime("%H:%M:%S")
    next_ts = datetime.fromtimestamp(next_boundary).strftime("%H:%M:%S")
    print(f"[{ts}] Sleep until {next_ts} ({wait:.0f}s)...")
    await asyncio.sleep(wait)


# ====================================================================
#  6-hour summary (push to secondary TG group)
# ====================================================================
async def send_tg_summary(message, http):
    """Send summary to the secondary TG bot/group."""
    token = TG_SUMMARY_TOKEN or TG_TOKEN
    chat_id = TG_SUMMARY_CHAT_ID
    if not token or not chat_id:
        logger.warning("Summary TG credentials not configured, skip")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message[:4000] + ("..." if len(message) > 4000 else ""),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            r = await http.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return True
            logger.warning(f"Summary TG push HTTP {r.status_code}")
        except Exception as e:
            logger.warning(f"Summary TG push error (attempt {attempt + 1}): {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error("Summary TG push final failure")
    return False


async def maybe_send_summary(http):
    """Check if we've crossed a 6h boundary, if so send summary.

    Ranking: prioritize coins with diverse rule types (not just raw count).
    Sort by: 1) distinct rule count desc, 2) max score desc, 3) push count desc.
    """
    global last_summary_hour, summary_alerts
    now = datetime.now(timezone.utc)
    current_hour = now.hour
    # 6h boundaries: 0, 6, 12, 18
    summary_boundary = (current_hour // 6) * 6
    if summary_boundary == last_summary_hour:
        return  # Not time yet
    if last_summary_hour == -1:
        # First run, just initialize
        last_summary_hour = summary_boundary
        return

    # Time to send summary
    if not summary_alerts:
        msg = (
            f"📋 <b>6h 数据总结</b> ({last_summary_hour:02d}:00 ~ {summary_boundary:02d}:00 UTC)\n"
            "━━━━━━━━━━━━━━━━━\n"
            "本时段无告警触发。"
        )
    else:
        # Count by level
        level_counts = {"🔴": 0, "🟠": 0, "🟡": 0}
        # Per-symbol aggregation
        sym_data: dict[str, dict] = {}
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
            # Collect all distinct rule tags ever seen for this symbol
            for tag, _ in alert["hits"]:
                sd["distinct_rules"].add(tag)
            sd["max_score"] = max(sd["max_score"], alert.get("score", 0))
            if alert.get("should_push"):
                sd["push_count"] += 1
            # Track highest level
            if LEVEL_ORDER.get(lv, 0) > LEVEL_ORDER.get(sd["max_level"], 0):
                sd["max_level"] = lv

        # Sort: distinct rule count desc → max_score desc → push_count desc
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

    await send_tg_summary(msg, http)
    last_summary_hour = summary_boundary
    summary_alerts.clear()


async def main():
    http = httpx.AsyncClient(
        timeout=httpx.Timeout(20, connect=10),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        headers={"User-Agent": "PumpMonitor/4.0"},
    )

    bf = BinanceFutures(http)
    okx_swap = OkxSwap(http)

    spot_clients = [
        BinanceSpot(http),  # index 0 — also used for price
        OkxSpot(http),
        BybitSpot(http),
        BitgetSpot(http),
        HtxSpot(http),
        GateSpot(http),
        MexcSpot(http),
        KucoinSpot(http),
    ]

    # Load spot symbol lists (parallel)
    print("Loading spot symbol lists from 8 exchanges...")
    results = await asyncio.gather(
        *[c.load_spot_symbols() for c in spot_clients],
        return_exceptions=True,
    )
    for c, r in zip(spot_clients, results):
        if isinstance(r, Exception):
            logger.error(f"{c.name} symbol load failed: {r}")

    warmup_min = CONFIG["netflow_window_count"] * CONFIG["loop_interval_seconds"] // 60
    mem_window = CONFIG["scoring"]["memory_window_rounds"] * CONFIG["loop_interval_seconds"] // 60
    startup_msg = (
        f"✅ 监控脚本 v4 已启动\n"
        f"• 币种池: Binance ∩ OKX 永续交集\n"
        f"• 现货 Netflow: 8 家聚合\n"
        f"• 规则: R1-R5, 评分制 (≥2 规则才推送, 🔴 单条即推)\n"
        f"• 信号记忆窗口: {mem_window} 分钟\n"
        f"• R4 需要预热 {warmup_min} 分钟后才开始触发"
    )
    await send_tg_alert(startup_msg, http)
    print(f"Startup complete. R4 warmup: {warmup_min} min, memory: {mem_window} min. Starting monitor loop...")

    try:
        while True:
            try:
                await monitor_once(http, bf, okx_swap, spot_clients)
                # Check if 6h summary is due
                await maybe_send_summary(http)
            except Exception as e:
                logger.exception(f"Round failed: {e}")
                await asyncio.sleep(60)
                continue
            # Sleep until next 15-min clock boundary
            await sleep_until_next_boundary()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        try:
            await send_tg_alert("🛑 监控脚本已手动停止", http)
        except Exception:
            pass
        await http.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Ctrl+C] 退出完成")
