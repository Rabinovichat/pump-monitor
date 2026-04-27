# 加密货币拉升预警监控

每 15 分钟扫描一次 Binance + OKX 永续合约交集币种，识别"空头陷阱型拉升"信号，推送到 Telegram 群。

## 监控规则

| 编号 | 规则 | 触发条件 | 级别 |
|------|------|----------|------|
| R1 | OI 异常增长 | 1h OI 增长率 ≥ 3× 价格涨幅 | 🟠 预警 |
| R2 | 资金费率持续为负 | 过去 24h 所有结算均 < 0 | 🟠 预警 |
| R3 | 资金费率极值 | 当前资金费率 ≤ -0.05% | 🔴 行动 |
| R4 | 现货净流异常 | 8 家聚合 1h 净流入/出 ≥ $500k | 🟡 关注 |

## 数据源

- **衍生品**: Binance Futures + OKX Swap（OI、资金费率）
- **现货 Netflow**: Binance、OKX、Bybit、Bitget、HTX、Gate、MEXC、KuCoin（8 家聚合）

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 填入 Telegram Bot Token 和 Chat ID

# 启动
python monitor.py
```

## 获取 Telegram Bot Token 和 Chat ID

1. 在 TG 搜索 `@BotFather`，发送 `/newbot`，按提示创建机器人，获得 Bot Token
2. 创建一个 TG 群，把机器人拉进群并设为管理员
3. 在群里发一条消息，然后访问 `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. 在返回 JSON 中找到 `"chat":{"id":-100xxxxxxxxxx}`，这个负数就是 Chat ID

## 注意事项

- **R4 预热期**: 启动后前 45 分钟 R4 不会触发（需要累积 4 个 15min 窗口）
- **不去重**: 同一币种连续命中会连续推送，频繁告警本身传递"信号持续"的信息
- **日志**: `logs/monitor.log` 日级轮转，保留 30 天，含完整 JSON 格式告警记录
- **网络要求**: 需要能直连交易所 API，企业代理可能屏蔽相关域名

## 调参

修改 `monitor.py` 顶部的 `CONFIG` 字典：

```python
"rules": {
    "r1_oi_vs_price_ratio": 3.0,        # OI/价格倍率阈值
    "r2_negative_funding_periods": 3,    # 连续负费率期数（×8h=24h）
    "r3_funding_rate_threshold": -0.0005, # 资金费率极值阈值
    "r4_netflow_threshold_usd": 500_000, # 净流入/出 USD 阈值
}
```
