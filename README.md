# 加密货币拉升预警监控 v4

每 30 分钟扫描 Binance + OKX 永续合约交集币种，识别拉升信号，通过评分机制过滤噪音，推送到 Telegram 群。

## 监控规则

| 编号 | 规则 | 触发条件 | 级别 |
|------|------|----------|------|
| R1 | OI 异常增长 | 1h OI 增长率 ≥ 3× 价格涨幅，且 ≥3.3%、≥$50k | 🟠 预警 |
| R2 | 资金费率持续为负 | 过去 24h（3 期）所有结算均 < 0 | 🟠 预警 |
| R3 | 资金费率极值 | 当前资金费率 ≤ -0.05% | 🔴 行动 |
| R4 | 现货净流异常 | 8 家聚合 1h 净流入/出 ≥ $500k | 🟡 关注 |
| R5 | OI 增长 + 正费率 | OI 异常增长且正费率 ≥ +0.05%（多头主导） | 🟠 预警 |

## 评分与推送机制

单规则触发**只记日志不推 TG**，大幅降低噪音。

- **🔴 级别（R3）**: 单条即推送
- **其余规则**: 2h 窗口内 ≥2 条不同规则触发才推送
- **评分公式**: `总分 = 各规则基础分之和 × 1.5 ^ (规则数 - 1)`

| 规则 | 基础分 |
|------|--------|
| R1 | 3 |
| R2 | 2 |
| R3 | 5 |
| R4 | 2 |
| R5 | 3 |

示例：R1 + R2 触发 → (3+2) × 1.5 = **7.5 分**；R1 + R2 + R4 → (3+2+2) × 1.5² = **15.8 分**

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

- **R4 预热期**: 启动后前 60 分钟 R4 不会触发（需要累积 2 个 30min 窗口）
- **信号记忆**: 2h 滑动窗口关联不同轮次的规则，不要求同时触发
- **推送去噪**: 单规则仅记录日志，多规则联合触发或 🔴 级别才推送 TG
- **6h 总结**: 按规则多样性排序（而非简单计数），优先展示多信号交叉的币种
- **日志**: `logs/monitor.log` 日级轮转，保留 30 天，JSON 格式含评分和推送状态

## 调参

修改 `monitor.py` 顶部的 `CONFIG` 字典：

```python
"scoring": {
    "memory_window_rounds": 8,          # 信号记忆窗口（轮数）
    "push_min_rules": 2,                # 推送所需最低规则数
    "multi_rule_multiplier": 1.5,       # 多规则乘数
},
"rules": {
    "r1_oi_vs_price_ratio": 3.0,        # OI/价格倍率阈值
    "r2_negative_funding_periods": 3,    # 连续负费率期数（×8h=24h）
    "r3_funding_rate_threshold": -0.0005, # 资金费率极值阈值
    "r4_netflow_threshold_usd": 500_000, # 净流入/出 USD 阈值
    "r5_positive_funding_threshold": 0.0005, # 正费率阈值 +0.05%
}
```
