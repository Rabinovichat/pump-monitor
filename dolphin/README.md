# DolphinScheduler 部署版

适配调度平台的单次运行版本，每次跑一轮后退出，状态持久化到 `state.json`。

## 与常驻版区别

| | 常驻版 (monitor.py) | 调度版 (dolphin/) |
|---|---|---|
| 运行模式 | while True 循环 | 跑一轮就退出 |
| 调度方式 | 自己 sleep 30min | DolphinScheduler 每 30min 触发 |
| 状态保存 | 内存 | state.json 文件 |
| 崩溃恢复 | 需要 systemd 重启 | 下次调度自动恢复 |

## DolphinScheduler 配置

1. 任务类型: **Shell**
2. 命令:
```bash
cd /path/to/pump-monitor && source .venv/bin/activate && cd dolphin && python monitor_once.py
```
3. 定时策略: `0 0/30 * * * ?` (每 30 分钟)
4. 失败重试: 2 次，间隔 1 分钟

## 文件说明

```
dolphin/
  monitor_once.py   # 主脚本 (导入上层 monitor.py 的交易所类和工具函数)
  state.json        # 运行时自动生成，保存跨轮次状态
  logs/             # 日志目录
```

## 注意

- `.env` 文件放在上层目录 (`pump-monitor/.env`)，脚本会自动读取
- `state.json` 不要提交到 git（已在 .gitignore 中排除）
- 首次运行时没有 state.json，会从零开始（R4 需要预热 2 轮 = 60 分钟）
