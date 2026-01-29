# poly_martmoney_query 使用说明

本目录实现了基于 `smartmoney_query_plan.md` 的「聪明钱」监控脚本，涵盖排行榜扫描、成交抓取、指标聚合与 CSV 持久化。以下说明面向希望复用脚本的同事，示例均使用官方 Data API，尽量减少自定义兜底逻辑。

## 目录结构
- `api_client.py`：Data API 客户端，支持 leaderboard 分页扫描与按时间截断的成交翻页获取。
- `models.py`：Trade、MarketAggregation、AggregatedStats 数据模型与字段规范。
- `processors.py`：交易过滤、市场层聚合与胜率/PNL/成交量统计。
- `storage.py`：CSV 工具，支持成交明细去重追加、市场统计与摘要写盘。
- `__init__.py`：便捷导出。

## 环境依赖
- Python 3.9+，第三方依赖仅包含 `requests`。
- 可通过环境变量调整请求节奏：
  - `SMART_QUERY_MAX_BACKOFF`：指数回退的最大等待秒数，默认 60。
  - `SMART_QUERY_MAX_RPS`：全局限速（每秒请求数），默认 2。

## 快速开始
下面示例演示从排行榜发现用户、抓取成交、按时间窗口聚合并写入 CSV：

```bash
pip install requests
```

```python
import datetime as dt
from pathlib import Path

from poly_martmoney_query.api_client import DataApiClient
from poly_martmoney_query.processors import aggregate_markets
from poly_martmoney_query.storage import append_trades_csv, write_market_stats_csv

client = DataApiClient()

# 1) 扫描 leaderboard（ALL 周期，按成交量排序），取前 200 个用户
leaderboard_users = []
for item in client.iter_leaderboard(period="ALL", order_by="vol", page_size=100, max_pages=2):
    addr = item.get("proxyWallet") or item.get("address")
    if addr:
        leaderboard_users.append(addr)

# 2) 针对单个地址抓取成交，并按时间截断
user = leaderboard_users[0]
start = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=30)
trades = client.fetch_trades(user=user, start_time=start)

# 3) 聚合市场表现（若有结算结果，可在 resolutions 中填入 market_id -> outcome）
stats = aggregate_markets(trades, user=user, start_time=start, end_time=None, resolutions={})

# 4) 写入 CSV：成交明细与市场聚合
append_trades_csv(Path("data/trades_raw.csv"), trades)
write_market_stats_csv(Path("data/market_stats.csv"), stats)
```

## 常见用法
- **按时间窗口重算胜率/PNL**：调用 `aggregate_markets` 时传入 `start_time`/`end_time`，函数会先过滤交易再计算汇总。
- **增量抓取**：`fetch_trades` 内置 offset 翻页，可结合本地最新成交时间作为 `start_time` 做“截断式”更新。
- **调节请求节奏**：无需改代码，设置环境变量即可控制全局限速与回退等待。
- **数据落盘格式**：
  - `append_trades_csv` 以 `tx_hash` 去重写入标准字段，便于后续增量同步。
  - `write_market_stats_csv` 生成市场级明细与 `_summary` 汇总两份文件，便于排名或看板展示。

## VPS 场景下的运行步骤（直接执行脚本）
若需要在没有 IDE 的 VPS 上直接运行，可按以下顺序在仓库根目录操作：

1. **准备虚拟环境与依赖**（推荐隔离）：
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip requests
   ```

2. **设置环境变量（可选）**：根据带宽与 API 限流情况调整节奏，示例为将最大回退 30 秒、限速 3 RPS：
   ```bash
   export SMART_QUERY_MAX_BACKOFF=30
   export SMART_QUERY_MAX_RPS=3
   ```

3. **直接执行示例脚本**：仓库根目录已提供 `poly_martmoney_query_run.py`，默认抓取「盈利榜（MONTH）」前 50 名并统计近 30 天数据，结果写入当前目录的 `data/` 下：
   ```bash
   python poly_martmoney_query_run.py
   ```

4. **查看输出**：
   ```bash
   ls -lh data
   head -n 5 data/market_stats_summary.csv
   ```

上述流程适合在 VPS 里开一个临时 `tmux`/`screen` 会话直接执行；若要定时运行，可直接调用 `python poly_martmoney_query_run.py` 并配合 `cron` 或 `systemd` 定时。

## 与参考材料的衔接
仓库中的《POLYMARKET_MAKER_REVERSE-原版代码参考材料》提供了构建连接、成交查询、条件筛选等已验证逻辑。本套脚本在请求节奏控制与数据解析上延续了原版做法，可直接替换或嵌入到原有自动化流程中，仅需按上述示例导入对应函数即可。

## 运行自检
如需快速校验代码可运行：

```bash
python -m compileall -q poly_martmoney_query
```

若命令通过则表示当前 Python 环境能成功解析本目录脚本。

## 常用统计口径调整
`poly_martmoney_query_run.py` 支持通过参数切换排行榜口径与统计窗口，常见需求如下：

- **从盈利榜改为成交量榜**：将 `--order-by` 改成 `vol`（Data API 识别为 `VOL`）。
  ```bash
  python poly_martmoney_query_run.py --order-by vol
  ```
- **调整 leaderboard 周期**：将 `--period` 改成你需要的口径（例如 `ALL` / `MONTH` 等）。
  ```bash
  python poly_martmoney_query_run.py --period ALL
  ```
- **调整统计窗口天数**：`--days` 控制 summary 的统计区间，默认 30 天。
  ```bash
  python poly_martmoney_query_run.py --days 7
  ```
- **只统计单地址**：传入 `--user` 会跳过 leaderboard，直接查询该地址。
  ```bash
  python poly_martmoney_query_run.py --user 0x1234...
  ```

输出的 `users_summary.csv`/`summary.csv` 已拆分为：
- `closed_realized_pnl_sum`：已平仓已实现盈亏。
- `open_realized_pnl_sum`：当前持仓内已实现盈亏。
- `open_unrealized_pnl_sum`：当前持仓未实现浮盈浮亏。
