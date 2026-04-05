# TQQQ-Only 整体重构设计

## 目标

从第一性原理出发，将项目从多标的架构重构为 TQQQ 专用架构。重新划分模块职责为清晰的三层分离（CLI 薄壳 → 业务逻辑 → 输出），拆分臃肿的 `data_store.py`，消除重复代码。

**不可动摇的底线**：HTML 报告的完整功能保持不变。

## 约束

- 保留 DuckDB 作为本地存储
- 所有现有功能保留（数据同步、IV、指标、决策树、回测、期权匹配、熔断、报告、部署、通知）
- `template.html` 不修改
- 回测到期周数为 4 周（`EXPIRY_WEEKS = 4`）

## 架构总览

```
config.py                # 全局常量（TQQQ 硬编码）

cli/                     # 薄壳入口
  run.py                 # 策略生成 CLI
  deploy.py              # 部署 CLI
  sync.py                # 数据同步 CLI

data/                    # 数据层
  store.py               # DuckDB 连接 + 初始化
  schema.py              # 建表 DDL + 迁移
  queries.py             # 所有 SELECT 操作
  writers.py             # 所有 INSERT/UPSERT + 拆股因子
  sync/                  # 数据获取
    orchestrator.py      # ensure_synced 编排
    equity.py            # REST 股票日K
    options.py           # S3 期权（合并 s3_downloader + flat_file_fetcher）
    splits.py            # 拆股检测
    iv.py                # IV 计算

core/                    # 业务逻辑层
  indicators.py          # MA/MACD/Pivot
  strategy.py            # 分层决策树 + 周分组
  backtest.py            # 回测引擎 + 期权 enrichment
  options.py             # OCC 解析 + 期权合约匹配
  circuit_breaker.py     # 熔断逻辑

output/                  # 输出层
  report.py              # JSON 生成 + HTML 嵌入
  deploy.py              # 密码包装 + Cloudflare + Telegram
  template.html          # 模板（不修改）

tests/                   # 测试（镜像源码结构）
  conftest.py
  data/
    test_store.py
    test_schema.py
    test_queries.py
    test_writers.py
    sync/
      test_orchestrator.py
      test_equity.py
      test_options.py
      test_splits.py
      test_iv.py
  core/
    test_indicators.py
    test_strategy.py
    test_backtest.py
    test_options.py
    test_circuit_breaker.py
  output/
    test_report.py
    test_deploy.py
```

## Section 1：全局常量

`config.py` 集中管理所有硬编码常量，去掉所有函数签名中的 `ticker` 参数传递。

```python
TICKER = "TQQQ"
LEVERAGE = 3
EXPIRY_WEEKS = 4

DEFAULT_OTM = {
    "A": 8, "B1": 10, "B2": 12, "B3": 14, "B4": 16,
    "C1": 16, "C2": 17, "C3": 18, "C4": 20,
}

DB_PATH = "output/market_data.duckdb"

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"
```

删除项：
- `strategy.py` 的 `get_otm_for_ticker(ticker)` — 直接用 `DEFAULT_OTM`
- `strategy.py` 的 `LEVERAGE_MAP` 字典 — 直接用 `LEVERAGE = 3`
- 所有模块的 `ticker` 参数 — 通过 `from config import TICKER` 获取

## Section 2：数据层

### 2.1 存储层拆分（data_store.py 739行 → 4个模块）

**`data/store.py`（~30行）** — 连接管理：
- `get_connection()` — 返回 DuckDB 连接
- `init_db()` — 调用 schema 建表 + 迁移，程序启动时调用一次

**`data/schema.py`（~100行）** — DDL + 迁移：
- `create_tables(conn)` — equity_bars, option_bars, sync_log, splits, ticker_iv
- `run_migrations(conn)` — 列补全等迁移逻辑

**`data/queries.py`（~200行）** — 所有读操作：
- `query_equity_bars(from_date, to_date)` — 不再需要 ticker 参数
- `query_option_on_date(entry_date, expiry_date, strike)`
- `query_ticker_iv(from_date, to_date)`
- `get_latest_equity_date()`
- `get_sync_log(data_type)` / `check_month_synced(month)`
- `compute_split_factor(date_str)`

**`data/writers.py`（~300行）** — 所有写操作：
- `upsert_equity_bars(rows)`
- `insert_option_bars_from_csv(csv_path, date_str)` — 不再需要 tickers 过滤
- `upsert_ticker_iv(rows)`
- `upsert_splits(rows)`
- `write_sync_log(data_type, date_str)`
- `delete_all_data()` — 拆股时清空重拉（TQQQ-only 不需要按 ticker 删）

### 2.2 数据同步层

**`data/sync/orchestrator.py`（~80行）** — `ensure_synced(api_key)` 编排：
```
splits检测 → 有新拆股则清空重拉 → equity增量同步 → options按月同步 → IV增量计算
```
不再接受 `tickers` 参数，内部直接用 `config.TICKER`。

**`data/sync/equity.py`（~100行）** — 从 `rest_downloader.py` 提取：
- `download_and_store_equity(from_date, to_date, api_key)`
- 保留 429 重试逻辑

**`data/sync/options.py`（~250行）** — 合并 `s3_downloader.py` + `flat_file_fetcher.py`：
- `make_s3_client()` — S3 客户端创建
- `download_day_file(date_str, s3_client)` — 单日下载+缓存
- `sync_options(from_date, to_date, s3_client)` — 按月同步，producer/consumer 线程模式
- 消除重复的 S3 client 创建逻辑

**`data/sync/splits.py`（~50行）** — 从 `rest_downloader.py` 提取：
- `download_splits(api_key)` — 拉取拆股数据，返回新记录

**`data/sync/iv.py`（~180行）** — 合并 `iv.py` + `data_sync.py` IV 部分：
- `bs_implied_vol(price, spot, strike, tte, r, option_type)` — B-S 反算
- `compute_ticker_iv(option_bars, spot, date)` — 30天 ATM 包夹插值
- `sync_ticker_iv()` — 增量计算编排
- 依赖 `core.options.parse_occ_symbol` 做 OCC 解析（唯一跨层引用）

## Section 3：业务逻辑层

**`core/indicators.py`（~50行）** — 基本不变：
- `add_ma(df)`, `add_macd(df)`, `add_dynamic_pivot(df)`
- 纯函数，无外部依赖

**`core/strategy.py`（~120行）** — 决策树 + 周分组：
- `classify_tier(row)` — 9级决策树，不变
- `group_by_week(df)` — 周分组，不变
- `find_expiry_date(entry_date)` — 去掉 `weeks` 参数，直接用 `config.EXPIRY_WEEKS`
- `TIER_NAMES` 字典
- 删除：`get_otm_for_ticker()`, `LEVERAGE_MAP`, `backtest_weeks()`, `compute_summary/tiers/latest()`

**`core/backtest.py`（~150行）** — 从 strategy.py + run.py 提取：
- `backtest_weeks(weekly_rows, daily_df)` — 分层→strike→到期价→结算指标
- `enrich_with_options(weeks, daily_df)` — 用真实期权合约补全
- `compute_summary(tiers)` — 胜率、平均OTM等
- `compute_tiers(tiers)` — 按层级分组统计
- `compute_latest(daily_df)` — 最新一周策略信号

**`core/options.py`（~120行）** — OCC 解析归一化：
- `parse_occ_symbol(symbol)` — 唯一实现（从 iv.py 移入）
- `match_option_contract(entry_date, expiry_date, strike)` — 封装查询 + OCC 解析

**`core/circuit_breaker.py`（~40行）** — 从 run.py 提取：
- `apply_circuit_breaker(weeks, daily_df)` — 连续亏损检测→标记 skip

## Section 4：输出层与 CLI 入口

### 输出层

**`output/report.py`（~100行）** — 从 run.py 提取：
- `build_report_data(tiers, summary, tier_stats, latest, market, iv_series, hv_series)` — 组装 JSON
- `save_json(data, path)` — 写 JSON 文件
- `render_html(data)` — 嵌入 template.html 生成自包含 HTML

**`output/deploy.py`（~250行）** — 从 deploy.py 平移：
- `wrap_with_password(html, password)` — 密码包装
- `deploy_to_cloudflare(html)` — Cloudflare Pages 上传
- `send_telegram(url)` — Telegram 通知（去掉 ticker 参数）

**`output/template.html`** — 完全不动。

### CLI 入口

**`cli/run.py`（~50行）**：
```python
def main():
    api_key = os.environ["MASSIVE_API_KEY"]
    init_db()
    ensure_synced(api_key)
    daily = query_equity_bars(...)
    daily = add_ma(add_macd(add_dynamic_pivot(daily)))
    weekly = group_by_week(daily)
    tiers = backtest_weeks(weekly, daily)
    tiers = enrich_with_options(tiers, daily)
    tiers = apply_circuit_breaker(tiers, daily)
    summary = compute_summary(tiers)
    latest = compute_latest(daily)
    market = build_market_snapshot(daily)
    data = build_report_data(tiers, summary, ...)
    save_json(data)
    render_html(data)
```

**`cli/deploy.py`（~20行）**：读 HTML → 密码包装 → Cloudflare → Telegram

**`cli/sync.py`（~20行）**：init_db → ensure_synced

### 调用方式

```bash
python -m cli.run        # 替代 python run.py
python -m cli.deploy     # 替代 python deploy.py
python -m cli.sync       # 替代 python data_sync.py
```

## Section 5：测试与依赖

### 测试迁移原则

- 测试逻辑不重写，只调整 import 路径和去掉 ticker 参数
- `weeks=3` 修正为引用 `EXPIRY_WEEKS`
- 合并模块的测试同步合并

### 模块依赖方向

```
cli/  →  core/  →  data/queries  →  data/store
 │        │                           ↑
 │        ↓                           │
 │    data/queries                data/schema
 │                                    │
 ↓                                    ↓
output/  ←── (只读 template.html)   data/writers
```

约束：
- `core/` 只通过 `data/queries` 读数据，不直接操作连接
- `output/` 不依赖 `core/`，只接收组装好的数据字典
- `data/sync/iv.py` 引用 `core/options.parse_occ_symbol`（唯一跨层引用）
- `cli/` 是唯一知道完整流程的地方

### 文件清单与行数预估

| 新路径 | 来源 | 预估行数 |
|--------|------|----------|
| `config.py` | 新建 + strategy.py 常量 | ~30 |
| `data/store.py` | data_store.py 连接部分 | ~30 |
| `data/schema.py` | data_store.py DDL+迁移 | ~100 |
| `data/queries.py` | data_store.py 查询 | ~200 |
| `data/writers.py` | data_store.py 写入 | ~300 |
| `data/sync/orchestrator.py` | data_sync.py | ~80 |
| `data/sync/equity.py` | rest_downloader.py | ~100 |
| `data/sync/options.py` | s3_downloader + flat_file_fetcher | ~250 |
| `data/sync/splits.py` | rest_downloader.py 拆股部分 | ~50 |
| `data/sync/iv.py` | iv.py + data_sync.py IV 部分 | ~180 |
| `core/indicators.py` | indicators.py | ~50 |
| `core/strategy.py` | strategy.py（减去回测） | ~120 |
| `core/backtest.py` | strategy.py 回测 + run.py enrich | ~150 |
| `core/options.py` | iv.py OCC解析 + run.py 合约匹配 | ~120 |
| `core/circuit_breaker.py` | run.py 熔断部分 | ~40 |
| `output/report.py` | run.py JSON+HTML | ~100 |
| `output/deploy.py` | deploy.py | ~250 |
| `output/template.html` | template.html | 不变 |
| `cli/run.py` | run.py main 瘦身 | ~50 |
| `cli/deploy.py` | deploy.py main | ~20 |
| `cli/sync.py` | data_sync.py main | ~20 |
| **合计** | | **~2290** |

从 ~2700 行减至 ~2290 行，净减 ~15%。

## 旧文件处理

重构完成且测试通过后，删除根目录下的旧文件：
- `run.py`, `deploy.py`, `strategy.py`, `indicators.py`, `iv.py`
- `data_sync.py`, `data_store.py`, `s3_downloader.py`, `flat_file_fetcher.py`, `rest_downloader.py`
