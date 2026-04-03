# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

基于股票的策略研究与交易项目，集成两个 API：
- **Futu OpenAPI**（富途）— 香港券商，提供 Python SDK，支持港股/美股/A股行情获取与交易
- **Massive API** — 金融数据平台，提供股票、加密货币、外汇、期权、指数的平文件和 REST 接口，支持纳秒级 tick 数据

## 项目结构

```
├── run.py               # 策略入口：拉取数据 → 策略计算 → JSON → HTML → 截图 PNG
├── strategy.py          # 策略核心：周分组、分层判定(A/B1-B4/C1-C4)、OTM推导、回测
├── indicators.py        # 技术指标（MA/MACD/Pivot）
├── template.html        # 可视化报告模板
│
├── data_sync.py         # 数据同步 CLI：ensure_synced 自动判断全量/增量
├── data_store.py        # DuckDB 本地存储：建表、写入、查询（option_bars / equity_bars）
├── s3_downloader.py     # S3 期权 Flat Files 按月下载并写入 DB（S3 客户端复用 flat_file_fetcher）
├── flat_file_fetcher.py # S3 单日文件下载/缓存（output/flat_files_cache/）
├── rest_downloader.py   # Massive REST API 股票日K下载并写入 DB
├── notify.py            # Telegram 截图推送：扫描 output/*.png → 发送 → 删除
├── com.lambda.scheduled-notify.plist  # macOS launchd 定时任务（每日自动运行策略+推送）
│
├── output/              # 运行产物（gitignore）
│   ├── market_data.duckdb        # 本地期权/股票数据库
│   ├── flat_files_cache/         # S3 原始 .csv.gz 本地缓存（按日期命名）
│   ├── {TICKER}.json             # 策略数据（含 daily_bars，供 HTML 生成用）
│   └── {TICKER}.html             # 可视化报告
├── tests/               # pytest 单元测试
├── requirements.txt     # Python 依赖
├── .venv/               # Python 虚拟环境（不提交）
└── docs/                # 文档 + API 参考
    ├── api/             # API 文档（Massive 等）
    ├── superpowers/     # 实现计划与设计文档
    ├── strategy-*.md/html  # 策略说明文档
    ├── scheduled-notify.md  # Telegram 定时推送配置说明
    └── entry-timing-research.md  # 入场限价优化研究报告
```

## 数据流

**策略报告（run.py）：**
```
ensure_synced() → DuckDB equity_bars → 指标计算 → 策略计算 → JSON(output/) → HTML
```

**本地数据库（data_sync.py）：**
```
S3 Flat Files (.csv.gz) → DuckDB option_bars   # 期权日K，按月批量写入
Massive REST API        → DuckDB equity_bars   # 股票日K
```

- `run.py` 每次运行先调用 `ensure_synced()`，空库同步近 2 年，有数据则增量补齐
- 增量同步基于 `sync_log` 表（月级，`data_type='option_month'`）

## API 文档

参考文档位于 `docs/api/`：
- `docs/api/massive-llms.txt` — Massive API 接口参考

## 开发说明

- 项目基于 Python（富途 SDK 为 Python）
- 调用富途 API 前需在本地启动 OpenD 网关进程；可使用 `install-opend` skill 进行安装配置
- 开发时可使用 `openapi` skill 快速查询富途交易/行情接口

## 常用命令

```bash
# 虚拟环境（首次需 python3 -m venv .venv && pip install -r requirements.txt）
source .venv/bin/activate

# ── Lambda 策略（Sell Put）──
python run.py              # 默认 TQQQ：自动同步 → 策略计算 → JSON → HTML → 截图 PNG
python run.py TQQQ QQQ     # 多标的批量处理
# 双击 {TICKER}.html 查看报告（数据已内嵌，无需服务器）

# 截图依赖 Playwright + Chromium（首次需安装，缺失时自动跳过不影响主流程）
# pip install playwright && playwright install chromium

# ── 本地数据库同步 ──
python data_sync.py                      # 同步所有标的（空库近 2 年，有数据增量）
python data_sync.py --tickers TQQQ QQQ   # 同步指定标的

# 运行测试（由用户手动执行，AI 不主动运行）
python -m pytest tests/ -v
```

## 环境变量

- `MASSIVE_API_KEY` — Massive REST API 密钥，`run.py` 和股票同步必须设置
- `MASSIVE_S3_ACCESS_KEY` — S3 Access Key，期权 Flat Files 下载必须设置
- `MASSIVE_S3_SECRET_KEY` — S3 Secret Key，期权 Flat Files 下载必须设置
- `MASSIVE_S3_ENDPOINT` — 可选，默认 `https://files.massive.com`
- `MASSIVE_S3_BUCKET` — 可选，默认 `flatfiles`
- `TELEGRAM_BOT_TOKEN` — Telegram Bot API Token，`notify.py` 推送必须设置
- `TELEGRAM_CHAT_ID` — Telegram 接收者 ID，`notify.py` 推送必须设置

所有 key 存放在 `~/.zshrc`，使用前 `source ~/.zshrc`。

## Gotchas

- **月级 sync_log 用独立 data_type**：`sync_options` 写入 sync_log 时 `data_type='option_month'`，键为月份第一天（如 `2024-04-01`）。若误用 `'option'`，会与旧日级记录冲突，导致整月被错误跳过。
- **flat_files_cache 是永久缓存**：`output/flat_files_cache/*.csv.gz` 不会自动清理，重跑直接复用，无需重新下载。
- **INSERT OR IGNORE**：`insert_option_bars_from_csv` 使用 `INSERT OR IGNORE`，同月重跑安全，不会报主键冲突。
- **ensure_synced 以 equity_bars 最新日期为基准**：空库时同步近 2 年；有数据时从最新日期次日增量补齐；数据已是昨天则直接跳过，< 1 秒返回。
- **equity_bars 存储前复权价格**：`adjusted=true` 由 API 返回，DB 中不是原始价格。每次新拆股事件会触发全量重拉，获取最新复权基准。
- **option_bars 入库时自动复权**：根据 splits 表计算累积因子，调整价格/volume/OCC symbol 中的 strike。拆股后的数据因子为 1.0，不调整。
- **splits 表检测新事件**：`ensure_synced` 每次先拉 splits API，发现新记录时自动清空该 ticker 的所有数据并全量重拉。无新事件时 < 1 秒。
- **OTM 模型为 per-tier dict**：`DEFAULT_OTM` 是 `dict[str, float]`，每个层级独立映射 OTM 值。`get_otm_for_ticker()` 返回 dict（非 tuple）。C2/C4 为 20% OTM，其余均为 10%。
- **classify_tier 返回 C1-C4**：不再返回 `"C"`，而是 `"C1"`（趋势延续）、`"C2"`（过热追涨）、`"C3"`（跌势减速）、`"C4"`（加速下杀）。C 子分类基于 Close vs MA20/MA60 和 MACD 收窄/放大。

## 开发与提交规范

### 分支

所有新功能直接在主仓库建分支，分支统一放在 `feature/` 前缀下。不使用 git worktree。

### Commit 提交格式

提交内容使用中文，格式为：

```
[分支名][类别] 提交描述
```

- 分支名使用小写加 `-` 命名
- 类别为 2-4 个字
- `[类别]` 后接一个空格再写具体描述

示例：

```
[feature/naming-refactor][重构] 统一函数命名，删除废弃接口
[feature/db-first-flow][功能] 新增期权数据 DB 查询接口
[feature/db-first-flow][修复] 修正 ensure_synced 增量起始日期计算
```

### 代码质量

- 代码需附带简洁有力的注释
- 所有开发完的内容需编写并通过单元测试
- 【重要】测试用例由用户手动运行，AI 不主动执行测试命令
- 修改模块的 API、参数、输出格式等接口变更时，须同步更新对应模块的 `README.md`

### Git 操作偏好

- 【重要】不要自动 commit，等用户指示
- 【重要】每次 git 提交前需**人工预览确认**，预览时附带提交标题
- 合并到 main 需人工确认后再执行