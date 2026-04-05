# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

基于股票的策略研究与交易项目，使用 **Massive API** — 金融数据平台，提供股票、加密货币、外汇、期权、指数的平文件和 REST 接口，支持纳秒级 tick 数据

## 项目结构

```
├── run.py               # 策略入口：拉取数据 → 策略计算 → JSON → HTML
├── deploy.py            # 部署脚本：密码包装 → Cloudflare Pages 部署 → Telegram 通知
├── strategy.py          # 策略核心：周分组、分层判定(A/B1-B4/C1-C4)、OTM推导、回测
├── indicators.py        # 技术指标（MA/MACD/Pivot）
├── template.html        # 可视化报告模板
│
├── iv.py                # 标的级 IV 计算：OCC 解析、B-S 反算、30天 ATM 包夹插值
├── data_sync.py         # 数据同步 CLI：ensure_synced 自动判断全量/增量（含 IV 计算）
├── data_store.py        # DuckDB 本地存储：建表、写入、查询（option_bars / equity_bars / ticker_iv）
├── s3_downloader.py     # S3 期权 Flat Files 按月下载并写入 DB（S3 客户端复用 flat_file_fetcher）
├── flat_file_fetcher.py # S3 单日文件下载/缓存（output/flat_files_cache/）
├── rest_downloader.py   # Massive REST API 股票日K下载并写入 DB
│
├── output/
│   ├── market_data.duckdb        # 本地期权/股票数据库（已提交，方便共享基线数据）
│   ├── flat_files_cache/         # S3 原始 .csv.gz 本地缓存（gitignore）
│   ├── {TICKER}.json             # 策略数据（gitignore）
│   └── {TICKER}.html             # 可视化报告（gitignore）
├── conftest.py          # pytest 全局配置：注册 online marker，默认跳过在线测试
├── tests/               # pytest 单元测试
├── requirements.txt     # Python 依赖
├── .venv/               # Python 虚拟环境（不提交）
└── docs/                # 文档 + API 参考
    ├── api/             # API 文档（Massive 等）
    ├── superpowers/     # 实现计划与设计文档
    ├── strategy-*.md/html  # 策略说明文档
    └── entry-timing-research.md  # 入场限价优化研究报告
```

## 数据流

**策略报告（run.py）：**
```
ensure_synced() → DuckDB equity_bars → 指标计算 → 策略计算 → JSON(output/, 含 market 行情快照) → HTML
```

**本地数据库（data_sync.py）：**
```
S3 Flat Files (.csv.gz) → DuckDB option_bars   # 期权日K，按月批量写入
Massive REST API        → DuckDB equity_bars   # 股票日K
option_bars + equity_bars → B-S 反算 → DuckDB ticker_iv  # 标的级 IV
```

- `run.py` 每次运行先调用 `ensure_synced()`，每个 ticker 独立判断：空库全量（近 2 年），有数据增量补齐
- equity 增量基于 per-ticker `MAX(date)`；option 增量基于 per-ticker 月级 `sync_log`
- IV 计算在每次 `ensure_synced()` 末尾自动执行，空表全量、有数据增量

## API 文档

参考文档位于 `docs/api/`：
- `docs/api/massive-llms.txt` — Massive API 接口参考

## 开发说明

- 项目基于 Python，版本 3.12+（开发环境使用 3.14）

## 常用命令

```bash
# 虚拟环境（首次需 python3 -m venv .venv && pip install -r requirements.txt）
source .venv/bin/activate

# ── Lambda 策略（Sell Put）──
python run.py              # 默认 TQQQ：自动同步 → 策略计算 → JSON → HTML
python run.py TQQQ QQQ     # 多标的批量处理
# 双击 {TICKER}.html 查看报告（数据已内嵌，无需服务器）

# ── 部署到 Cloudflare Pages ──
python deploy.py                # 默认 TQQQ：密码包装 → Cloudflare 部署 → Telegram 通知
python deploy.py --ticker QQQ   # 部署指定标的

# ── 本地数据库同步 ──
python data_sync.py                      # 同步所有标的（空库近 2 年，有数据增量）
python data_sync.py --tickers TQQQ QQQ   # 同步指定标的

# 运行测试（由用户手动执行，AI 不主动运行）
python -m pytest tests/ -v

# IV 在线验证（调 Massive Snapshot API 对比 B-S 反算，需联网 + MASSIVE_API_KEY）
python -m pytest tests/test_iv.py -m online -v -s --log-cli-level=INFO
```

## 环境变量

- `MASSIVE_API_KEY` — Massive REST API 密钥，`run.py` 和股票同步必须设置
- `MASSIVE_S3_ACCESS_KEY` — S3 Access Key，期权 Flat Files 下载必须设置
- `MASSIVE_S3_SECRET_KEY` — S3 Secret Key，期权 Flat Files 下载必须设置
- `MASSIVE_S3_ENDPOINT` — 可选，默认 `https://files.massive.com`
- `MASSIVE_S3_BUCKET` — 可选，默认 `flatfiles`
- `LAMBDA_DEPLOY_PASSWORD` — 前端密码锁密码，`deploy.py` 必须设置
- `CLOUDFLARE_API_TOKEN` — Cloudflare API Token，`deploy.py` 必须设置
- `CLOUDFLARE_ACCOUNT_ID` — Cloudflare 账户 ID，`deploy.py` 必须设置
- `CLOUDFLARE_PAGES_PROJECT` — Cloudflare Pages 项目名，`deploy.py` 必须设置
- `LAMBDA_TELEGRAM_BOT_TOKEN` — Telegram Bot Token，部署通知使用（可选，缺少时跳过通知）
- `LAMBDA_TELEGRAM_CHAT_ID` — Telegram Chat ID，部署通知使用（可选，缺少时跳过通知）

所有 key 存放在 `~/.zshrc`，使用前 `source ~/.zshrc`。

## Gotchas

- **deploy.py 使用未文档化 API**：Cloudflare Pages Direct Upload REST API 非官方文档，可能随时变动。若部署失败，优先检查 API 是否变更，备选方案是改用 `wrangler pages deploy`。
- **月级 sync_log 用独立 data_type**：`sync_options` 写入 sync_log 时 `data_type='option_month'`，键为月份第一天（如 `2024-04-01`）。若误用 `'option'`，会与旧日级记录冲突，导致整月被错误跳过。sync_log 新增 `ticker` 列，`option_month` 记录按 ticker 独立标记，TQQQ 已同步的月份不影响 QQQ 的同步判断。
- **ensure_synced 按 ticker 独立计算同步起点**：每个 ticker 查自己的 `MAX(date)`（`get_latest_equity_date`），没数据的走全量（近 2 年），已最新的跳过。不再使用全局 `MAX(date)`。
- **equity_bars 存储前复权价格**：`adjusted=true` 由 API 返回，DB 中不是原始价格。每次新拆股事件会触发全量重拉，获取最新复权基准。
- **option_bars 入库时自动复权**：根据 splits 表计算累积因子，调整价格/volume/OCC symbol 中的 strike。拆股后的数据因子为 1.0，不调整。
- **splits 表检测新事件**：`ensure_synced` 每次先拉 splits API，发现新记录时自动清空该 ticker 的所有数据并全量重拉。无新事件时 < 1 秒。
- **ticker_iv 与拆股联动**：`delete_ticker_data()` 同步清空 `ticker_iv`，重拉后自动全量回算。该函数只清目标 ticker 的 `option_month` sync_log，不影响其他 ticker 的同步状态。
- **IV 的 tte 用日历天/365**：`compute_ticker_iv` 中 `tte = calendar_days / 365.0`，不是交易日/252。这是 B-S 标准做法。
- **IV 算法变更需清表重算**：`ticker_iv` 是增量计算的，修改 `compute_ticker_iv` 算法后，需先 `DELETE FROM ticker_iv WHERE ticker='XXX'`，再 `python data_sync.py --tickers XXX` 全量回算。
- **结算差比使用合约真实 strike**：`enrich_weeks_with_options` 从匹配的 OCC symbol 末 8 位提取精确 strike（如 50.5），用于重算 `settle_diff` 和 `safe_expiry`，与页面显示的期权合约一致。OCC strike 判定为平稳到期时，同步清除 `recovery_days` 和 `recovery_gap`，避免策略 strike 与 OCC strike 微小差异导致残留。
- **期权合约向下匹配**：`query_option_on_date` 取 strike ≤ 策略目标值且最接近的合约，确保实际 OTM ≥ 策略要求。当合约 strike 间距较大（如 TQQQ $5 间距）时，实际 OTM 可能显著大于策略值。

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