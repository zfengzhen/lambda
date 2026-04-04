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
├── iv.py                # 标的级 IV 计算：OCC 解析、B-S 反算、VIX 风格加权
├── data_sync.py         # 数据同步 CLI：ensure_synced 自动判断全量/增量（含 IV 计算）
├── data_store.py        # DuckDB 本地存储：建表、写入、查询（option_bars / equity_bars / ticker_iv）
├── s3_downloader.py     # S3 期权 Flat Files 按月下载并写入 DB（S3 客户端复用 flat_file_fetcher）
├── flat_file_fetcher.py # S3 单日文件下载/缓存（output/flat_files_cache/）
├── rest_downloader.py   # Massive REST API 股票日K下载并写入 DB
├── notify.py            # Telegram 截图推送：扫描 output/*.png → 发送 → 删除
├── com.lambda.scheduled-notify.plist  # macOS launchd 定时任务（每日自动运行策略+推送）
│
├── output/
│   ├── market_data.duckdb        # 本地期权/股票数据库（已提交，方便共享基线数据）
│   ├── flat_files_cache/         # S3 原始 .csv.gz 本地缓存（gitignore）
│   ├── {TICKER}.json             # 策略数据（gitignore）
│   ├── {TICKER}.html             # 可视化报告（gitignore）
│   └── lambda-strategy-{TICKER}-{DATE}.png  # 截图（gitignore，由 notify.py 推送后删除）
├── conftest.py          # pytest 全局配置：注册 online marker，默认跳过在线测试
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
option_bars + equity_bars → B-S 反算 → DuckDB ticker_iv  # 标的级 IV
```

- `run.py` 每次运行先调用 `ensure_synced()`，每个 ticker 独立判断：空库全量（近 2 年），有数据增量补齐
- equity 增量基于 per-ticker `MAX(date)`；option 增量基于 per-ticker 月级 `sync_log`
- IV 计算在每次 `ensure_synced()` 末尾自动执行，空表全量、有数据增量

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

# IV 在线验证（调 Massive Snapshot API 对比 B-S 反算，需联网 + MASSIVE_API_KEY）
python -m pytest tests/test_iv.py -m online -v -s --log-cli-level=INFO
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

- **月级 sync_log 用独立 data_type**：`sync_options` 写入 sync_log 时 `data_type='option_month'`，键为月份第一天（如 `2024-04-01`）。若误用 `'option'`，会与旧日级记录冲突，导致整月被错误跳过。sync_log 新增 `ticker` 列，`option_month` 记录按 ticker 独立标记，TQQQ 已同步的月份不影响 QQQ 的同步判断。
- **flat_files_cache 是永久缓存**：`output/flat_files_cache/*.csv.gz` 不会自动清理，重跑直接复用，无需重新下载。
- **INSERT OR IGNORE**：`insert_option_bars_from_csv` 使用 `INSERT OR IGNORE`，同月重跑安全，不会报主键冲突。
- **ensure_synced 按 ticker 独立计算同步起点**：每个 ticker 查自己的 `MAX(date)`（`get_latest_equity_date`），没数据的走全量（近 2 年），已最新的跳过。不再使用全局 `MAX(date)`。
- **equity_bars 存储前复权价格**：`adjusted=true` 由 API 返回，DB 中不是原始价格。每次新拆股事件会触发全量重拉，获取最新复权基准。
- **option_bars 入库时自动复权**：根据 splits 表计算累积因子，调整价格/volume/OCC symbol 中的 strike。拆股后的数据因子为 1.0，不调整。
- **splits 表检测新事件**：`ensure_synced` 每次先拉 splits API，发现新记录时自动清空该 ticker 的所有数据并全量重拉。无新事件时 < 1 秒。
- **OTM 模型为 per-tier dict**：`DEFAULT_OTM` 是 `dict[str, float]`，每个层级独立映射 OTM 值。`get_otm_for_ticker()` 返回 dict（非 tuple）。A/B1/B2 为 8% OTM，B3/C1 为 12% OTM，B4/C2/C3 为 15% OTM，C4 为 20% OTM。
- **classify_tier 返回 C1-C4**：不再返回 `"C"`，而是 `"C1"`（跌势减速）、`"C2"`（趋势延续）、`"C3"`（过热追涨）、`"C4"`（加速下杀）。C 子分类基于 Close vs MA20/MA60 和 MACD 收窄/放大。
- **ticker_iv 与拆股联动**：`delete_ticker_data()` 同步清空 `ticker_iv`，重拉后自动全量回算。该函数只清目标 ticker 的 `option_month` sync_log，不影响其他 ticker 的同步状态。
- **option_bars 新增结构化列**：`strike`/`expiration`/`option_type` 在入库时从 OCC symbol 解析填入。存量数据由 `init_db()` 自动回填。
- **IV 计算依赖 scipy**：`iv.py` 使用 `scipy.stats.norm` 做 B-S 定价，需 `pip install scipy`。
- **IV 的 tte 用日历天/365**：`compute_ticker_iv` 中 `tte = calendar_days / 365.0`，不是交易日/252。这是 B-S 标准做法，与 VIX 方法论一致。
- **连续弱势熔断规则**：在 `run.py` 的 `compute_strategy` 中实现，标记 `skip=True` 的周暂停 Sell Put。前 2 周连续 C 类且本周也是 C 类时触发判断（本周是 A/B 类始终放行）：
  - 本周 C 类但非 C1：趋势没有减速迹象（C2 还在涨但不稳、C3 过热、C4 加速下杀），风险未收敛，暂停
  - 本周 C1 + 前 2 周有 C1：跌势已连续收窄（MACD 收窄出现过至少 2 次），空头力量持续衰减，底部信号较可靠，继续卖出
  - 本周 C1 + 前 2 周无 C1：之前都是 C2/C3/C4 纯弱势，本周才首次出现减速，可能只是下跌中继的短暂喘息而非真正见底，不够安全，暂停
  - 核心思路：单次 C1 减速不可信，连续减速才可信；A/B 类已有支撑条件，不受熔断影响
- **结算差比使用合约真实 strike**：`enrich_weeks_with_options` 从匹配的 OCC symbol 末 8 位提取精确 strike（如 50.5），用于重算 `settle_diff` 和 `safe_expiry`，与页面显示的期权合约一致。OCC strike 判定为平稳到期时，同步清除 `recovery_days` 和 `recovery_gap`，避免策略 strike 与 OCC strike 微小差异导致残留。
- **期权合约向下匹配**：`query_option_on_date` 取 strike ≤ 策略目标值且最接近的合约，确保实际 OTM ≥ 策略要求。当合约 strike 间距较大（如 TQQQ $5 间距）时，实际 OTM 可能显著大于策略值。
- **EXPIRY_WEEKS = 4**：到期周数为 4 周（原为 3 周）。`find_expiry_date` 基于 NYSE 交易日历向前推 4 周取最近的周五。

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