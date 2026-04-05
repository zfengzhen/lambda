# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

TQQQ Sell Put 策略研究与交易项目，使用 **Massive API** 获取股票和期权数据，本地 DuckDB 存储，生成可视化报告并部署到 Cloudflare Pages。

项目仅针对 **TQQQ**（3x 杠杆 QQQ），不支持多标的。

## 项目结构

```
├── config.py                # 全局常量：TICKER/OTM表/杠杆/DB路径/API参数
│
├── cli/                     # 薄壳入口（CLI）
│   ├── run.py               # 策略生成：同步 → 计算 → JSON → HTML
│   ├── deploy.py            # 部署：密码包装 → Cloudflare → Telegram
│   └── sync.py              # 数据同步
│
├── data/                    # 数据层
│   ├── store.py             # DuckDB 连接管理 + init_db
│   ├── schema.py            # 建表 DDL + 迁移
│   ├── queries.py           # 所有 SELECT 操作
│   ├── writers.py           # 所有 INSERT/UPSERT 操作
│   └── sync/                # 数据获取
│       ├── orchestrator.py  # ensure_synced 编排
│       ├── equity.py        # REST 股票日K下载
│       ├── options.py       # S3 期权 Flat Files 下载（含本地缓存）
│       ├── splits.py        # 拆股检测
│       └── iv.py            # IV 计算（B-S 反算 + 30天 ATM 包夹插值）
│
├── core/                    # 业务逻辑层
│   ├── indicators.py        # 技术指标（MA/MACD/动态Pivot）
│   ├── strategy.py          # 分层决策树(A/B1-B4/C1-C4) + 周分组 + 到期日
│   ├── backtest.py          # 回测引擎 + 期权enrichment + 汇总统计
│   ├── options.py           # OCC 合约解析 + 合约匹配（统一入口）
│   └── circuit_breaker.py   # 熔断逻辑（连续 C 类暂停）
│
├── output/                  # 输出层（也是数据目录）
│   ├── report.py            # JSON 组装 + HTML 嵌入
│   ├── deploy.py            # Cloudflare Pages + Telegram 通知
│   ├── template.html        # 可视化报告模板
│   ├── market_data.duckdb   # 本地数据库（已提交）
│   ├── flat_files_cache/    # S3 缓存（gitignore）
│   ├── TQQQ.json            # 策略数据（gitignore）
│   └── TQQQ.html            # 可视化报告（gitignore）
│
├── conftest.py              # pytest 全局配置
├── tests/                   # 测试（镜像源码 data/core/output 结构）
│
├── requirements.txt
├── .venv/                   # 虚拟环境（不提交）
└── docs/
    ├── api/                 # API 文档（Massive 等）
    └── superpowers/         # 设计文档与实施计划
```

## 架构分层

```
cli/  →  core/  →  data/queries  →  data/store
 │        │                           ↑
 │        ↓                           │
 │    data/queries                data/schema
 │                                    │
 ↓                                    ↓
output/  ←── (只读 template.html)   data/writers
```

- `core/` 只通过 `data/queries` 读数据，不直接操作连接
- `output/` 不依赖 `core/`，只接收组装好的数据字典
- `data/sync/iv.py` 引用 `core/options.parse_occ_symbol`（唯一跨层引用）
- `cli/` 是唯一知道完整流程的地方
- 所有模块通过 `config.TICKER` 获取标的代码，不传参

## 数据流

**策略报告（cli/run.py）：**
```
ensure_synced() → DuckDB equity_bars → 指标计算 → 策略计算 → JSON(output/) → HTML
```

**本地数据库（data/sync/orchestrator.py）：**
```
splits检测 → equity增量同步 → options按月同步 → IV增量计算
```

- 空库全量同步近 2 年，有数据增量补齐
- equity 增量基于 `MAX(date)`；option 增量基于月级 `sync_log`
- IV 计算在每次 `ensure_synced()` 末尾自动执行

## DuckDB 数据表

| 表名 | 主键 | 说明 |
|------|------|------|
| `equity_bars` | (date, ticker) | 股票日K（前复权） |
| `option_bars` | (date, symbol) | 期权日K（含 strike/expiration/option_type） |
| `splits` | (ticker, exec_date) | 拆股记录 |
| `sync_log` | auto id | 同步审计（按 ticker+月 跟踪 option_month） |
| `ticker_iv` | (date, ticker) | 每日 IV（30天 ATM 插值） |

## API 文档

参考文档位于 `docs/api/`：
- `docs/api/massive-llms.txt` — Massive API 接口参考

## 开发说明

- 项目基于 Python，版本 3.12+（开发环境使用 3.14）
- 项目仅支持 TQQQ，所有函数不接受 ticker 参数

## 常用命令

```bash
# 虚拟环境（首次需 python3 -m venv .venv && pip install -r requirements.txt）
source .venv/bin/activate

# ── Lambda 策略（Sell Put）──
python -m cli.run          # TQQQ：自动同步 → 策略计算 → JSON → HTML
# 双击 output/TQQQ.html 查看报告（数据已内嵌，无需服务器）

# ── 部署到 Cloudflare Pages ──
python -m cli.deploy       # 密码包装 → Cloudflare 部署 → Telegram 通知

# ── 本地数据库同步 ──
python -m cli.sync         # 同步 TQQQ（空库近 2 年，有数据增量）

# 运行测试（由用户手动执行，AI 不主动运行）
python -m pytest tests/ -v

# IV 在线验证（调 Massive Snapshot API 对比 B-S 反算，需联网 + MASSIVE_API_KEY）
python -m pytest tests/data/sync/test_iv.py -m online -v -s --log-cli-level=INFO
```

## 环境变量

- `MASSIVE_API_KEY` — Massive REST API 密钥，策略生成和股票同步必须设置
- `MASSIVE_S3_ACCESS_KEY` — S3 Access Key，期权 Flat Files 下载必须设置
- `MASSIVE_S3_SECRET_KEY` — S3 Secret Key，期权 Flat Files 下载必须设置
- `MASSIVE_S3_ENDPOINT` — 可选，默认 `https://files.massive.com`
- `MASSIVE_S3_BUCKET` — 可选，默认 `flatfiles`
- `LAMBDA_DEPLOY_PASSWORD` — 前端密码锁密码，部署必须设置
- `CLOUDFLARE_API_TOKEN` — Cloudflare API Token，部署必须设置
- `CLOUDFLARE_ACCOUNT_ID` — Cloudflare 账户 ID，部署必须设置
- `CLOUDFLARE_PAGES_PROJECT` — Cloudflare Pages 项目名，部署必须设置
- `LAMBDA_TELEGRAM_BOT_TOKEN` — Telegram Bot Token（可选，缺少时跳过通知）
- `LAMBDA_TELEGRAM_CHAT_ID` — Telegram Chat ID（可选，缺少时跳过通知）

所有 key 存放在 `~/.zshrc`，使用前 `source ~/.zshrc`。

## Gotchas

- **deploy 使用未文档化 API**：Cloudflare Pages Direct Upload REST API 非官方文档，可能随时变动。若部署失败，优先检查 API 是否变更，备选方案是改用 `wrangler pages deploy`。
- **月级 sync_log 用独立 data_type**：`sync_options` 写入 sync_log 时 `data_type='option_month'`，键为月份第一天（如 `2024-04-01`）。
- **equity_bars 存储前复权价格**：`adjusted=true` 由 API 返回。每次新拆股事件会触发全量重拉。
- **option_bars 入库时自动复权**：根据 splits 表计算累积因子，调整价格/volume/OCC symbol 中的 strike。
- **splits 表检测新事件**：`ensure_synced` 每次先拉 splits API，发现新记录时自动清空所有数据并全量重拉。
- **IV 的 tte 用日历天/365**：`compute_ticker_iv` 中 `tte = calendar_days / 365.0`，B-S 标准做法。
- **IV 算法变更需清表重算**：`ticker_iv` 是增量计算的，修改算法后需先 `python -c "from data.store import get_connection; get_connection().execute(\"DELETE FROM ticker_iv WHERE ticker='TQQQ'\").close()"` 清表，再 `python -m cli.sync` 全量回算。
- **结算差比使用合约真实 strike**：`enrich_with_options` 从 OCC symbol 提取精确 strike 重算 `settle_diff`。
- **期权合约向下匹配**：`query_option_on_date` 取 strike ≤ 策略目标值且最接近的合约。
- **output/ 双重角色**：既是 Python 包（report.py/deploy.py）也是数据目录（.duckdb/.json/.html）。

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
[feature/tqqq-only-refactor][重构] 统一函数命名，删除废弃接口
[feature/tqqq-only-refactor][功能] 新增期权数据 DB 查询接口
```

### 代码质量

- 代码需附带简洁有力的注释
- 所有开发完的内容需编写并通过单元测试
- 【重要】测试用例由用户手动执行，AI 不主动执行测试命令

### Git 操作偏好

- 【重要】不要自动 commit，等用户指示
- 【重要】每次 git 提交前需**人工预览确认**，预览时附带提交标题
- 合并到 main 需人工确认后再执行
