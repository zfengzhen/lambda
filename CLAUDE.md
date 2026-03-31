# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

基于股票的策略研究与交易项目，集成两个 API：
- **Futu OpenAPI**（富途）— 香港券商，提供 Python SDK，支持港股/美股/A股行情获取与交易
- **Massive API** — 金融数据平台，提供股票、加密货币、外汇、期权、指数的平文件和 REST 接口，支持纳秒级 tick 数据

## 项目结构

```
lambda/
├── run.py               # 唯一入口：拉取数据 → 策略计算 → JSON → HTML → 截图 PNG
├── strategy.py          # 策略核心：周分组、分层判定(A/B1-B4/C)、OTM推导、回测
├── fetch_client.py      # Massive API 多周期K线拉取
├── indicators.py        # 技术指标（MA/MACD/Pivot）
├── template.html        # 可视化报告模板
├── output/              # 运行产物（gitignore）
│   ├── {TICKER}.json    # 策略数据（含 daily_bars，用于增量拉取）
│   └── {TICKER}.html    # 可视化报告
├── tests/               # pytest 单元测试
├── requirements.txt     # Python 依赖
├── .venv/               # Python 虚拟环境（不提交）
└── docs/                # 文档 + API 参考
    ├── api/             # API 文档（Massive 等）
    └── strategy-*.md/html  # 策略说明文档
```

## 数据流

```
fetch API → DataFrame(内存) → 指标计算 → 策略计算 → JSON(output/) → 内嵌到 HTML
```

- 无 CSV 中间步骤，JSON 中 `daily_bars` 字段存储原始日K数据
- 增量拉取基于 JSON 的 `data_range[1]`（上次最新日期）

## API 文档

参考文档位于 `docs/api/`：
- `docs/api/massive-llms.txt` — Massive API 接口参考

## 开发说明

- 项目基于 Python（富途 SDK 为 Python）
- 调用富途 API 前需在本地启动 OpenD 网关进程；可使用 `install-opend` skill 进行安装配置
- 开发时可使用 `openapi` skill 快速查询富途交易/行情接口

## 虚拟环境

项目使用 `.venv` 虚拟环境管理 Python 依赖，首次克隆后需初始化：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

后续每次打开项目只需激活：

```bash
source .venv/bin/activate
```

## 常用命令

```bash
# 激活虚拟环境
source .venv/bin/activate

# 安装依赖（含 pandas/numpy/requests/pytest/playwright）
pip install -r requirements.txt

# ── Lambda 策略（Sell Put）──
python run.py              # 默认 TQQQ 增量拉取 → 策略计算 → JSON → HTML → 截图 PNG
python run.py TQQQ QQQ     # 多标的批量处理
python run.py --full       # 全量拉取（默认 10 年，按 API 可用范围）
python run.py --years 3    # 指定回溯年数
# 双击 {TICKER}.html 查看报告（数据已内嵌，无需服务器）

# 截图依赖 Playwright + Chromium（首次需安装，缺失时自动跳过不影响主流程）
# pip install playwright && playwright install chromium

# 运行测试
python -m pytest tests/ -v
```

## 环境变量

- `MASSIVE_API_KEY` — Massive API 密钥，运行数据拉取前必须设置

## 开发与提交规范

### 分支与 Worktree 管理

所有新功能通过 `git worktree` 开发，分支统一放在 `feature/` 前缀下。Worktree 目录采用**项目同级专用目录**方式组织：

```
~/projects/
├── my-app/                     # 主仓库 (main)，原则上不做开发
├── my-app-worktrees/
│   ├── feature-login/          # 对应分支 feature/feature-login
│   └── feature-canvas/         # 对应分支 feature/feature-canvas
└── other-project/
```

创建 worktree 由人工完成，AI 进入对应 worktree 目录后进行编码工作。

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
[feature/dev-canvas][功能] 增加canvas特效
[feature/feature-login][修复] 修正登录token过期判断
[feature/feature-login][重构] 提取用户验证逻辑为独立模块
```

### 代码质量

- 代码需附带简洁有力的注释
- 所有开发完的内容需编写并通过单元测试
- 修改模块的 API、参数、输出格式等接口变更时，须同步更新对应模块的 `README.md`

### Git 操作偏好

- 不要自动 commit，等用户指示
- 多个 commit 可能需要 squash，先确认

### 提交与合并流程

1. AI 完成编码和单元测试
2. 每次 git 提交前需**人工预览确认**，预览时附带提交标题
3. 功能开发完成后，合并到 `main` 需**人工确认**后再执行
4. 合并完成后清理 worktree（`git worktree remove`）