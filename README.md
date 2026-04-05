# Lambda

TQQQ Sell Put 分层策略回测系统，基于 Massive API 日K与期权数据，自动分层决策并生成可视化报告。

## 策略概要

每周首个交易日，根据技术指标将市场状态分为 9 层（A / B1-B4 / C1-C4），自动推导 OTM 幅度并匹配期权合约：

| 层级 | 名称 | OTM |
|------|------|-----|
| A | 企稳双撑 | 8% |
| B1 | 回调均线 | 8% |
| B2 | 超跌支撑 | 8% |
| B3 | 趋势动能弱 | 12% |
| B4 | 低波整理 | 15% |
| C1 | 跌势减速 | 12% |
| C2 | 趋势延续 | 15% |
| C3 | 过热追涨 | 15% |
| C4 | 加速下杀 | 20% |

含熔断机制：连续 3 周 C 类分层时暂停卖出。

## 快速开始

```bash
# 安装
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 设置环境变量（API Key 等，详见 CLAUDE.md）
export MASSIVE_API_KEY="your-key"
export MASSIVE_S3_ACCESS_KEY="your-key"
export MASSIVE_S3_SECRET_KEY="your-key"

# 运行策略（自动同步数据 → 计算 → 生成报告）
python -m cli.run

# 部署到 Cloudflare Pages
python -m cli.deploy

# 仅同步数据
python -m cli.sync
```

运行后生成：
- `output/TQQQ.json` — 策略数据
- `output/TQQQ.html` — 可视化报告（双击直接打开）

## 项目结构

```
├── config.py            # 全局常量（TQQQ 专用）
├── cli/                 # CLI 入口（run / deploy / sync）
├── core/                # 业务逻辑（策略 / 回测 / 指标 / 熔断）
├── data/                # 数据层（DuckDB 存储 / 同步）
├── output/              # 输出（报告生成 / 部署 / 模板）
├── tests/               # 测试（镜像源码结构）
└── docs/                # 文档
```

## 测试

```bash
python -m pytest tests/ -v
```
