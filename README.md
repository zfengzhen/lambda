# Lambda

Sell Put 分层策略回测系统，基于 Massive API 日K数据，自动分层决策并生成可视化报告。

## 策略概要

每周首个交易日，根据技术指标将市场状态分为 5 层：

| 层级 | 名称 | 条件 | OTM |
|------|------|------|-----|
| A | 企稳双撑 | MACD 收窄 + 价格高于 Pivot | 低 |
| B1 | 回调均线 | 价格回落至 MA20~MA60 之间 | 中 |
| B2 | 低波整理 | 波动率 < 50 且靠近 MA20 | 中 |
| B3 | 超跌支撑 | DIF < 0 但高于 Pivot 支撑 | 中 |
| B4 | 趋势动能弱 | MA20 > MA60 但 DIF < 0 | 中 |
| C | 观望 | 不满足以上条件，跳过 | 高 |

OTM 幅度根据标的杠杆倍数自动推导（3x ETF: A=10%, B=15%）。

## 快速开始

```bash
# 安装
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 设置 API Key
export MASSIVE_API_KEY="your-key"

# 运行（默认 TQQQ，增量拉取）
python run.py

# 多标的 / 全量拉取
python run.py TQQQ QQQ
python run.py --full --years 3
```

运行后生成：
- `data/{TICKER}.json` — 策略数据（含日K，用于增量拉取）
- `{TICKER}.html` — 可视化报告（双击直接打开）

## 项目结构

```
├── run.py           # 入口：拉取 → 策略计算 → JSON → HTML
├── strategy.py      # 策略核心：分层判定、回测引擎
├── fetch_client.py  # Massive API 客户端
├── indicators.py    # 技术指标（MA/MACD/Pivot）
├── template.html    # 报告模板
├── tests/           # 单元测试
└── data/            # JSON 数据
```

## 测试

```bash
python -m pytest tests/ -v
```
