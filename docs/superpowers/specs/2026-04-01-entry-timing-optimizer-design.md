# 设计文档：卖 Put 周内入场限价优化器

**日期**：2026-04-01
**分支**：feature/weekly-strategy
**关联策略**：Lambda 策略（A/B 分层 Sell Put）

---

## 背景

Lambda 策略已解决"卖多深"的问题：周一收盘后判定分层，确定行权价和到期日。但策略未定义执行层——周二开盘时应挂多少价格的限价单，才能在保持高成交率的同时最大化收取的权利金。

本模块通过历史回测，找到使**总权利金最大**的限价倍数 `k`，输出一条可直接用于实盘的规则：

> 周一收盘后查到对应期权收盘价 P，周二开盘挂限价 `P × k`，持续整周（GTC，周五收盘前有效）。

---

## 问题定义

对回测中 48 笔历史交易（A 层 25 笔 + B 层 23 笔），每笔已知：

| 字段 | 来源 |
|------|------|
| `week_start` | 信号周周一日期 |
| `layer` | A / B1 / B2 / B3 / B4 |
| `mon_close` | TQQQ 周一收盘价 |
| `strike` | 行权价 = `mon_close × (1 - otm_pct)` |
| `expiry` | 到期日 = 信号周第 3 个周五 |

**决策变量**：限价 = `mon_close_option × k`，其中 `mon_close_option` 为对应期权周一收盘价。

**成交条件**：`week_high ≥ limit`，其中 `week_high = max(周二高, 周三高, 周四高, 周五高)`。

**优化目标**：`max over k: sum(limit × I(week_high ≥ limit) for all trades)`

---

## 架构

```
strategy.py (现有)          新增模块
───────────────────         ──────────────────────────────
weekly_groups +             option_fetcher.py
分层判定结果     ──────→     · 查合约 symbol（All Contracts）
(48 笔信号周)               · 拉信号周 Mon-Fri 日线 OHLC
                                      ↓
                            entry_optimizer.py
                            · 扫描 k ∈ [0.5, 3.0]
                            · 计算每个 k 的 total_premium
                            · 分层分析（A vs B）
                                      ↓
                            输出报告 + k_star
```

两个新模块独立于现有 `run.py` / `strategy.py`，以单独脚本运行，结果以配置参数形式给出。

---

## 数据获取（option_fetcher.py）

### Step 1：从策略输出提取信号周

复用 `strategy.py` 中现有的 `weekly_groups` 和分层判定逻辑，提取每笔信号周的结构化数据：

```python
{
    "week_start":  "2025-01-06",
    "layer":       "A",
    "mon_close":   42.84,    # TQQQ 标的周一收盘
    "strike":      38.56,    # = mon_close × 0.90
    "expiry":      "2025-01-31",
    "otm_pct":     0.10
}
```

### Step 2：查期权合约 Symbol

使用 Massive API `All Contracts` 端点，按行权价和到期日匹配：

```
GET /options/contracts
    ?underlying_ticker=TQQQ
    &contract_type=put
    &strike_price=38.56
    &expiration_date=2025-01-31
→ "O:TQQQ250131P00038560"
```

### Step 3：拉信号周日线 OHLC

使用 `Options Custom Bars (OHLC)` 端点，取信号周**周一到周五**：

```
GET /options/aggregates/O:TQQQ250131P00038560/range/1/day
    ?from=2025-01-06&to=2025-01-10
→ [{date, open, high, low, close}, ...]  # 最多 5 条
```

存储字段：`mon_close_option, tue_open, tue_high, wed_high, thu_high, fri_high`

**数据量**：48 合约 × 最多 6 条 = 约 288 条记录，预计 API 调用 < 30 秒。

### 异常处理

| 情况 | 处理 |
|------|------|
| 合约不存在（行权价不在链上）| 跳过，记录 warning，用最近 strike 替代 |
| 周一期权无成交（无收盘价）| 改用 Black-Scholes 估算值作为参考价 |
| 某天无成交（week_high 缺失）| 用前一天收盘价填充，或跳过该笔 |

---

## 优化算法（entry_optimizer.py）

```python
import numpy as np

ks = np.arange(0.5, 3.01, 0.05)
results = []

for k in ks:
    total_premium = 0.0
    fill_count = 0
    for trade in trades:
        limit = trade["mon_close_option"] * k
        week_high = max(
            trade["tue_high"], trade["wed_high"],
            trade["thu_high"], trade["fri_high"]
        )
        if week_high >= limit:
            total_premium += limit
            fill_count += 1
    results.append({
        "k": k,
        "total_premium": total_premium,
        "fill_count": fill_count,
        "fill_rate": fill_count / len(trades)
    })

k_star = max(results, key=lambda r: r["total_premium"])["k"]
```

**分层分析**：同样的扫描对 A 层（25 笔）和 B 层（23 笔）分别运行，确认两层是否适合同一 k 或需要分开设置。

---

## 输出

### 1. 最优参数

```
最优 k_star: 1.XX
成交率: XX%（XX / 48 笔成交）
总权利金: +XX.XX%（相对于 100% 资金）
vs 市价单 (k=1.0): 多收 +X.X%，损失 X 笔成交
```

### 2. k-收益曲线

折线图：x 轴为 k，y 轴为 total_premium，标注 k_star 和 k=1.0 两个关键点。

### 3. 分层对比表

| 层级 | 最优 k | 成交率 | 每笔平均权利金 |
|------|--------|--------|---------------|
| A    | ?      | ?%     | ?%            |
| B    | ?      | ?%     | ?%            |
| 合并 | ?      | ?%     | ?%            |

### 4. 未成交分析

在 k_star 下未成交的几笔：列出对应周的市场状态（层级、IV、TQQQ 走势），判断是否有可识别的特征可用于预判。

---

## 实盘操作规则（研究产出）

研究完成后输出如下可执行规则（待填入实际数值）：

> **周一收盘后**：
> 1. 确认信号层级（A 或 B）和行权价
> 2. 查对应期权当日收盘价 P（富途/IBKR 可直接查）
> 3. **周二开盘挂限价卖单 = P × k_star**，GTC 有效至周五收盘
> 4. 若整周未成交，本周放弃（不追单，等下周信号）

---

## 不在本模块范围内

- 止损/提前平仓规则（独立研究方向，见 strategy-research.md §6）
- 到期后的轮子策略（Wheel）
- 保证金占用优化
- 多标的（QQQ、SOXL 等）适配

---

## 测试要求

- 单元测试：`option_fetcher.py` 的合约查找和 OHLC 解析逻辑（mock API 响应）
- 集成测试：端到端跑 2-3 笔已知历史交易，验证数据获取和计算结果正确
- 回测验证：最终 48 笔数据的 total_premium 可重现，与手动计算一致
