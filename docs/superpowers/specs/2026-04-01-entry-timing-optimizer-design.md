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
分层判定结果     ──────→     · 构建 OCC 合约 symbol
(48 笔信号周)               · 从 DuckDB 拉信号周 Mon-Fri 日线 OHLC
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

使用 `build_occ_symbol()` 按标的、到期日、行权价构建 OCC 格式合约名：

```
build_occ_symbol("TQQQ", "2025-01-31", 38.5, "P")
→ "O:TQQQ250131P00038500"
```

### Step 3：从 DuckDB 拉信号周日线 OHLC

查询本地 `option_bars` 表，取信号周**周一到周五**：

```python
data_store.query_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-10")
→ [{date, open, high, low, close}, ...]  # 最多 5 条
```

存储字段：`mon_close_option, tue_open, tue_high, wed_high, thu_high, fri_high`

**数据量**：48 合约 × 最多 6 条 = 约 288 条记录，全部从本地 DuckDB 读取，无网络开销。

### 异常处理

| 情况 | 处理 |
|------|------|
| 合约不存在（行权价不在链上）| 跳过，记录 warning，用最近 strike 替代 |
| 周一期权无成交（无收盘价）| 标记 data_complete=False，跳过该笔 |
| 某天无成交（week_high 缺失）| 有效天数 ≥ 3 仍视为 complete，否则跳过 |

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

---

## 实现状态

| 模块 | 状态 | 说明 |
|------|------|------|
| `option_fetcher.py` | ✅ 已完成 | OCC 合约构建、三级数据源（DuckDB → S3 Flat Files → REST）、信号提取 |
| `entry_optimizer.py` | ✅ 已完成 | 期权日线 enrichment、k 扫描（0.5–3.0）、分层分析、最优 k 输出 |
| `strategy.py` 集成 | ✅ 已完成 | `backtest_weeks()` → `get_signal_trades()` → `_enrich()` → `sweep_k()` 全链路打通 |
| 单元测试 | ✅ 已完成 | `tests/` 下覆盖合约查找、OHLC 解析、k 扫描逻辑 |

### 数据源（已实现）

```
DuckDB (output/market_data.duckdb)  ← 唯一数据源，本地查询，无网络开销
```

前置条件：运行 `python data_sync.py` 同步期权数据到本地 DB。

### 关键实现细节

- **行权价取整**：`round_to_strike_increment()` 将计算出的 strike 四舍五入到 0.5 间距（TQQQ 合约规则）
- **数据完整性判定**：`data_complete = True` 要求有 `mon_close_option` + 至少 3 天有效高点
- **OTM 动态调整**：`get_otm_for_ticker()` 按杠杆倍数缩放：3x 用基准值，2x 减 1/3，1x 减 2/3

---

## 敏感性分析

k_star 的可靠性取决于以下维度，建议在报告中附带：

### 1. k 曲线平坦度

若 total_premium 在 k ∈ [k_star - 0.2, k_star + 0.2] 区间变化 < 5%，说明优化目标对 k 不敏感，k_star 的精确值不重要，可取整到 0.1。反之，若曲线陡峭（存在明显拐点），k_star 置信度高。

### 2. 样本量影响

48 笔交易为小样本。建议用 bootstrap 重采样（1000 次有放回抽样）计算 k_star 的 95% 置信区间：

```python
import numpy as np

k_stars = []
for _ in range(1000):
    sample = np.random.choice(trades, size=len(trades), replace=True)
    k_stars.append(find_optimal_k(sample))

ci_low, ci_high = np.percentile(k_stars, [2.5, 97.5])
```

若置信区间窄（如 [1.1, 1.3]），结果稳健；若宽（如 [0.8, 2.0]），需谨慎使用。

### 3. 时间稳定性

将 48 笔按时间前后各半（前 24 笔 vs 后 24 笔）分别求 k_star。若两段差异 > 0.3，说明市场环境变化显著，单一 k 可能不够。

### 4. A/B 分层差异

若 A 层和 B 层的 k_star 差异 > 0.3，应考虑分层设置不同的 k 值，而非统一参数。

---

## 风险与局限性

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| **过拟合** | 48 笔样本优化出的 k 在未来失效 | bootstrap 置信区间 + 时间分段验证 |
| **流动性假设** | 回测假设 `week_high ≥ limit` 即成交，忽略实际买盘深度 | 适用于 TQQQ 高流动性期权；低流动性标的需打折 |
| **周一收盘价偏差** | 期权收盘价可能 ≠ 实际可交易价（盘后/低成交量） | 周一无成交时改用 BS 估算值；实盘可取 bid-ask 中点 |
| **GTC 单假设** | 限价单挂一周，实际可能部分成交或被券商取消 | 每日检查挂单状态，必要时重挂 |
| **市场状态漂移** | 高波动/低波动周期切换导致最优 k 变化 | 定期（每季度）用最新数据重跑优化 |

---

## 后续迭代方向

### Phase 2：动态 k 策略

当前 k 为静态常量。可探索基于当周 IV（隐含波动率）或 VIX 水平动态调整：

```
k = k_base + α × (IV_current / IV_median - 1)
```

高 IV 时适当提高 k（要更多权利金），低 IV 时降低 k（保成交率）。

### Phase 3：日内分时优化

当前以"周二开盘挂单"为假设。可进一步研究：
- 周二开盘 vs 周二收盘 vs 周三开盘，哪个时点挂单效果最好
- 是否存在日内最优挂单时间窗口

### Phase 4：与 Wheel 策略联动

若 Put 被行权（到期 ITM），进入 Wheel 循环（持股 → 卖 Call）。优化器可扩展为覆盖完整 Wheel 周期的收益模拟。

### Phase 5：多标的适配

将 TQQQ 验证过的框架推广到 QQQ、SOXL 等标的，需调整：
- 行权价取整间距（不同标的规则不同）
- OTM 杠杆缩放系数
- 流动性阈值

---

## 变更记录

| 日期 | 变更内容 |
|------|----------|
| 2026-04-01 | 初版设计文档，定义问题、架构和优化算法 |
| 2026-04-01 | 补充实现状态、敏感性分析、风险分析、迭代方向 |
