# 标的级 IV（Implied Volatility）计算设计文档

## 背景

当前系统只有 equity 的 20 日历史波动率（`compute_hist_vol()`），缺少期权隐含波动率。标的级 IV 是衡量市场对未来波动预期的核心指标，后续可纳入分层策略判定。本次功能参照 CBOE VIX 算法思路，从 ATM 附近、最近两个到期日的少量合约加权计算标的整体 IV。

## 决策摘要

| 决策项 | 选择 |
|--------|------|
| 数据来源 | 本地 Black-Scholes 反算（用 option_bars close + equity_bars close） |
| 无风险利率 | 固定常量 `r = 0.05` |
| option_bars 改造 | 新增 `strike`, `expiration`, `option_type` 列，入库时从 OCC symbol 解析填入 |
| 存储 | 新建 `ticker_iv` 表（date, ticker, iv） |
| 同步策略 | 空表全量回算，有数据增量补齐（同 equity_bars 模式） |
| 策略接入 | 本分支暂不接入，仅计算并存储 |

## 方案选型

**选定方案：本地 B-S 反算**

- 已有全量历史 option_bars 数据，可回算任意历史日期的 IV
- 不依赖额外 API 调用，同步流程不变
- VIX 算法本身就是从期权价格推导的，B-S 反算是标准做法

淘汰方案：
- Massive REST API Snapshot 端点获取现成 IV — 仅能获取当前快照，无法回算历史
- 动态获取无风险利率 — 短期期权对利率不敏感，固定值足够

## 详细设计

### 1. 数据层 — option_bars 加列

option_bars 新增三列（data_store.py）：

```sql
ALTER TABLE option_bars ADD COLUMN strike DOUBLE;
ALTER TABLE option_bars ADD COLUMN expiration DATE;
ALTER TABLE option_bars ADD COLUMN option_type VARCHAR(1);  -- 'P' or 'C'
```

- 新数据入库时从 OCC symbol 解析并填入
- 存量数据通过一次性迁移脚本补填（遍历已有记录，解析 symbol 回填）

OCC symbol 解析规则：
```
"O:TQQQ260424P00030000"
  → ticker=TQQQ, expiration=2026-04-24, type=P, strike=30.0

格式: O:{TICKER}{YYMMDD}{P|C}{STRIKE*1000 补零到 8 位}
```

### 2. 数据层 — ticker_iv 表

新建 `ticker_iv` 表（data_store.py）：

```sql
CREATE TABLE IF NOT EXISTS ticker_iv (
    date    DATE     NOT NULL,
    ticker  VARCHAR  NOT NULL,
    iv      DOUBLE   NOT NULL,
    PRIMARY KEY (date, ticker)
)
```

### 3. IV 计算模块（新建 iv.py）

#### 3.1 OCC Symbol 解析

```python
def parse_occ_symbol(symbol: str) -> dict:
    """解析 OCC symbol，返回 ticker, expiration, option_type, strike"""
```

#### 3.2 Black-Scholes 反算

```python
def bs_implied_vol(price: float, spot: float, strike: float,
                   tte: float, r: float, option_type: str) -> float:
    """二分法/Newton 法反算 IV，输入期权价格，输出隐含波动率"""
```

- `tte`: time to expiry，年化（交易日 / 252）
- `r`: 固定 0.05
- 无法收敛时（深度 OTM 合约价格极低）返回 NaN，后续过滤

#### 3.3 合约筛选

```python
def select_contracts(option_bars: list, spot: float, date: date) -> list:
    """筛选 ATM 附近、最近两个到期日的合约"""
```

筛选规则：
1. 排除到期日距 `date` ≤ 7 天的合约（临期噪音大）
2. 取最近的两个到期日
3. 每个到期日取 strike 最接近 spot 的 Put + Call 各 3-5 档
4. 最终约 10-20 个合约

#### 3.4 加权汇总

```python
def compute_ticker_iv(option_bars: list, spot: float, date: date) -> float:
    """VIX 风格加权，输出标的级 IV"""
```

加权方式：
1. 对筛选出的每个合约做 B-S 反算得到单合约 IV
2. 过滤 NaN 和异常值（IV < 0.01 或 > 5.0）
3. 同一到期日内按等权平均（ATM 附近 delta 接近，vega 差异小）
4. 近月/远月两个到期日线性插值到 30 天期限，得到最终 IV

### 4. 同步集成（data_sync.py）

在 `ensure_synced()` 流程末尾新增 IV 计算步骤：

```
ensure_synced() 现有流程
  → 同步 splits
  → 同步 equity_bars
  → 同步 option_bars
  → [新增] 计算 ticker_iv
```

计算逻辑：
- 查 `ticker_iv` 表该 ticker 最新日期
- 空表 → 取 option_bars 最早日期，全量回算
- 有数据 → 从最新日期次日开始增量计算
- 逐日计算：查当天 option_bars + equity_bars close → `compute_ticker_iv()` → 写入

拆股联动：
- 现有逻辑发现新拆股事件时会清空该 ticker 的 option_bars + equity_bars 并全量重拉
- 需同步清空该 ticker 的 `ticker_iv` 记录，重拉完成后自动触发全量回算（因为表为空，走全量路径）

### 5. 入库流程改造

**s3_downloader.py / flat_file_fetcher.py：**
- `insert_option_bars_from_csv()` 入库前调用 `parse_occ_symbol()` 填充 strike/expiration/option_type
- 复权逻辑（调整 strike）保持不变，在解析之后执行

**存量迁移：**
- `data_store.py` 新增迁移函数，检测 strike 列是否全为 NULL
- 全为 NULL 时遍历 option_bars，解析 symbol 回填三列
- 在 `ensure_synced()` 开头执行（仅首次生效）

## 涉及文件

| 文件 | 改动 |
|------|------|
| `data_store.py` | option_bars 加列 + 迁移、新建 ticker_iv 表、查询/写入接口 |
| `iv.py`（新建） | OCC 解析、B-S 反算、合约筛选、VIX 风格加权 |
| `data_sync.py` | ensure_synced 末尾调用 IV 计算 |
| `s3_downloader.py` | 入库时填充 strike/expiration/option_type |
| `flat_file_fetcher.py` | CSV 解析时提取并返回新字段 |

## 验证方案

### 单元测试

1. **OCC symbol 解析** — 覆盖正常格式、拆股调整后的 symbol，验证 strike/expiry/type 正确
2. **B-S 反算精度** — 用已知 IV 正向算期权价格，再反算回 IV，误差 < 0.01%
3. **合约筛选** — mock option_bars 数据，验证正确选出最近两个到期日 + ATM 附近合约
4. **加权汇总** — 固定输入验证最终 IV 值
5. **合理性断言** — IV > 0、无 NaN、在合理区间内

### 人工验证

1. **对比 VXN 指数** — TQQQ IV ≈ QQQ IV × 3，走势形态应高度相关
2. **对比 HV** — IV 长期趋势应接近 hist_vol，通常略高（波动率溢价）
3. **极端日检查** — 市场大跌日（如 2024-08 日本加息恐慌）IV 应明显飙升
4. **单合约抽检** — 挑几个日期手动 B-S 验算
