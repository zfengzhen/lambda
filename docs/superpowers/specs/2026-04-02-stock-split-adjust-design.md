# 拆股前复权调整设计文档

## 背景

TQQQ 于 2025-11-20 执行 1:2 拆股。当前系统以 `adjusted=false` 存储原始价格，所有跨拆股日期的策略计算（MA/MACD/Pivot、分层判定、Strike 定价、回测）均会失真。需引入拆股数据，对历史价格做前复权处理。

## 决策摘要

| 决策项 | 选择 |
|--------|------|
| 复权方向 | 前复权（当前价格为真实价格，历史价格按比例调低） |
| 调整时机 | 入库时调整（DB 始终存前复权数据） |
| 拆股数据来源 | Massive API `/v2/stocks/corporate-actions/splits` |
| 调整范围 | equity_bars + option_bars 均调整 |
| 已有数据处理 | 发现新拆股事件时清空重拉 |
| 同步时机 | 集成到 `ensure_synced` 流程 |
| 复权方式 | equity: API `adjusted=true` 直接返回；option: 自行按因子调整 |

## 方案选型

**选定方案：API 端复权 + 期权自行调整**

- 股票数据：Massive API 支持 `adjusted=true`，直接返回前复权价格，无需自行计算
- 期权数据：S3 flat files 无复权参数，入库时根据 splits 表自行计算因子并调整

淘汰方案：全部自行复权 — 股票和期权统一自己算复权因子。缺点是多了一层不必要的计算逻辑，且容易与 API 自身的复权结果产生微小精度差异。

## 详细设计

### 1. 数据层 — splits 表

新增 `splits` 表（data_store.py）：

```sql
CREATE TABLE IF NOT EXISTS splits (
    ticker       VARCHAR  NOT NULL,
    exec_date    DATE     NOT NULL,   -- 拆股执行日
    split_from   INTEGER  NOT NULL,   -- 拆前份数（如 1）
    split_to     INTEGER  NOT NULL,   -- 拆后份数（如 2）
    PRIMARY KEY (ticker, exec_date)
)
```

新增拆股数据拉取（rest_downloader.py）：
- 调用 `/v2/stocks/corporate-actions/splits` 获取指定 ticker 的拆股历史
- 写入 splits 表（INSERT OR IGNORE，幂等）

### 2. equity_bars 复权 — API 端直接返回

rest_downloader.py 修改：
- `download_and_store_equity()` 的 API 参数从 `adjusted=false` 改为 `adjusted=true`
- API 直接返回前复权价格（open/high/low/close/vwap），入库逻辑不变

清空重拉触发条件：
- `ensure_synced()` 先同步 splits 表
- 对比 DB 中已有的 splits 记录与 API 最新返回
- 发现新增拆股事件时：删除该 ticker 的 equity_bars + sync_log，重新全量拉取

### 3. option_bars 复权 — 自行按因子调整

期权数据来自 S3 flat files，无法通过 API 参数获取复权数据，入库时自行调整。

复权规则（以 1:2 拆股为例）：

| 字段 | 调整公式 | 示例（拆前→拆后） |
|------|---------|-----------------|
| strike | strike × (split_from / split_to) | $100 → $50 |
| open/high/low/close | price × (split_from / split_to) | $5.00 → $2.50 |
| volume | volume × (split_to / split_from) | 100 → 200 |

关键细节：
- **调整范围**：只调整 `exec_date` 之前的期权记录
- **因子方向**：价格类字段乘以 `split_from/split_to`（缩小），数量类字段乘以 `split_to/split_from`（放大）
- **多次拆股**：因子累乘。如先 1:2 再 1:3，拆股前最早期的价格因子 = (1/2) × (1/3) = 1/6
- **精度**：strike 保留 2 位小数，期权价格保留 2 位小数，volume 取整

实现位置（s3_downloader.py）：
- `insert_option_bars_from_csv()` 写入前，查询该 ticker 的 splits 表
- 对每条记录根据其日期计算累积因子，调整价格和数量字段后再入库

清空重拉：
- 与 equity_bars 相同，发现新拆股事件时删除 option_bars + sync_log（`data_type='option_month'`），重新全量拉取

### 4. ensure_synced 流程变更

当前流程：
```
ensure_synced() → 检查 equity_bars 最新日期 → 增量/全量同步 equity + option
```

新流程：
```
ensure_synced()
  ├── 1. 同步 splits 表（每个 ticker 调用 API 拉最新拆股数据）
  ├── 2. 检测新增拆股事件
  │     ├── 有新拆股 → 清空该 ticker 的 equity_bars + option_bars + sync_log
  │     └── 无新拆股 → 跳过
  ├── 3. 同步 equity_bars（adjusted=true，逻辑同现有增量/全量）
  └── 4. 同步 option_bars（入库时按 splits 因子调整）
```

新拆股检测逻辑：
- API 返回的 splits 列表与 DB 中 splits 表对比
- 存在 DB 中没有的新记录 → 判定为新拆股事件
- 写入 splits 表后执行清空重拉

性能考虑：
- splits API 调用轻量（每 ticker 一次，返回数据极少）
- 无新拆股时仅多一次 splits 查询，< 1 秒
- 有新拆股时全量重拉是必要的一次性成本

### 5. 策略层与可视化 — 无需修改

- DB 中已是前复权价格，所有计算直接可用，**零改动**
- strategy.py / indicators.py / run.py / template.html 均不修改
- 报告中的 strike 为前复权值，实际交易下单需用真实市场 strike（当前系统仅做策略研究，不影响）

## 受影响文件

| 文件 | 变更类型 |
|------|---------|
| data_store.py | 新增 splits 表建表、查询、写入方法 |
| rest_downloader.py | 新增 splits API 拉取；equity `adjusted=false` → `adjusted=true` |
| s3_downloader.py | 入库前按 splits 因子调整期权价格和数量 |
| data_sync.py | ensure_synced 新增 splits 同步 + 新拆股检测 + 清空重拉逻辑 |

## 测试要点

- splits 表 CRUD 和幂等性
- 复权因子计算（单次拆股、多次拆股）
- 期权价格/strike/volume 调整精度
- 新拆股检测 → 清空重拉完整流程
- 无新拆股时 ensure_synced 性能无回退
