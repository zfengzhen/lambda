# Local Data Store 设计文档

## 概述

构建一个本地 DuckDB 数据库，通过 S3 Flat Files（期权）和 REST API（股票）两条通道统一存储历史行情数据，替代现有的 JSON 文件和实时 API 调用，供 `run.py` 和 `entry_optimizer.py` 查询使用。

---

## 背景与动机

当前数据访问存在两个问题：

1. **股票日K**：`run.py` 每次通过 REST API 拉取，受网络和限频影响
2. **期权日K**：`entry_optimizer.py` 依赖 S3 Flat Files 按日下载（每天 ~2 MB），每次运行都重复过滤 ~26 万行；历史数据只有 REST API 近 4 个月，导致大量信号周无数据

本地数据库解决方案：一次性下载存储，后续查询本地直接返回。

---

## 数据来源与限制

| 数据类型 | 来源 | 历史深度 | 覆盖范围 |
|---|---|---|---|
| 期权日K | S3 `us_options_opra/day_aggs_v1/` | 滚动 2 年（套餐限制） | 全美所有期权合约，不过滤 |
| 股票日K | REST API `/v2/aggs/ticker/{ticker}/range/1/day/` | 2 年 | 仅指定标的（TQQQ/QQQ 等） |

- S3 数据 T+1 11:00 AM ET 可用，无需 REST 补缺
- 股票数据量极小，REST API 完全够用

---

## 模块设计

```
data_store.py      — DuckDB 连接、建表、读写接口
s3_downloader.py   — 下载 S3 期权 Flat Files，写入 option_bars
rest_downloader.py — REST API 拉取指定股票日K，写入 equity_bars
data_sync.py       — CLI 编排，调度以上两个下载器
```

---

## 数据库表结构

### equity_bars（股票日K）

```sql
CREATE TABLE equity_bars (
    date         DATE     NOT NULL,
    ticker       VARCHAR  NOT NULL,
    open         DOUBLE   NOT NULL,
    high         DOUBLE   NOT NULL,
    low          DOUBLE   NOT NULL,
    close        DOUBLE   NOT NULL,
    volume       BIGINT,
    vwap         DOUBLE,
    transactions INTEGER,
    PRIMARY KEY (date, ticker)
);
```

### option_bars（期权日K）

```sql
CREATE TABLE option_bars (
    date         DATE     NOT NULL,
    symbol       VARCHAR  NOT NULL,   -- OCC 格式：O:TQQQ250131P00038500
    open         DOUBLE   NOT NULL,
    high         DOUBLE   NOT NULL,
    low          DOUBLE   NOT NULL,
    close        DOUBLE   NOT NULL,
    volume       BIGINT,
    transactions INTEGER,
    PRIMARY KEY (date, symbol)
);
```

### sync_log（下载记录）

```sql
CREATE TABLE sync_log (
    id           INTEGER  PRIMARY KEY,
    ts           TIMESTAMP NOT NULL,
    date         DATE     NOT NULL,
    data_type    VARCHAR  NOT NULL,   -- 'equity' | 'option'
    rows_written INTEGER  NOT NULL,
    status       VARCHAR  NOT NULL,   -- 'ok' | 'error'
    message      VARCHAR              -- 错误信息，可空
);
```

---

## 数据流

### 全量建库

```
python data_sync.py --years 2 --tickers TQQQ QQQ

1. 生成过去 N 年的交易日列表
2. s3_downloader:
   逐日下载 us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
   → 全量写入 option_bars（已在库中的日期跳过）
3. rest_downloader:
   按指定 tickers 拉取日K
   → 写入 equity_bars
4. 写 sync_log 记录
```

### 增量同步

```
python data_sync.py --incremental --tickers TQQQ QQQ

1. 查询 sync_log 找到 option_bars 最新已同步日期
2. 从次日到昨天（T-1）补齐缺失日期
3. equity_bars 同样增量补齐
```

---

## 存储量估算

| 表 | 行数（2年）| DuckDB 压缩估算 |
|---|---|---|
| option_bars | ~1.3 亿（260K/天 × 500 天）| ~4 GB |
| equity_bars | 极小（几个标的）| < 1 MB |

---

## 调用方集成

### entry_optimizer.py

`enrich_with_flat_files()` 改为查询本地 DB：

```python
# 现在：逐日下载 S3 文件再过滤
fetch_option_bars_flat(symbol, from_date, to_date, s3_client)

# 改为：
from data_store import query_option_bars
bars = query_option_bars(symbol, from_date, to_date)
```

### run.py

`fetch_daily_bars()` 改为优先查本地 DB，缺失日期再调 REST API 补取：

```python
from data_store import query_equity_bars
bars = query_equity_bars(ticker, from_date, to_date)
```

---

## CLI 接口

```bash
# 全量建库（首次）
python data_sync.py --years 2 --tickers TQQQ QQQ

# 指定年数（可按需扩展）
python data_sync.py --years 1 --tickers TQQQ

# 增量补齐到昨天
python data_sync.py --incremental --tickers TQQQ QQQ

# 仅同步指定类型
python data_sync.py --incremental --type option
python data_sync.py --incremental --type equity --tickers TQQQ
```

---

## 测试策略

- `data_store.py`：用 tmp_path 创建临时 DuckDB，测试建表、插入、查询、upsert
- `s3_downloader.py`：mock boto3 客户端，测试日期生成、跳过已有数据、写入逻辑
- `rest_downloader.py`：mock requests，测试按 ticker 拉取、写入逻辑
- `data_sync.py`：集成测试，mock 下载器，验证编排逻辑和 CLI 参数解析

---

## 不在范围内

- 分钟K、Quotes、Trades 数据
- 股票 S3 数据（无权限）
- 实时数据推送
- 多用户/多进程并发写入
