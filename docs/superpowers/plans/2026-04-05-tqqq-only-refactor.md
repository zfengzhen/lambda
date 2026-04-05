# TQQQ-Only 整体重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将项目从多标的架构重构为 TQQQ 专用三层架构，保持 HTML 报告功能不变。

**Architecture:** 三层分离（CLI 薄壳 → 业务逻辑 → 输出），数据层从单文件 data_store.py (739行) 拆为 store/schema/queries/writers 四模块，同步层合并 s3_downloader+flat_file_fetcher，消除全局 ticker 参数传递。

**Tech Stack:** Python 3.12+, DuckDB, pandas, boto3, exchange_calendars, scipy

**Design spec:** `docs/superpowers/specs/2026-04-05-tqqq-only-refactor-design.md`

---

## 文件结构

### 新建文件

| 文件 | 职责 | 来源 |
|------|------|------|
| `config.py` | 全局常量 | 新建 + strategy.py 常量 |
| `data/__init__.py` | 包初始化 | 新建 |
| `data/store.py` | DuckDB 连接管理 | data_store.py:80-84 |
| `data/schema.py` | DDL + 迁移 | data_store.py:14-153 |
| `data/queries.py` | 所有 SELECT | data_store.py:394-739 (查询部分) |
| `data/writers.py` | 所有 INSERT/UPSERT | data_store.py:156-391 (写入部分) |
| `data/sync/__init__.py` | 包初始化 | 新建 |
| `data/sync/orchestrator.py` | ensure_synced 编排 | data_sync.py:24-81 |
| `data/sync/equity.py` | REST 股票日K | rest_downloader.py:21-83 |
| `data/sync/options.py` | S3 期权下载 | s3_downloader.py + flat_file_fetcher.py |
| `data/sync/splits.py` | 拆股检测 | rest_downloader.py:86-131 |
| `data/sync/iv.py` | IV 计算 | iv.py + data_sync.py:83-163 |
| `core/__init__.py` | 包初始化 | 新建 |
| `core/indicators.py` | MA/MACD/Pivot | indicators.py (几乎不变) |
| `core/strategy.py` | 决策树+周分组 | strategy.py (减去回测/OTM函数) |
| `core/backtest.py` | 回测引擎 | strategy.py:215-390 + run.py:69-129,131-326 |
| `core/options.py` | OCC 解析+合约匹配 | iv.py:17-41 + data_store.py:420-501 |
| `core/circuit_breaker.py` | 熔断逻辑 | run.py:154-171 |
| `output/__init__.py` | 包初始化 | 新建 |
| `output/report.py` | JSON+HTML 生成 | run.py:329-364 |
| `output/deploy.py` | Cloudflare+Telegram | deploy.py (去掉 --ticker) |
| `cli/__init__.py` | 包初始化 | 新建 |
| `cli/run.py` | 策略生成入口 | run.py:367-398 (瘦身) |
| `cli/deploy.py` | 部署入口 | deploy.py:226-271 (瘦身) |
| `cli/sync.py` | 数据同步入口 | data_sync.py:165-185 (瘦身) |

### 移动文件

| 文件 | 操作 |
|------|------|
| `template.html` | 移到 `output/template.html` |

### 删除文件（最终阶段）

`run.py`, `deploy.py`, `strategy.py`, `indicators.py`, `iv.py`, `data_sync.py`, `data_store.py`, `s3_downloader.py`, `flat_file_fetcher.py`, `rest_downloader.py`

---

### Task 1: 创建 config.py 和包结构

**Files:**
- Create: `config.py`
- Create: `data/__init__.py`, `data/sync/__init__.py`, `core/__init__.py`, `output/__init__.py`, `cli/__init__.py`

- [ ] **Step 1: 创建 config.py**

```python
"""全局常量：TQQQ 专用配置。"""
from pathlib import Path

TICKER = "TQQQ"
LEVERAGE = 3
EXPIRY_WEEKS = 4
TRADING_DAYS_YEAR = 252

# 策略 OTM 表（3x 杠杆基准值）
DEFAULT_OTM = {
    "A": 0.08,
    "B1": 0.08, "B2": 0.08, "B3": 0.12, "B4": 0.15,
    "C1": 0.12, "C2": 0.15, "C3": 0.15, "C4": 0.20,
}

# 层级中文名
TIER_NAMES = {
    "A": "企稳双撑",
    "B1": "回调均线", "B2": "超跌支撑", "B3": "趋势动能弱", "B4": "低波整理",
    "C1": "跌势减速", "C2": "趋势延续", "C3": "过热追涨", "C4": "加速下杀",
}

ALL_TIERS = ["A", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4"]

# 数据库
DB_PATH = Path(__file__).parent / "output" / "market_data.duckdb"

# S3 默认值
S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"

# REST API
REST_BASE_URL = "https://api.massive.com"
REST_MAX_RETRIES = 5
REST_RETRY_DELAY = 15

# IV 计算参数
RISK_FREE_RATE = 0.05
IV_MIN_DTE = 7
IV_TARGET_DAYS = 30

# 数据同步
FULL_SYNC_YEARS = 2
```

- [ ] **Step 2: 创建包 __init__.py 文件**

创建 5 个空 `__init__.py`：`data/__init__.py`, `data/sync/__init__.py`, `core/__init__.py`, `output/__init__.py`, `cli/__init__.py`

- [ ] **Step 3: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 config.py 全局常量和包目录结构
```

---

### Task 2: 创建 data/store.py 和 data/schema.py

**Files:**
- Create: `data/store.py`
- Create: `data/schema.py`

- [ ] **Step 1: 创建 data/store.py**

从 `data_store.py:80-84` 提取连接管理，引用 `config.DB_PATH`：

```python
"""DuckDB 连接管理。"""
import logging

import duckdb

from config import DB_PATH

logger = logging.getLogger(__name__)


def get_connection() -> duckdb.DuckDBPyConnection:
    """打开数据库连接，自动创建 output 目录。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    """建表 + 迁移 + 存量回填，程序启动时调用一次。"""
    from data.schema import create_tables, run_migrations
    from data.writers import backfill_option_bars_columns

    con = get_connection()
    try:
        create_tables(con)
        run_migrations(con)
    finally:
        con.close()
    backfill_option_bars_columns()
    logger.info(f"DB 初始化完成: {DB_PATH}")
```

- [ ] **Step 2: 创建 data/schema.py**

从 `data_store.py:14-78` 提取所有 DDL，加上 `data_store.py:86-96,134-152` 的迁移逻辑：

```python
"""DuckDB 表结构定义与迁移。"""
import logging

import duckdb

logger = logging.getLogger(__name__)

_CREATE_EQUITY = """
CREATE TABLE IF NOT EXISTS equity_bars (
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
)
"""

_CREATE_OPTION = """
CREATE TABLE IF NOT EXISTS option_bars (
    date         DATE     NOT NULL,
    symbol       VARCHAR  NOT NULL,
    open         DOUBLE   NOT NULL,
    high         DOUBLE   NOT NULL,
    low          DOUBLE   NOT NULL,
    close        DOUBLE   NOT NULL,
    volume       BIGINT,
    transactions INTEGER,
    strike       DOUBLE,
    expiration   DATE,
    option_type  VARCHAR(1),
    PRIMARY KEY (date, symbol)
)
"""

_CREATE_SYNC_LOG = """
CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1;
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER   DEFAULT nextval('sync_log_seq'),
    ts           TIMESTAMP NOT NULL,
    date         DATE      NOT NULL,
    data_type    VARCHAR   NOT NULL,
    ticker       VARCHAR,
    rows_written INTEGER   NOT NULL,
    status       VARCHAR   NOT NULL,
    message      VARCHAR
)
"""

_CREATE_SPLITS = """
CREATE TABLE IF NOT EXISTS splits (
    ticker       VARCHAR  NOT NULL,
    exec_date    DATE     NOT NULL,
    split_from   INTEGER  NOT NULL,
    split_to     INTEGER  NOT NULL,
    PRIMARY KEY (ticker, exec_date)
)
"""

_CREATE_TICKER_IV = """
CREATE TABLE IF NOT EXISTS ticker_iv (
    date    DATE     NOT NULL,
    ticker  VARCHAR  NOT NULL,
    iv      DOUBLE   NOT NULL,
    PRIMARY KEY (date, ticker)
)
"""


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    """建表（幂等，已存在则跳过）。"""
    con.execute(_CREATE_EQUITY)
    con.execute(_CREATE_OPTION)
    con.execute(_CREATE_SYNC_LOG)
    con.execute(_CREATE_SPLITS)
    con.execute(_CREATE_TICKER_IV)


def run_migrations(con: duckdb.DuckDBPyConnection) -> None:
    """执行数据库迁移（幂等）。"""
    _migrate_option_bars(con)
    _migrate_sync_log_ticker(con)


def _migrate_option_bars(con: duckdb.DuckDBPyConnection) -> None:
    """为存量 option_bars 添加 strike/expiration/option_type 列。"""
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'option_bars'"
    ).fetchall()}
    if "strike" not in cols:
        con.execute("ALTER TABLE option_bars ADD COLUMN strike DOUBLE")
        con.execute("ALTER TABLE option_bars ADD COLUMN expiration DATE")
        con.execute("ALTER TABLE option_bars ADD COLUMN option_type VARCHAR(1)")
        logger.info("[schema] option_bars 迁移：添加 strike/expiration/option_type 列")


def _migrate_sync_log_ticker(con: duckdb.DuckDBPyConnection) -> None:
    """为存量 sync_log 添加 ticker 列。"""
    try:
        con.execute("ALTER TABLE sync_log ADD COLUMN ticker VARCHAR")
    except duckdb.CatalogException:
        pass
```

- [ ] **Step 3: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 data/store.py 连接管理和 data/schema.py 表结构
```

---

### Task 3: 创建 data/queries.py

**Files:**
- Create: `data/queries.py`

- [ ] **Step 1: 创建 data/queries.py**

从 `data_store.py` 提取所有 SELECT 操作，去掉 ticker 参数（改用 `config.TICKER`）：

```python
"""DuckDB 查询操作（只读）。"""
import logging

import duckdb

from config import TICKER, DB_PATH
from data.store import get_connection

logger = logging.getLogger(__name__)


def query_equity_bars(from_date: str, to_date: str) -> list[dict]:
    """查询 TQQQ 在日期范围内的日K数据。

    Returns:
        [{date, ticker, open, high, low, close, volume, vwap, transactions}] 按日期升序
    """
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT date, ticker, open, high, low, close, volume, vwap, transactions
            FROM equity_bars
            WHERE ticker = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [TICKER, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "ticker": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6],
         "vwap": r[7], "transactions": r[8]}
        for r in rows
    ]


def query_option_bars(symbol: str, from_date: str, to_date: str) -> list[dict]:
    """查询指定期权合约在日期范围内的日K数据。

    Returns:
        [{date, symbol, open, high, low, close}] 按日期升序
    """
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT date, symbol, open, high, low, close
            FROM option_bars
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [symbol, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "symbol": r[1], "open": r[2],
         "high": r[3], "low": r[4], "close": r[5]}
        for r in rows
    ]


def query_option_on_date(entry_date: str, expiry_date: str,
                         strike: float) -> dict | None:
    """查询最接近目标行权价的 Put 期权在入场日的价格。

    在候选到期日（精确日 ±3 天）中查找所有可用 Put 合约，
    取 strike ≤ 目标值且最接近的（向下匹配）。

    Returns:
        {symbol, date, open, high, low, close, volume, vwap} 或 None
    """
    from datetime import datetime, timedelta

    base = datetime.strptime(expiry_date, "%Y-%m-%d")
    candidates = [expiry_date]
    for offset in range(1, 4):
        candidates.append((base - timedelta(days=offset)).strftime("%Y-%m-%d"))
        candidates.append((base + timedelta(days=offset)).strftime("%Y-%m-%d"))

    patterns = []
    for exp in candidates:
        yy, mm, dd = exp[2:4], exp[5:7], exp[8:10]
        patterns.append(f"O:{TICKER}{yy}{mm}{dd}P%")

    con = get_connection()
    try:
        placeholders = " OR ".join(["symbol LIKE ?"] * len(patterns))
        rows = con.execute(
            f"""
            SELECT symbol, date, open, high, low, close, volume
            FROM option_bars
            WHERE ({placeholders}) AND date = ?
            """,
            patterns + [entry_date],
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return None

    def _extract_strike(symbol: str) -> float:
        return int(symbol[-8:]) / 1000.0

    below = [r for r in rows if _extract_strike(r[0]) <= strike]
    if below:
        best = max(below, key=lambda r: _extract_strike(r[0]))
    else:
        best = min(rows, key=lambda r: abs(_extract_strike(r[0]) - strike))

    vol = best[6] or 0
    vwap = round((best[4] + best[5] + best[3]) / 3, 4) if vol > 0 else best[5]
    return {
        "symbol": best[0], "date": str(best[1]),
        "open": best[2], "high": best[3], "low": best[4], "close": best[5],
        "volume": vol, "vwap": round(vwap, 4),
    }


def query_ticker_iv(from_date: str, to_date: str) -> list[dict]:
    """查询 TQQQ 的 IV 数据。

    Returns:
        [{date, ticker, iv}] 按日期升序
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT date, ticker, iv FROM ticker_iv "
            "WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date",
            [TICKER, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [{"date": str(r[0]), "ticker": r[1], "iv": r[2]} for r in rows]


def query_splits() -> list[dict]:
    """查询 TQQQ 的所有拆股记录，按执行日期升序。

    Returns:
        [{ticker, exec_date, split_from, split_to}]
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT ticker, exec_date, split_from, split_to "
            "FROM splits WHERE ticker = ? ORDER BY exec_date",
            [TICKER],
        ).fetchall()
    finally:
        con.close()
    return [
        {"ticker": r[0], "exec_date": str(r[1]),
         "split_from": r[2], "split_to": r[3]}
        for r in rows
    ]


def get_latest_equity_date() -> str | None:
    """返回 TQQQ 在 equity_bars 中的最新日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MAX(date) FROM equity_bars WHERE ticker = ?",
            [TICKER],
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_latest_iv_date() -> str | None:
    """返回 TQQQ 在 ticker_iv 中的最新日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MAX(date) FROM ticker_iv WHERE ticker = ?", [TICKER]
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_latest_option_date() -> str | None:
    """返回 option_bars 中 TQQQ 合约的最新日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MAX(date) FROM option_bars WHERE symbol LIKE ?",
            [f"O:{TICKER}%"],
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_earliest_option_date() -> str | None:
    """返回 option_bars 中 TQQQ 合约的最早日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MIN(date) FROM option_bars WHERE symbol LIKE ?",
            [f"O:{TICKER}%"],
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_option_dates_in_range(from_date: str, to_date: str) -> list[str]:
    """返回日期范围内有 TQQQ 期权数据的所有日期。"""
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT DISTINCT date FROM option_bars "
            "WHERE symbol LIKE ? AND date BETWEEN ? AND ? ORDER BY date",
            [f"O:{TICKER}%", from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [str(r[0]) for r in rows]


def query_option_bars_for_iv(date_str: str) -> list[dict]:
    """查询指定日期的 TQQQ 全部期权数据，用于 IV 计算。"""
    con = get_connection()
    try:
        bars = con.execute(
            "SELECT date, symbol, open, high, low, close, volume, "
            "transactions, strike, expiration, option_type "
            "FROM option_bars "
            "WHERE symbol LIKE ? AND date = ?",
            [f"O:{TICKER}%", date_str],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "symbol": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6], "transactions": r[7],
         "strike": r[8], "expiration": str(r[9]) if r[9] else None,
         "option_type": r[10]}
        for r in bars
    ]


def is_synced(date_str: str, data_type: str) -> bool:
    """检查指定日期和类型是否已在 sync_log 中有 ok 记录。

    对 option_month 类型自动按 TQQQ 过滤。
    """
    if not DB_PATH.exists():
        return False
    con = get_connection()
    try:
        if data_type == "option_month":
            result = con.execute(
                "SELECT COUNT(*) FROM sync_log "
                "WHERE date=? AND data_type=? AND ticker=? AND status='ok'",
                [date_str, data_type, TICKER],
            ).fetchone()[0]
        else:
            result = con.execute(
                "SELECT COUNT(*) FROM sync_log "
                "WHERE date=? AND data_type=? AND status='ok'",
                [date_str, data_type],
            ).fetchone()[0]
    except duckdb.CatalogException:
        return False
    finally:
        con.close()
    return result > 0


def compute_split_factor(date_str: str) -> float:
    """计算 TQQQ 在指定日期的前复权累积因子。

    因子 = ∏(split_from / split_to)，对所有 exec_date > date_str 的拆股事件累乘。
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT split_from, split_to FROM splits "
            "WHERE ticker = ? AND exec_date > CAST(? AS DATE) "
            "ORDER BY exec_date",
            [TICKER, date_str],
        ).fetchall()
    finally:
        con.close()
    factor = 1.0
    for split_from, split_to in rows:
        factor *= split_from / split_to
    return factor
```

- [ ] **Step 2: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 data/queries.py，所有查询去掉 ticker 参数
```

---

### Task 4: 创建 data/writers.py

**Files:**
- Create: `data/writers.py`

- [ ] **Step 1: 创建 data/writers.py**

从 `data_store.py` 提取所有写操作。关键变化：
- `insert_option_bars_from_csv` 去掉 `tickers` 参数，硬编码 `TICKER` 过滤
- `delete_ticker_data` → `delete_all_data`，不再按 ticker 过滤
- `upsert_equity_bars` / `upsert_splits` 不变（rows 中已含 ticker）
- `compute_split_factor` 引用 `data.queries`

```python
"""DuckDB 写入操作。"""
import logging
from pathlib import Path

from config import TICKER
from data.store import get_connection

logger = logging.getLogger(__name__)


def upsert_equity_bars(rows: list[dict]) -> int:
    """批量写入/更新股票日K。主键冲突时覆盖。

    Args:
        rows: list of {date, ticker, open, high, low, close, volume, vwap, transactions}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = get_connection()
    try:
        con.executemany(
            """
            INSERT INTO equity_bars
                (date, ticker, open, high, low, close, volume, vwap, transactions)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (date, ticker) DO UPDATE SET
                open         = excluded.open,
                high         = excluded.high,
                low          = excluded.low,
                close        = excluded.close,
                volume       = excluded.volume,
                vwap         = excluded.vwap,
                transactions = excluded.transactions
            """,
            [(r["date"], r["ticker"], r["open"], r["high"], r["low"],
              r["close"], r.get("volume"), r.get("vwap"), r.get("transactions"))
             for r in rows],
        )
    finally:
        con.close()
    return len(rows)


def upsert_splits(rows: list[dict]) -> int:
    """批量写入拆股记录（主键冲突时忽略）。"""
    if not rows:
        return 0
    con = get_connection()
    try:
        con.executemany(
            """
            INSERT OR IGNORE INTO splits (ticker, exec_date, split_from, split_to)
            VALUES (?, ?, ?, ?)
            """,
            [(r["ticker"], r["exec_date"], r["split_from"], r["split_to"])
             for r in rows],
        )
    finally:
        con.close()
    return len(rows)


def upsert_ticker_iv(rows: list[dict]) -> int:
    """批量写入/更新 ticker IV。主键冲突时覆盖。"""
    if not rows:
        return 0
    con = get_connection()
    try:
        con.executemany(
            """
            INSERT INTO ticker_iv (date, ticker, iv)
            VALUES (?, ?, ?)
            ON CONFLICT (date, ticker) DO UPDATE SET iv = excluded.iv
            """,
            [(r["date"], r["ticker"], r["iv"]) for r in rows],
        )
    finally:
        con.close()
    return len(rows)


def insert_option_bars_from_csv(csv_path: "Path", date_str: str) -> int:
    """从 gzip CSV 文件批量写入 option_bars（TQQQ 合约专用）。

    自动检测拆股记录，对拆股前的历史数据做前复权调整。

    Returns:
        写入行数
    """
    from data.queries import compute_split_factor

    where_sql = f"WHERE ticker LIKE 'O:{TICKER}%'"

    # 计算 TQQQ 的拆股因子
    factor = compute_split_factor(date_str)

    if factor == 1.0:
        # 无拆股调整
        sql = f"""
            INSERT OR IGNORE INTO option_bars
                (date, symbol, open, high, low, close, volume, transactions,
                 strike, expiration, option_type)
            SELECT
                CAST('{date_str}' AS DATE),
                ticker,
                CAST(open AS DOUBLE),
                CAST(high AS DOUBLE),
                CAST(low  AS DOUBLE),
                CAST(close AS DOUBLE),
                TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT),
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT),
                CAST(substr(ticker, length(ticker) - 7) AS DOUBLE) / 1000.0,
                CAST('20' || substr(ticker, length(ticker) - 14, 2) || '-'
                     || substr(ticker, length(ticker) - 12, 2) || '-'
                     || substr(ticker, length(ticker) - 10, 2) AS DATE),
                substr(ticker, length(ticker) - 8, 1)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """
    else:
        # 有拆股调整：应用因子
        pf = factor
        vf = 1.0 / pf
        like = f"ticker LIKE 'O:{TICKER}%'"

        symbol_expr = (
            f"CASE WHEN {like} THEN "
            f"substr(ticker, 1, length(ticker) - 8) || "
            f"lpad(CAST(CAST(ROUND("
            f"CAST(substr(ticker, length(ticker) - 7) AS BIGINT) * {pf}"
            f") AS BIGINT) AS VARCHAR), 8, '0') "
            f"ELSE ticker END"
        )
        open_expr = f"CASE WHEN {like} THEN ROUND(CAST(open AS DOUBLE) * {pf}, 2) ELSE CAST(open AS DOUBLE) END"
        high_expr = f"CASE WHEN {like} THEN ROUND(CAST(high AS DOUBLE) * {pf}, 2) ELSE CAST(high AS DOUBLE) END"
        low_expr = f"CASE WHEN {like} THEN ROUND(CAST(low AS DOUBLE) * {pf}, 2) ELSE CAST(low AS DOUBLE) END"
        close_expr = f"CASE WHEN {like} THEN ROUND(CAST(close AS DOUBLE) * {pf}, 2) ELSE CAST(close AS DOUBLE) END"
        vol_expr = (
            f"CASE WHEN {like} THEN CAST(ROUND("
            f"TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) * {vf}) AS BIGINT) "
            f"ELSE TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) END"
        )
        strike_expr = (
            f"CASE WHEN {like} THEN "
            f"CAST(substr(ticker, length(ticker) - 7) AS DOUBLE) / 1000.0 * {pf} "
            f"ELSE CAST(substr(ticker, length(ticker) - 7) AS DOUBLE) / 1000.0 END"
        )

        sql = f"""
            INSERT OR IGNORE INTO option_bars
                (date, symbol, open, high, low, close, volume, transactions,
                 strike, expiration, option_type)
            SELECT
                CAST('{date_str}' AS DATE),
                {symbol_expr},
                {open_expr},
                {high_expr},
                {low_expr},
                {close_expr},
                {vol_expr},
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT),
                {strike_expr},
                CAST('20' || substr(ticker, length(ticker) - 14, 2) || '-'
                     || substr(ticker, length(ticker) - 12, 2) || '-'
                     || substr(ticker, length(ticker) - 10, 2) AS DATE),
                substr(ticker, length(ticker) - 8, 1)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """

    con = get_connection()
    try:
        con.execute("BEGIN")
        con.execute(sql)
        written = con.execute(
            "SELECT COUNT(*) FROM option_bars WHERE date = CAST(? AS DATE)",
            [date_str],
        ).fetchone()[0]
        con.execute("COMMIT")
        logger.info(f"[writers] {date_str}: {written:,} 行写入 option_bars")
        return written
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def write_sync_log(date: str, data_type: str, rows_written: int,
                   status: str, message: str = None) -> None:
    """写入一条同步记录（TQQQ 专用）。"""
    con = get_connection()
    try:
        con.execute(
            """
            INSERT INTO sync_log (ts, data_type, date, ticker, rows_written, status, message)
            VALUES (now(), ?, ?, ?, ?, ?, ?)
            """,
            [data_type, date, TICKER, rows_written, status, message],
        )
    finally:
        con.close()


def delete_all_data() -> None:
    """清空 TQQQ 的 equity_bars、option_bars、option_month sync_log 和 ticker_iv。

    用于拆股后的全量重拉前清理。
    """
    con = get_connection()
    try:
        con.execute("DELETE FROM equity_bars WHERE ticker = ?", [TICKER])
        con.execute(
            "DELETE FROM option_bars WHERE symbol LIKE ?",
            [f"O:{TICKER}%"],
        )
        con.execute(
            "DELETE FROM sync_log WHERE data_type = 'option_month' AND ticker = ?",
            [TICKER],
        )
        con.execute("DELETE FROM ticker_iv WHERE ticker = ?", [TICKER])
    finally:
        con.close()
    logger.info(f"[writers] 已清空 {TICKER} 的全部数据")


def backfill_option_bars_columns() -> int:
    """回填存量 option_bars 的 strike/expiration/option_type 列。

    只更新 strike IS NULL 的行，从 symbol 解析。幂等操作。
    """
    con = get_connection()
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM option_bars WHERE strike IS NULL"
        ).fetchone()
        null_count = result[0] if result else 0
        if null_count == 0:
            return 0

        con.execute("""
            UPDATE option_bars SET
                strike = CAST(substr(symbol, length(symbol) - 7) AS DOUBLE) / 1000.0,
                expiration = CAST(
                    '20' || substr(symbol, length(symbol) - 14, 2) || '-'
                    || substr(symbol, length(symbol) - 12, 2) || '-'
                    || substr(symbol, length(symbol) - 10, 2)
                    AS DATE),
                option_type = substr(symbol, length(symbol) - 8, 1)
            WHERE strike IS NULL
                AND symbol LIKE 'O:%' AND length(symbol) >= 17
        """)
        logger.info(f"[writers] 存量回填完成：{null_count} 行")
        return null_count
    finally:
        con.close()
```

- [ ] **Step 2: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 data/writers.py，去掉多标的过滤参数
```

---

### Task 5: 创建 core/options.py

**Files:**
- Create: `core/options.py`

- [ ] **Step 1: 创建 core/options.py**

合并 `iv.py:17-41` 的 `parse_occ_symbol` 和 `data_store.py:420-437` 的 `build_occ_symbol`，加上从 `run.py` 提取的合约匹配封装：

```python
"""OCC 期权合约解析与匹配。"""
import re

from config import TICKER

# OCC symbol 格式: O:{TICKER}{YYMMDD}{P|C}{STRIKE_8DIGITS}
_OCC_RE = re.compile(r"^O:([A-Z]+)(\d{6})([PC])(\d{8})$")


def parse_occ_symbol(symbol: str) -> dict:
    """解析 OCC 期权 symbol，返回 ticker/expiration/option_type/strike。

    Args:
        symbol: OCC 格式，如 "O:TQQQ260424P00030000"

    Returns:
        {"ticker": str, "expiration": "YYYY-MM-DD", "option_type": "P"|"C", "strike": float}

    Raises:
        ValueError: 格式不合法时
    """
    m = _OCC_RE.match(symbol)
    if not m:
        raise ValueError(f"Invalid OCC symbol: {symbol!r}")
    ticker, date6, opt_type, strike8 = m.groups()
    yy, mm, dd = date6[:2], date6[2:4], date6[4:6]
    expiration = f"20{yy}-{mm}-{dd}"
    strike = int(strike8) / 1000.0
    return {
        "ticker": ticker,
        "expiration": expiration,
        "option_type": opt_type,
        "strike": strike,
    }


def build_occ_symbol(expiry_date: str, strike: float,
                     option_type: str = "P") -> str:
    """构建 OCC 期权合约代码。

    Args:
        expiry_date: 到期日 "YYYY-MM-DD"
        strike: 行权价（美元），如 30.0
        option_type: "P" 或 "C"

    Returns:
        OCC 格式代码，如 "O:TQQQ260424P00030000"
    """
    yy = expiry_date[2:4]
    mm = expiry_date[5:7]
    dd = expiry_date[8:10]
    strike_int = int(round(strike * 1000))
    return f"O:{TICKER}{yy}{mm}{dd}{option_type}{strike_int:08d}"


def extract_strike(symbol: str) -> float:
    """从 OCC symbol 末 8 位提取 strike（千分之一美元单位）。"""
    return int(symbol[-8:]) / 1000.0


def extract_expiry(symbol: str) -> str:
    """从 OCC symbol 提取到期日 YYYY-MM-DD。"""
    p_idx = symbol.index("P") if "P" in symbol else symbol.index("C")
    date6 = symbol[p_idx - 6:p_idx]
    return f"20{date6[0:2]}-{date6[2:4]}-{date6[4:6]}"


def format_strike_str(strike: float) -> str:
    """格式化 strike 显示：整数显示整数（50），有小数显示小数（50.5）。"""
    return str(int(strike)) if strike == int(strike) else str(strike)


def match_option_contract(entry_date: str, expiry_date: str,
                          strike: float) -> dict | None:
    """查询匹配的期权合约，返回完整合约信息。

    封装 data.queries.query_option_on_date + OCC 解析。

    Returns:
        {symbol, display_symbol, occ_strike, occ_expiry, dte, price, vwap,
         open, high, low, close, volume} 或 None
    """
    from datetime import datetime
    from data.queries import query_option_on_date

    opt = query_option_on_date(entry_date, expiry_date, strike)
    if not opt:
        return None

    occ = opt["symbol"]
    occ_expiry = extract_expiry(occ)
    occ_strike = extract_strike(occ)
    dte = (datetime.strptime(occ_expiry, "%Y-%m-%d")
           - datetime.strptime(entry_date, "%Y-%m-%d")).days
    strike_str = format_strike_str(occ_strike)

    return {
        "symbol": occ,
        "display_symbol": f"{TICKER} {occ_expiry} P{strike_str}",
        "occ_strike": occ_strike,
        "occ_expiry": occ_expiry,
        "dte": dte,
        "price": round(opt["close"], 2),
        "vwap": round(opt["vwap"], 4),
        "open": opt["open"],
        "high": opt["high"],
        "low": opt["low"],
        "close": opt["close"],
        "volume": opt["volume"],
    }
```

- [ ] **Step 2: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 core/options.py，统一 OCC 解析和合约匹配
```

---

### Task 6: 创建 core/indicators.py 和 core/strategy.py

**Files:**
- Create: `core/indicators.py`
- Create: `core/strategy.py`

- [ ] **Step 1: 创建 core/indicators.py**

从 `indicators.py` 原样复制，不需要改动：

```python
"""技术指标计算：MA、MACD、动态 Pivot"""
import pandas as pd

MA_PERIODS = [5, 10, 20, 60]
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
PIVOT_PERIODS = [5, 30]


def add_ma(df: pd.DataFrame) -> pd.DataFrame:
    """添加 MA 均线列：ma5, ma10, ma20, ma60"""
    for period in MA_PERIODS:
        df[f"ma{period}"] = df["close"].rolling(window=period).mean()
    return df


def add_macd(df: pd.DataFrame) -> pd.DataFrame:
    """添加 MACD 指标列。DIF/DEA/MACD"""
    ema_fast = df["close"].ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = df["close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["dif"] = ema_fast - ema_slow
    df["dea"] = df["dif"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    df["macd"] = 2 * (df["dif"] - df["dea"])
    return df


def add_dynamic_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """添加动态 Pivot 指标（5日/30日）。"""
    for period in PIVOT_PERIODS:
        h = df["high"].rolling(window=period).max()
        l = df["low"].rolling(window=period).min()
        c = df["close"]

        pp = (h + l + c) / 3
        df[f"pivot_{period}_pp"] = pp
        df[f"pivot_{period}_r1"] = 2 * pp - l
        df[f"pivot_{period}_s1"] = 2 * pp - h
        df[f"pivot_{period}_r2"] = pp + (h - l)
        df[f"pivot_{period}_s2"] = pp - (h - l)
        df[f"pivot_{period}_r3"] = h + 2 * (pp - l)
        df[f"pivot_{period}_s3"] = l - 2 * (h - l)
    return df
```

- [ ] **Step 2: 创建 core/strategy.py**

从 `strategy.py` 提取决策树 + 周分组 + 到期日 + 历史波动率，去掉回测和 OTM 函数：

```python
"""策略核心：决策树分层、周分组、到期日计算、历史波动率。"""
import datetime
import math

import numpy as np
import pandas as pd

from config import EXPIRY_WEEKS, TRADING_DAYS_YEAR

# NYSE 交易日历（懒加载）
_nyse_calendar = None


def _get_nyse_calendar():
    global _nyse_calendar
    if _nyse_calendar is None:
        import exchange_calendars as xcals
        _nyse_calendar = xcals.get_calendar("XNYS")
    return _nyse_calendar


def compute_hist_vol(closes: pd.Series, window: int = 20) -> float:
    """计算年化历史波动率（百分比）。"""
    if len(closes) < window + 1:
        return 0.0
    log_returns = np.log(closes / closes.shift(1)).dropna()
    recent = log_returns.iloc[-window:]
    std = recent.std(ddof=1)
    if std == 0 or np.isnan(std):
        return 0.0
    return float(std * math.sqrt(TRADING_DAYS_YEAR) * 100)


def group_by_week(df: pd.DataFrame) -> list[dict]:
    """按 ISO 年+周分组，取每周第一个交易日的行。"""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["_iso_yw"] = df["date"].apply(lambda d: (d.isocalendar()[0], d.isocalendar()[1]))
    df = df.sort_values("date")

    result = []
    for _, group in df.groupby("_iso_yw", sort=False):
        group = group.sort_values("date")
        first = group.iloc[0]
        row = first.drop(labels=["_iso_yw"]).to_dict()
        row["date"] = first["date"].date()
        result.append(row)

    result.sort(key=lambda r: r["date"])
    return result


def classify_tier(row: dict) -> str:
    """分层决策树，按优先级依次判定，首个命中即返回。

    层级（按判定优先级）：
      A  企稳双撑    |MACD_today| < |MACD_yesterday| AND Close > P5_PP AND Close > P30_PP
      B1 回调均线    Close < MA20 AND Close > MA60
      B4 低波整理    hist_vol < 50 AND |MA20距离| <= 4.5%
      B2 超跌支撑    DIF < 0 AND Close > P30_PP
      B3 趋势动能弱  MA20 > MA60 AND DIF < 0
      C2 趋势延续    Close >= MA20 AND |MA20偏离| <= 10%
      C3 过热追涨    Close >= MA20 AND |MA20偏离| > 10%
      C1 跌势减速    Close < MA60 AND |MACD| < |prev_MACD|
      C4 加速下杀    Close < MA60 AND |MACD| >= |prev_MACD|
    """
    close = row["close"]
    macd = row["macd"]
    prev_macd = row["prev_macd"]
    p5_pp = row["pivot_5_pp"]
    p30_pp = row["pivot_30_pp"]
    ma20 = row["ma20"]
    ma60 = row["ma60"]
    dif = row["dif"]
    hist_vol = row["hist_vol"]

    if abs(macd) < abs(prev_macd) and close > p5_pp and close > p30_pp:
        return "A"
    if close < ma20 and close > ma60:
        return "B1"
    ma20_dist = abs((close - ma20) / ma20 * 100)
    if hist_vol < 50 and ma20_dist <= 4.5:
        return "B4"
    if dif < 0 and close > p30_pp:
        return "B2"
    if ma20 > ma60 and dif < 0:
        return "B3"
    if close >= ma20:
        if ma20_dist > 10:
            return "C3"
        return "C2"
    if close < ma60:
        if abs(macd) < abs(prev_macd):
            return "C1"
        return "C4"
    return "C2"


def extract_rules(row: dict) -> dict:
    """从周数据行提取决策规则详情，供前端决策面板展示。"""
    close = row["close"]
    macd_today = row["macd"]
    macd_yesterday = row["prev_macd"]
    ma20 = row["ma20"]
    return {
        "macd_today": macd_today,
        "macd_yesterday": macd_yesterday,
        "macd_narrow": abs(macd_today) < abs(macd_yesterday),
        "p5_pp": row["pivot_5_pp"],
        "above_p5": close > row["pivot_5_pp"],
        "p30_pp": row["pivot_30_pp"],
        "above_p30": close > row["pivot_30_pp"],
        "ma20": ma20,
        "ma60": row["ma60"],
        "dif": row["dif"],
        "hist_vol": row["hist_vol"],
        "ma20_dist": round((close - ma20) / ma20 * 100, 2),
        "above_ma60": close >= row["ma60"],
    }


def find_expiry_date(entry_date: datetime.date) -> datetime.date:
    """从 entry_date 所在周的周一起算，向后推 EXPIRY_WEEKS 整周，
    返回该目标周内最后一个美股交易日。
    """
    monday = entry_date - datetime.timedelta(days=entry_date.weekday())
    target_monday = monday + datetime.timedelta(weeks=EXPIRY_WEEKS)
    target_friday = target_monday + datetime.timedelta(days=4)

    cal = _get_nyse_calendar()
    sessions = cal.sessions_in_range(
        pd.Timestamp(target_monday), pd.Timestamp(target_friday)
    )
    if len(sessions) == 0:
        return target_friday
    return sessions[-1].date()
```

- [ ] **Step 3: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 core/indicators.py 和 core/strategy.py
```

---

### Task 7: 创建 core/backtest.py

**Files:**
- Create: `core/backtest.py`

- [ ] **Step 1: 创建 core/backtest.py**

合并 `strategy.py:215-390` 的回测逻辑 + `run.py:69-129` 的期权 enrichment + `run.py:309-326` 的 summary/tiers/latest：

```python
"""回测引擎：逐周回测、期权 enrichment、汇总统计。"""
from datetime import datetime

import pandas as pd

from config import DEFAULT_OTM, ALL_TIERS, TIER_NAMES, TICKER
from core.strategy import classify_tier, extract_rules, find_expiry_date


def backtest_weeks(weekly_rows: list[dict],
                   daily_df: pd.DataFrame) -> list[dict]:
    """逐周回测：分层 → 定行权价 → 找到期日价格 → 判断是否平稳到期。

    返回倒序（最新一周在前）的 list[dict]。
    """
    daily = daily_df.copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    last_data_date = daily["date"].max()

    results = []
    for row in weekly_rows:
        tier = classify_tier(row)
        entry_date = row["date"]
        close = row["close"]
        otm_frac = DEFAULT_OTM.get(tier, 0.10)
        otm_pct = int(otm_frac * 100)
        strike = round(close * (1 - otm_frac), 2)
        rules = extract_rules(row)
        expiry_date = find_expiry_date(entry_date)

        pending = False
        expiry_close = None
        settle_diff = None
        safe_expiry = None

        if expiry_date > last_data_date:
            pending = True
        else:
            expiry_row = daily[daily["date"] == expiry_date]
            if expiry_row.empty:
                before = daily[daily["date"] <= expiry_date].sort_values("date")
                if not before.empty:
                    expiry_close = float(before.iloc[-1]["close"])
                else:
                    pending = True
            else:
                expiry_close = float(expiry_row.iloc[0]["close"])

        if not pending and expiry_close is not None:
            settle_diff = round((expiry_close - strike) / strike * 100, 2)
            safe_expiry = settle_diff > 0

        recovery_days = None
        recovery_gap = None
        if safe_expiry is False:
            after = daily[daily["date"] > expiry_date].sort_values("date")
            recovered = after[after["close"] > strike]
            if not recovered.empty:
                recovery_date = recovered.iloc[0]["date"]
                recovery_days = (recovery_date - expiry_date).days
            else:
                latest_close = float(daily.iloc[-1]["close"])
                recovery_gap = round((latest_close - strike) / strike * 100, 1)

        results.append({
            "date": str(entry_date),
            "close": close,
            "tier": tier,
            "rules": rules,
            "otm": otm_pct,
            "strike": strike,
            "expiry_date": str(expiry_date),
            "expiry_close": expiry_close,
            "settle_diff": settle_diff,
            "safe_expiry": safe_expiry,
            "recovery_days": recovery_days,
            "recovery_gap": recovery_gap,
            "pending": pending,
        })

    results.sort(key=lambda r: r["date"], reverse=True)
    return results


def enrich_with_options(weeks: list[dict], daily: pd.DataFrame) -> None:
    """为每周回测数据补充期权合约信息，并用 OCC 真实 strike 重算结算指标。"""
    from core.options import match_option_contract, extract_strike

    for w in weeks:
        strike = w.get("strike")
        expiry = w.get("expiry_date")
        entry = w.get("date")
        if not strike or not expiry or not entry:
            continue

        contract = match_option_contract(entry, expiry, strike)
        if contract:
            occ_strike = contract["occ_strike"]
            w["option_symbol"] = contract["display_symbol"]
            w["option_strike"] = occ_strike
            w["option_dte"] = contract["dte"]
            w["option_price"] = contract["price"]
            w["option_vwap"] = contract["vwap"]
            # 用合约真实 strike 重算结算差比和平稳到期
            if w.get("expiry_close") is not None and occ_strike > 0:
                w["settle_diff"] = round(
                    (w["expiry_close"] - occ_strike) / occ_strike * 100, 2)
                w["safe_expiry"] = w["settle_diff"] > 0
                if w["safe_expiry"]:
                    w["recovery_days"] = None
                    w["recovery_gap"] = None
                else:
                    # 穿仓：用 OCC strike 重算恢复天数
                    expiry_str = w["expiry_date"]
                    after = daily[daily["date"] > expiry_str].sort_values("date")
                    recovered = after[after["close"] > occ_strike]
                    if not recovered.empty:
                        rec_date_str = str(recovered.iloc[0]["date"])
                        delta = (datetime.strptime(rec_date_str, "%Y-%m-%d")
                                 - datetime.strptime(expiry_str, "%Y-%m-%d"))
                        w["recovery_days"] = delta.days
                        w["recovery_gap"] = None
                    else:
                        w["recovery_days"] = None
                        latest_close = float(daily.iloc[-1]["close"])
                        w["recovery_gap"] = round(
                            (latest_close - occ_strike) / occ_strike * 100, 1)
        else:
            w["option_symbol"] = None
            w["option_strike"] = None
            w["option_dte"] = None
            w["option_price"] = None
            w["option_vwap"] = None


def compute_summary(weeks: list[dict]) -> dict:
    """汇总统计。"""
    settled = [w for w in weeks if not w["pending"]]
    safe_count = sum(1 for w in settled if w.get("safe_expiry") is True)
    safe_rate = round(safe_count / len(settled) * 100, 1) if settled else 0.0
    return {
        "total_weeks": len(weeks),
        "settled": len(settled),
        "pending": len(weeks) - len(settled),
        "safe_count": safe_count,
        "safe_rate": safe_rate,
    }


def compute_tiers(weeks: list[dict]) -> dict:
    """按层级统计，包含平稳到期比例。"""
    result = {}
    for tier_key in ALL_TIERS:
        items = [w for w in weeks if w["tier"] == tier_key]
        if not items:
            continue
        settled = [w for w in items if not w["pending"]]
        safe_count = sum(1 for w in settled if w.get("safe_expiry") is True)
        safe_rate = round(safe_count / len(settled) * 100, 1) if settled else 0.0
        result[tier_key] = {
            "name": TIER_NAMES[tier_key],
            "otm": int(DEFAULT_OTM.get(tier_key, 0.10) * 100),
            "count": len(items),
            "settled": len(settled),
            "safe_count": safe_count,
            "safe_rate": safe_rate,
        }
    return result


def compute_latest(weekly_rows: list[dict],
                   daily_df: pd.DataFrame) -> dict:
    """最近一周的完整决策详情。"""
    if not weekly_rows:
        return {}

    row = weekly_rows[-1]
    tier = classify_tier(row)
    close = row["close"]
    rules = extract_rules(row)
    strikes = {t: round(close * (1 - o), 2) for t, o in DEFAULT_OTM.items()}
    expiry_date = find_expiry_date(row["date"])
    otm_frac = DEFAULT_OTM.get(tier, 0.10)

    result = {
        "date": str(row["date"]),
        "close": close,
        "tier": tier,
        "rules": rules,
        "otm": int(otm_frac * 100),
        "strikes": strikes,
        "expiry_date": str(expiry_date),
    }

    # 查询当周期权合约
    from core.options import match_option_contract
    lt_strike = strikes.get(tier)
    contract = match_option_contract(
        result["date"], result["expiry_date"], lt_strike or 0)
    if contract:
        result["option_symbol"] = contract["display_symbol"]
        result["option_dte"] = contract["dte"]
        result["option_price"] = contract["price"]
        result["option_strike"] = contract["occ_strike"]
        result["option_expiry"] = contract["occ_expiry"]

    return result
```

- [ ] **Step 2: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 core/backtest.py，合并回测+enrichment+统计
```

---

### Task 8: 创建 core/circuit_breaker.py

**Files:**
- Create: `core/circuit_breaker.py`

- [ ] **Step 1: 创建 core/circuit_breaker.py**

从 `run.py:154-171` 提取：

```python
"""熔断机制：连续弱势暂停卖出。"""

_C_TIERS = {"C1", "C2", "C3", "C4"}


def apply_circuit_breaker(weeks: list[dict]) -> None:
    """检测连续 C 类分层，标记 skip=True 暂停卖出。

    规则：
    - 前 2 周都是 C 类 + 本周也是 C 类时才可能暂停
    - 本周 C1 且前 2 周含 C1 → 继续卖出（跌势已有减速信号）
    - 其余情况 → 暂停

    就地修改 weeks，添加 skip / skip_reason 字段。
    """
    weeks_asc = sorted(weeks, key=lambda w: w["date"])
    for i, w in enumerate(weeks_asc):
        if i >= 2:
            p1 = weeks_asc[i - 1]["tier"]
            p2 = weeks_asc[i - 2]["tier"]
            if p1 in _C_TIERS and p2 in _C_TIERS and w["tier"] in _C_TIERS:
                if w["tier"] == "C1" and (p1 == "C1" or p2 == "C1"):
                    w["skip"] = False
                else:
                    w["skip"] = True
                    w["skip_reason"] = f"前2周 {p2}→{p1}，本周 {w['tier']}，连续弱势暂停"
                continue
        w["skip"] = False
```

- [ ] **Step 2: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 core/circuit_breaker.py
```

---

### Task 9: 创建 data/sync/ 模块

**Files:**
- Create: `data/sync/splits.py`
- Create: `data/sync/equity.py`
- Create: `data/sync/options.py`
- Create: `data/sync/iv.py`
- Create: `data/sync/orchestrator.py`

- [ ] **Step 1: 创建 data/sync/splits.py**

从 `rest_downloader.py:86-131` 提取，去掉 ticker 参数：

```python
"""拆股数据同步。"""
import logging

import requests

from config import TICKER, REST_BASE_URL
from data.queries import query_splits
from data.writers import upsert_splits

logger = logging.getLogger(__name__)


def download_splits(api_key: str) -> list[dict]:
    """从 Massive API 拉取 TQQQ 拆股历史，写入 splits 表，返回新增事件。"""
    existing = {r["exec_date"] for r in query_splits()}

    url = f"{REST_BASE_URL}/stocks/v1/splits"
    params = {"ticker": TICKER, "limit": 1000, "apiKey": api_key}

    resp = requests.get(url, params=params)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[splits] API 错误: {e}")
        return []

    results = resp.json().get("results", [])
    if not results:
        return []

    all_rows = [
        {"ticker": TICKER, "exec_date": r["execution_date"],
         "split_from": r["split_from"], "split_to": r["split_to"]}
        for r in results
    ]

    upsert_splits(all_rows)

    new_rows = [r for r in all_rows if r["exec_date"] not in existing]
    if new_rows:
        logger.info(f"[splits] 发现 {len(new_rows)} 个新拆股事件: "
                     f"{[r['exec_date'] for r in new_rows]}")
    return new_rows
```

- [ ] **Step 2: 创建 data/sync/equity.py**

从 `rest_downloader.py:21-83` 提取，去掉 ticker 参数：

```python
"""REST API 股票日K 下载。"""
import datetime
import logging
import time

import requests

from config import TICKER, REST_BASE_URL, REST_MAX_RETRIES, REST_RETRY_DELAY
from data.writers import upsert_equity_bars

logger = logging.getLogger(__name__)


def download_and_store(from_date: str, to_date: str, api_key: str) -> int:
    """从 Massive REST API 下载 TQQQ 日K并写入 equity_bars。

    Returns:
        写入行数
    """
    url = f"{REST_BASE_URL}/v2/aggs/ticker/{TICKER}/range/1/day/{from_date}/{to_date}"
    params = {"adjusted": "true", "sort": "asc",
              "limit": 50000, "apiKey": api_key}

    resp = None
    for attempt in range(REST_MAX_RETRIES):
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            wait = REST_RETRY_DELAY * (attempt + 1)
            logger.warning(f"[equity] 限流(429)，等待 {wait}s")
            time.sleep(wait)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"[equity] HTTP 错误: {e}")
            return 0
        break
    else:
        logger.error(f"[equity] 重试 {REST_MAX_RETRIES} 次后放弃")
        return 0

    raw = resp.json().get("results", [])
    if not raw:
        logger.info(f"[equity] {from_date}~{to_date} 无数据")
        return 0

    rows = []
    for r in raw:
        try:
            dt = datetime.datetime.fromtimestamp(
                r["t"] / 1000, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d")
            rows.append({
                "date": dt, "ticker": TICKER,
                "open": r["o"], "high": r["h"], "low": r["l"], "close": r["c"],
                "volume": r.get("v"), "vwap": r.get("vw"),
                "transactions": r.get("n"),
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"[equity] 跳过异常行: {e} — {r}")

    written = upsert_equity_bars(rows)
    logger.info(f"[equity] {from_date}~{to_date}: {written} 行写入")
    return written
```

- [ ] **Step 3: 创建 data/sync/options.py**

合并 `s3_downloader.py` + `flat_file_fetcher.py`：

```python
"""S3 期权 Flat Files 下载与同步。

合并原 s3_downloader.py 和 flat_file_fetcher.py。
S3 路径: us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
"""
import calendar
import datetime
import logging
import os
import queue
import threading
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import TICKER, S3_ENDPOINT, S3_BUCKET
from data.queries import is_synced
from data.writers import insert_option_bars_from_csv, write_sync_log

logger = logging.getLogger(__name__)

_PREFIX = "us_options_opra/day_aggs_v1"
_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "output" / "flat_files_cache"
_SENTINEL = object()


def make_s3_client():
    """从环境变量创建 S3 客户端。"""
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["MASSIVE_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MASSIVE_S3_SECRET_KEY"],
        endpoint_url=S3_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )


def download_day_file(date_str: str, s3_client=None) -> Path | None:
    """下载指定日期的期权文件到本地缓存。已缓存则跳过，非交易日返回 None。"""
    cache = _CACHE_DIR / f"{date_str}.csv.gz"
    if cache.exists():
        return cache

    if s3_client is None:
        s3_client = make_s3_client()

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    d = datetime.date.fromisoformat(date_str)
    key = f"{_PREFIX}/{d.year}/{d.month:02d}/{date_str}.csv.gz"
    try:
        s3_client.download_file(S3_BUCKET, key, str(cache))
        size_kb = cache.stat().st_size // 1024 if cache.exists() else 0
        logger.info(f"[options] 已下载 {date_str} ({size_kb} KB)")
        return cache if cache.exists() else None
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            logger.debug(f"[options] {date_str} 非交易日，跳过")
            return None
        raise


def _trading_months(from_date: str, to_date: str) -> list[tuple[int, int]]:
    """生成日期范围内所有 (year, month) 元组。"""
    start = datetime.date.fromisoformat(from_date)
    end = datetime.date.fromisoformat(to_date)
    months = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append((year, month))
        month += 1
        if month > 12:
            year += 1
            month = 1
    return months


def _trading_days(from_date: str, to_date: str) -> list[str]:
    """生成日期范围内所有周一至周五的日期列表。"""
    start = datetime.date.fromisoformat(from_date)
    end = datetime.date.fromisoformat(to_date)
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(str(current))
        current += datetime.timedelta(days=1)
    return days


def sync_options(from_date: str, to_date: str, s3_client=None) -> None:
    """同步 TQQQ 期权数据（按月粒度，下载线程+写入主线程流水线）。"""
    if s3_client is None:
        s3_client = make_s3_client()

    logger.info(f"[options] 同步期权数据 {from_date} ~ {to_date}")

    for year, month in _trading_months(from_date, to_date):
        month_key = f"{year}-{month:02d}-01"

        if is_synced(month_key, "option_month"):
            logger.debug(f"[options] {year}-{month:02d} 已同步，跳过")
            continue

        last_day = calendar.monthrange(year, month)[1]
        month_start = max(from_date, f"{year}-{month:02d}-01")
        month_end = min(to_date, f"{year}-{month:02d}-{last_day:02d}")
        days = _trading_days(month_start, month_end)
        if not days:
            continue

        logger.info(f"[options] 处理 {year}-{month:02d}，共 {len(days)} 个交易日")

        q = queue.Queue(maxsize=3)
        total_written = 0
        month_ok = True

        def producer(days=days):
            for date_str in days:
                try:
                    cache_path = download_day_file(date_str, s3_client)
                    q.put((date_str, cache_path))
                except Exception as e:
                    logger.error(f"[options] {date_str} 下载失败: {e}")
                    q.put((date_str, None))
            q.put(_SENTINEL)

        t = threading.Thread(target=producer, daemon=True)
        t.start()

        while True:
            item = q.get()
            if item is _SENTINEL:
                break
            date_str, cache_path = item
            if cache_path is not None:
                try:
                    written = insert_option_bars_from_csv(cache_path, date_str)
                    total_written += written
                    logger.info(f"[options] {date_str}: {written:,} 行写入")
                except Exception as e:
                    logger.error(f"[options] {date_str} 写入失败: {e}")
                    month_ok = False

        t.join()

        status = "ok" if month_ok else "error"
        msg = None if month_ok else "部分天写入失败"
        write_sync_log(month_key, "option_month", total_written, status, msg)
        if month_ok:
            logger.info(f"[options] {year}-{month:02d} 完成，共 {total_written:,} 行")
```

- [ ] **Step 4: 创建 data/sync/iv.py**

合并 `iv.py` 的 B-S 计算 + `data_sync.py:83-163` 的 IV 同步编排：

```python
"""IV 计算与同步：Black-Scholes 反算 + 30天 ATM 包夹插值。"""
import math
import datetime as _dt
import logging

from scipy.stats import norm

from config import TICKER, RISK_FREE_RATE, IV_MIN_DTE, IV_TARGET_DAYS
from core.options import parse_occ_symbol
from data.queries import (
    get_latest_iv_date, get_latest_option_date,
    get_earliest_option_date, get_option_dates_in_range,
    query_option_bars_for_iv, query_equity_bars,
)
from data.writers import upsert_ticker_iv

logger = logging.getLogger(__name__)

_IV_MIN = 0.01
_IV_MAX = 5.0


def _bs_price(spot, strike, tte, r, sigma, option_type):
    """Black-Scholes 正向定价。"""
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * tte) / (sigma * math.sqrt(tte))
    d2 = d1 - sigma * math.sqrt(tte)
    if option_type == "C":
        return spot * norm.cdf(d1) - strike * math.exp(-r * tte) * norm.cdf(d2)
    else:
        return strike * math.exp(-r * tte) * norm.cdf(-d2) - spot * norm.cdf(-d1)


def bs_implied_vol(price, spot, strike, tte, r, option_type,
                   max_iter=100, tol=1e-6):
    """二分法反算隐含波动率。无法收敛返回 float('nan')。"""
    if price <= 0 or tte <= 0 or spot <= 0 or strike <= 0:
        return float("nan")
    lo, hi = 0.01, 5.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        calc = _bs_price(spot, strike, tte, r, mid, option_type)
        if abs(calc - price) < tol:
            return mid
        if calc > price:
            hi = mid
        else:
            lo = mid
    return float("nan")


def _atm_iv(bars, spot, tte):
    """单个到期日的 ATM IV：最接近 spot 的 Put+Call 各 1 档，取平均。"""
    ivs = []
    for opt_type in ("P", "C"):
        typed = [b for b in bars if b["option_type"] == opt_type]
        if not typed:
            continue
        atm = min(typed, key=lambda b: abs(b["strike"] - spot))
        val = bs_implied_vol(atm["close"], spot, atm["strike"],
                             tte, RISK_FREE_RATE, opt_type)
        if not math.isnan(val) and _IV_MIN <= val <= _IV_MAX:
            ivs.append(val)
    return sum(ivs) / len(ivs) if ivs else float("nan")


def compute_ticker_iv(option_bars, spot, date):
    """30 天 ATM IV：包夹 30 天的近月/远月插值。

    流程：
    1. 按到期日分组，排除 DTE ≤ 7 天
    2. 选包夹 30 天的两个到期日
    3. 在方差-时间空间线性插值到 30 天
    """
    if not option_bars:
        return float("nan")

    current = _dt.date.fromisoformat(date)
    by_expiry = {}
    expiry_dte = {}
    for bar in option_bars:
        exp = bar["expiration"]
        if exp not in expiry_dte:
            expiry_dte[exp] = (_dt.date.fromisoformat(exp) - current).days
        if expiry_dte[exp] > IV_MIN_DTE:
            by_expiry.setdefault(exp, []).append(bar)

    if not by_expiry:
        return float("nan")

    near = [(e, expiry_dte[e]) for e in by_expiry if expiry_dte[e] <= IV_TARGET_DAYS]
    far = [(e, expiry_dte[e]) for e in by_expiry if expiry_dte[e] > IV_TARGET_DAYS]

    if near and far:
        near_exp, near_dte = max(near, key=lambda x: x[1])
        far_exp, far_dte = min(far, key=lambda x: x[1])
        iv_near = _atm_iv(by_expiry[near_exp], spot, near_dte / 365.0)
        iv_far = _atm_iv(by_expiry[far_exp], spot, far_dte / 365.0)
        if math.isnan(iv_near) or math.isnan(iv_far):
            return iv_far if math.isnan(iv_near) else iv_near
        var_near = iv_near ** 2 * (near_dte / 365.0)
        var_far = iv_far ** 2 * (far_dte / 365.0)
        w = (far_dte - IV_TARGET_DAYS) / (far_dte - near_dte)
        var_30 = w * var_near + (1 - w) * var_far
        return math.sqrt(var_30 / (IV_TARGET_DAYS / 365.0))

    all_exps = near + far
    best_exp, best_dte = min(all_exps, key=lambda x: abs(x[1] - IV_TARGET_DAYS))
    return _atm_iv(by_expiry[best_exp], spot, best_dte / 365.0)


def sync_ticker_iv() -> None:
    """计算并存储 TQQQ 的 IV。空表全量，有数据增量。"""
    latest_iv = get_latest_iv_date()
    latest_opt = get_latest_option_date()

    if not latest_opt:
        logger.info("[iv] 无 option_bars 数据，跳过 IV 计算")
        return

    if latest_iv:
        from_date = str(_dt.date.fromisoformat(latest_iv) + _dt.timedelta(days=1))
    else:
        from_date = get_earliest_option_date()
        if not from_date:
            return

    to_date = latest_opt
    if from_date > to_date:
        logger.info("[iv] IV 已是最新")
        return

    logger.info(f"[iv] 计算 IV: {from_date} ~ {to_date}")

    dates = get_option_dates_in_range(from_date, to_date)

    iv_rows = []
    for d in dates:
        option_bars = query_option_bars_for_iv(d)
        eq = query_equity_bars(d, d)
        if not eq:
            continue
        spot = eq[0]["close"]
        result = compute_ticker_iv(option_bars, spot, d)
        if not math.isnan(result):
            iv_rows.append({"date": d, "ticker": TICKER, "iv": result})

    if iv_rows:
        upsert_ticker_iv(iv_rows)
        logger.info(f"[iv] 写入 {len(iv_rows)} 天 IV")
```

- [ ] **Step 5: 创建 data/sync/orchestrator.py**

从 `data_sync.py:24-81` 提取，去掉 tickers 参数：

```python
"""数据同步编排：ensure_synced 统一入口。"""
import datetime
import logging

from config import TICKER, FULL_SYNC_YEARS
from data.store import init_db
from data.queries import get_latest_equity_date
from data.writers import delete_all_data
from data.sync.splits import download_splits
from data.sync.equity import download_and_store
from data.sync.options import sync_options
from data.sync.iv import sync_ticker_iv

logger = logging.getLogger(__name__)


def ensure_synced(api_key: str) -> None:
    """确保 DuckDB 数据最新。空库同步近 2 年，有数据增量补到昨天。

    流程：splits检测 → 清空重拉 → equity增量 → options按月 → IV增量
    """
    init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))
    full_sync_from = str(today - datetime.timedelta(days=365 * FULL_SYNC_YEARS))

    # 1. 同步拆股数据，检测新事件
    need_purge = False
    if api_key:
        new_splits = download_splits(api_key)
        if new_splits:
            need_purge = True

    # 2. 有新拆股 → 清空数据
    if need_purge:
        logger.info(f"[sync] {TICKER} 检测到新拆股，清空数据准备全量重拉")
        delete_all_data()

    # 3. 同步 equity
    if api_key:
        if need_purge:
            eq_from = full_sync_from
        else:
            latest = get_latest_equity_date()
            if not latest:
                eq_from = full_sync_from
            else:
                eq_from = str(datetime.date.fromisoformat(latest)
                              + datetime.timedelta(days=1))
        if eq_from <= to_date:
            logger.info(f"[sync] equity 同步 {eq_from} ~ {to_date}")
            download_and_store(eq_from, to_date, api_key)
        else:
            logger.info("[sync] equity 已是最新")

    # 4. 同步 option
    sync_options(full_sync_from, to_date)

    # 5. 计算 IV
    sync_ticker_iv()
```

- [ ] **Step 6: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 data/sync/ 模块，合并 s3_downloader+flat_file_fetcher
```

---

### Task 10: 创建 output/ 模块

**Files:**
- Create: `output/report.py`
- Create: `output/deploy.py`
- Move: `template.html` → `output/template.html`

- [ ] **Step 1: 创建 output/report.py**

从 `run.py` 提取 JSON 组装 + HTML 生成：

```python
"""报告生成：JSON 组装 + HTML 嵌入。"""
import json
import logging
import os

from config import TICKER

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "output")


def build_report_data(*, tiers, summary, tier_stats, latest, weeks,
                      daily_bars, market, data_range, generated,
                      otm_config) -> dict:
    """将各模块计算结果组装成 JSON 结构。"""
    return {
        "ticker": TICKER,
        "generated": generated,
        "data_range": data_range,
        "otm_config": otm_config,
        "summary": summary,
        "tiers": tier_stats,
        "latest": latest,
        "weeks": weeks,
        "daily_bars": daily_bars,
        "market": market,
    }


def save_json(data: dict) -> str:
    """保存 JSON 到 output/ 目录，返回文件路径。"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, f"{TICKER}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON 已保存: {json_path}")
    return json_path


def load_template() -> str | None:
    """读取 template.html 模板。"""
    template_path = os.path.join(SCRIPT_DIR, "template.html")
    if not os.path.exists(template_path):
        logger.warning("template.html 不存在")
        return None
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def render_html(data: dict) -> str | None:
    """将策略结果内嵌到模板，生成 output/{TICKER}.html，返回文件路径。"""
    template_html = load_template()
    if not template_html:
        return None

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html_path = os.path.join(OUTPUT_DIR, f"{TICKER}.html")

    data_str = json.dumps(data, ensure_ascii=False, indent=2)
    marker = "/* EMBEDDED_DATA_PLACEHOLDER */"
    if marker not in template_html:
        logger.warning("template.html 中未找到内嵌数据占位符")
        return None

    html = template_html.replace(
        marker, f'EMBEDDED_DATA["{TICKER}"] = {data_str};\n' + marker)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"HTML 已生成: {html_path}")
    return html_path
```

- [ ] **Step 2: 创建 output/deploy.py**

从 `deploy.py` 平移，去掉 `--ticker` 和 `ticker` 参数。`wrap_with_password`、`deploy_to_cloudflare`、`send_telegram` 原样保留（代码见当前 `deploy.py:25-224`），只修改 `send_telegram` 签名去掉 ticker 参数，硬编码 TQQQ。

```python
"""部署：密码包装 → Cloudflare Pages → Telegram 通知"""
import base64
import hashlib
import json
import logging
import os

import requests

from config import TICKER

logger = logging.getLogger(__name__)

CF_API_BASE = "https://api.cloudflare.com/client/v4"


def deploy_to_cloudflare(html: str) -> str:
    """将 HTML 部署到 Cloudflare Pages，返回站点 URL。"""
    # 代码与当前 deploy.py:25-105 完全一致，此处省略避免重复
    # 实现时直接从 deploy.py 复制
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
    project = os.environ.get("CLOUDFLARE_PAGES_PROJECT", "")
    if not token:
        raise ValueError("缺少环境变量 CLOUDFLARE_API_TOKEN")
    if not account_id:
        raise ValueError("缺少环境变量 CLOUDFLARE_ACCOUNT_ID")
    if not project:
        raise ValueError("缺少环境变量 CLOUDFLARE_PAGES_PROJECT")

    auth_headers = {"Authorization": f"Bearer {token}"}
    file_bytes = html.encode("utf-8")
    file_b64 = base64.b64encode(file_bytes).decode("ascii")
    file_hash = hashlib.md5(file_bytes).hexdigest()
    file_path = "/index.html"

    resp = requests.get(
        f"{CF_API_BASE}/accounts/{account_id}/pages/projects/{project}/upload-token",
        headers=auth_headers, timeout=30)
    resp.raise_for_status()
    jwt = resp.json()["result"]["jwt"]
    logger.info("Cloudflare upload token 已获取")

    upload_headers = {"Authorization": f"Bearer {jwt}"}

    resp = requests.post(
        f"{CF_API_BASE}/pages/assets/upload",
        headers={**upload_headers, "Content-Type": "application/json"},
        json=[{"key": file_hash, "value": file_b64,
               "metadata": {"contentType": "text/html"}, "base64": True}],
        timeout=60)
    resp.raise_for_status()
    logger.info("文件已上传到 Cloudflare")

    resp = requests.post(
        f"{CF_API_BASE}/pages/assets/upsert-hashes",
        headers={**upload_headers, "Content-Type": "application/json"},
        json={"hashes": [file_hash]}, timeout=30)
    resp.raise_for_status()
    logger.info("文件 hash 已注册")

    manifest = {file_path: file_hash}
    resp = requests.post(
        f"{CF_API_BASE}/accounts/{account_id}/pages/projects/{project}/deployments",
        headers=auth_headers,
        files={"manifest": (None, json.dumps(manifest))}, timeout=60)
    resp.raise_for_status()

    url = f"https://{project}.pages.dev"
    logger.info(f"Cloudflare Pages 部署成功: {url}")
    return url


def wrap_with_password(html: str, password: str) -> str:
    """将原始 HTML 用前端密码锁包装。"""
    # 代码与当前 deploy.py:108-200 完全一致
    # 实现时直接从 deploy.py 复制
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    encoded = base64.b64encode(html.encode("utf-8")).decode("ascii")
    # ... (完整 HTML 模板与当前 deploy.py 一致)
    return f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lambda Report</title>
<style>
  body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }}
  #auth-screen {{
    display: flex; flex-direction: column; align-items: center;
    justify-content: center; height: 100vh; background: #f5f5f5;
  }}
  #auth-screen input[type="password"] {{
    padding: 12px 16px; font-size: 16px; border: 1px solid #ccc;
    border-radius: 8px; width: 240px; margin-bottom: 12px;
    outline: none; text-align: center;
  }}
  #auth-screen input[type="password"]:focus {{ border-color: #4a90d9; }}
  #auth-screen button {{
    padding: 10px 32px; font-size: 16px; background: #4a90d9;
    color: white; border: none; border-radius: 8px; cursor: pointer;
  }}
  #auth-screen button:hover {{ background: #357abd; }}
  #auth-screen .error {{ color: #e74c3c; margin-top: 8px; font-size: 14px; }}
</style>
</head>
<body>
<div id="auth-screen">
  <h2>Lambda Report</h2>
  <input type="password" id="pw-input" placeholder="输入密码" autofocus>
  <button onclick="verify()">确认</button>
  <div class="error" id="error-msg"></div>
</div>
<script>
const HASH = "{pw_hash}";
const DATA = "{encoded}";
const CACHE_KEY = "lambda_auth";
const CACHE_DAYS = 7;

async function sha256(text) {{
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}}

function unlock() {{
  var decoded = atob(DATA);
  var bytes = new Uint8Array(decoded.length);
  for (var i = 0; i < decoded.length; i++) bytes[i] = decoded.charCodeAt(i);
  var html = new TextDecoder("utf-8").decode(bytes);
  document.getElementById("auth-screen").style.display = "none";
  var iframe = document.createElement("iframe");
  iframe.style.cssText = "position:fixed;top:0;left:0;width:100%;height:100%;border:none;margin:0;padding:0;";
  iframe.srcdoc = html;
  document.body.style.margin = "0";
  document.body.style.overflow = "hidden";
  document.body.appendChild(iframe);
}}

async function verify() {{
  var pw = document.getElementById("pw-input").value;
  var h = await sha256(pw);
  if (h === HASH) {{
    localStorage.setItem(CACHE_KEY, JSON.stringify({{hash: h, expires: Date.now() + CACHE_DAYS * 86400000}}));
    unlock();
  }} else {{
    document.getElementById("error-msg").textContent = "密码错误";
  }}
}}

document.getElementById("pw-input").addEventListener("keydown", function(e) {{
  if (e.key === "Enter") verify();
}});

(function() {{
  try {{
    var cache = JSON.parse(localStorage.getItem(CACHE_KEY));
    if (cache && cache.hash === HASH && cache.expires > Date.now()) unlock();
  }} catch(e) {{}}
}})();
</script>
</body>
</html>"""


def send_telegram(url: str) -> None:
    """部署成功后发送 Telegram 通知。缺少环境变量时静默跳过。"""
    token = os.environ.get("LAMBDA_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("LAMBDA_TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("缺少 Telegram 环境变量，跳过通知")
        return

    text = f"Lambda {TICKER} 报告已更新\n{url}"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text}, timeout=15)
        if resp.status_code == 200:
            logger.info("Telegram 通知已发送")
        else:
            logger.warning(f"Telegram 通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.warning(f"Telegram 通知异常: {e}")
```

- [ ] **Step 3: 移动 template.html**

```bash
mv template.html output/template.html
```

- [ ] **Step 4: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 output/ 模块（report/deploy/template）
```

---

### Task 11: 创建 CLI 入口

**Files:**
- Create: `cli/run.py`
- Create: `cli/deploy.py`
- Create: `cli/sync.py`
- Create: `cli/__main__.py` (可选，方便 `python -m cli.run`)

- [ ] **Step 1: 创建 cli/run.py**

```python
"""策略生成入口：同步数据 → 策略计算 → JSON → HTML"""
import logging
import os
import sys
from datetime import datetime

import pandas as pd

from config import TICKER, DEFAULT_OTM
from data.store import init_db
from data.sync.orchestrator import ensure_synced
from data.queries import query_equity_bars, query_ticker_iv
from core.indicators import add_ma, add_macd, add_dynamic_pivot
from core.strategy import group_by_week, compute_hist_vol
from core.backtest import (
    backtest_weeks, enrich_with_options,
    compute_summary, compute_tiers, compute_latest,
)
from core.circuit_breaker import apply_circuit_breaker
from core.options import match_option_contract
from output.report import build_report_data, save_json, render_html

logger = logging.getLogger(__name__)


def setup_logging():
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    setup_logging()
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    logger.info(f"===== {TICKER} =====")

    # 1. 数据同步
    ensure_synced(api_key)

    # 2. 加载数据 + 指标
    rows = query_equity_bars("1900-01-01", datetime.now().strftime("%Y-%m-%d"))
    if not rows:
        logger.warning(f"[{TICKER}] DuckDB 无数据")
        return
    df = pd.DataFrame(rows)
    df = add_ma(df)
    df = add_macd(df)
    df = add_dynamic_pivot(df)

    # 3. 策略计算
    df["hist_vol"] = df["close"].rolling(window=21, min_periods=21).apply(
        lambda x: compute_hist_vol(pd.Series(x.values), window=20), raw=False)
    df["prev_macd"] = df["macd"].shift(1)
    full_daily = df[["date", "open", "high", "low", "close"]].copy()
    full_daily["_date"] = pd.to_datetime(full_daily["date"]).dt.date
    full_daily = full_daily.sort_values("_date").reset_index(drop=True)
    df = df.dropna(subset=["ma60"]).reset_index(drop=True)
    logger.info(f"[{TICKER}] 有效数据 {len(df)} 行")

    weekly_rows = group_by_week(df)
    weeks = backtest_weeks(weekly_rows, df)
    enrich_with_options(weeks, df)
    apply_circuit_breaker(weeks)

    summary = compute_summary(weeks)
    tier_stats = compute_tiers(weeks)
    latest = compute_latest(weekly_rows, df)

    # 为 latest 标记熔断状态
    weeks_asc = sorted(weeks, key=lambda w: w["date"])
    if latest and len(weeks_asc) >= 3:
        last = weeks_asc[-1]
        if last.get("skip"):
            latest["skip"] = True
            latest["skip_reason"] = last["skip_reason"]

    # 4. 附加日K bars（pre_bars/post_bars）
    def _ohlc(r):
        return {"o": round(r["open"], 2), "h": round(r["high"], 2),
                "l": round(r["low"], 2), "c": round(r["close"], 2)}
    for w in weeks:
        entry = datetime.strptime(w["date"], "%Y-%m-%d").date() if isinstance(w["date"], str) else w["date"]
        expiry = datetime.strptime(w["expiry_date"], "%Y-%m-%d").date() if isinstance(w["expiry_date"], str) else w["expiry_date"]
        pre = full_daily[full_daily["_date"] <= entry].tail(21)
        post = full_daily[(full_daily["_date"] > entry) & (full_daily["_date"] <= expiry)]
        w["pre_bars"] = [_ohlc(r) for _, r in pre.iterrows()]
        w["post_bars"] = [_ohlc(r) for _, r in post.iterrows()]

    # 5. IV/HV 数据
    dates = pd.to_datetime(df["date"])
    iv_rows = query_ticker_iv(
        dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d"))
    iv_by_date = {r["date"]: round(r["iv"] * 100, 1) for r in iv_rows}
    hv_by_date = {
        row["date"]: round(row["hist_vol"], 1)
        for _, row in df.iterrows() if pd.notna(row.get("hist_vol"))
    }
    for w in weeks:
        w["iv"] = iv_by_date.get(w["date"])
        w["hv"] = hv_by_date.get(w["date"])

    # 6. Daily bars（图表用）
    df["vol_ma20"] = df["volume"].rolling(window=20, min_periods=20).mean()
    recent = df.tail(60)
    daily_bars = [
        {
            "date": row["date"],
            "open": round(row["open"], 2), "high": round(row["high"], 2),
            "low": round(row["low"], 2), "close": round(row["close"], 2),
            "volume": int(row.get("volume", 0)),
            "dif": round(row["dif"], 4) if pd.notna(row.get("dif")) else None,
            "dea": round(row["dea"], 4) if pd.notna(row.get("dea")) else None,
            "macd": round(row["macd"], 4) if pd.notna(row.get("macd")) else None,
            "ma5": round(row["ma5"], 2) if pd.notna(row.get("ma5")) else None,
            "ma10": round(row["ma10"], 2) if pd.notna(row.get("ma10")) else None,
            "ma20": round(row["ma20"], 2) if pd.notna(row.get("ma20")) else None,
            "ma60": round(row["ma60"], 2) if pd.notna(row.get("ma60")) else None,
            "vol_ma20": round(row["vol_ma20"], 0) if pd.notna(row.get("vol_ma20")) else None,
            "iv": iv_by_date.get(row["date"]),
            "hv": round(row["hist_vol"], 1) if pd.notna(row.get("hist_vol")) else None,
        }
        for _, row in recent.iterrows()
    ]

    # 7. 行情快照
    last_bar = daily_bars[-1] if daily_bars else {}
    prev_bar = daily_bars[-2] if len(daily_bars) >= 2 else {}
    if last_bar and prev_bar:
        market = {
            "date": last_bar["date"],
            "close": last_bar["close"],
            "change_pct": round((last_bar["close"] - prev_bar["close"]) / prev_bar["close"] * 100, 2),
            "iv": last_bar.get("iv"),
            "hv": last_bar.get("hv"),
        }
        active_contracts = []
        for w in sorted(weeks, key=lambda x: x["date"], reverse=True):
            if w.get("pending") and w.get("option_symbol") and not w.get("skip"):
                sym = w["option_symbol"]
                p_idx = sym.rindex("P")
                strike_val = float(sym[p_idx + 1:])
                active_contracts.append({
                    "date": w["date"], "tier": w["tier"], "otm": w.get("otm"),
                    "symbol": w["option_symbol"], "price": w.get("option_price"),
                    "strike": strike_val, "expiry": w.get("expiry_date"),
                    "dte": w.get("option_dte"),
                    "pre_bars": w.get("pre_bars", []),
                    "post_bars": w.get("post_bars", []),
                })
        if latest and latest.get("option_symbol"):
            latest_sym = latest["option_symbol"]
            if not any(c["symbol"] == latest_sym for c in active_contracts):
                sym = latest_sym
                p_idx = sym.rindex("P")
                strike_val = float(sym[p_idx + 1:])
                matched_w = next((w for w in weeks if w["date"] == latest.get("date")), {})
                active_contracts.insert(0, {
                    "date": latest.get("date"), "tier": latest.get("tier"),
                    "otm": latest.get("otm"), "symbol": latest_sym,
                    "price": latest.get("option_price"), "strike": strike_val,
                    "expiry": latest.get("option_expiry"),
                    "dte": latest.get("option_dte"),
                    "pre_bars": matched_w.get("pre_bars", []),
                    "post_bars": matched_w.get("post_bars", []),
                })
        market["active_contracts"] = active_contracts
    else:
        market = None

    # 8. 输出
    data = build_report_data(
        tiers=weeks, summary=summary, tier_stats=tier_stats,
        latest=latest, weeks=weeks, daily_bars=daily_bars,
        market=market,
        data_range=[dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")],
        generated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        otm_config=DEFAULT_OTM,
    )
    save_json(data)
    render_html(data)
    logger.info("全部完成")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 创建 cli/deploy.py**

```python
"""部署入口：读取 HTML → 密码包装 → Cloudflare → Telegram"""
import logging
import os
import sys

from config import TICKER
from output.deploy import wrap_with_password, deploy_to_cloudflare, send_telegram

logger = logging.getLogger(__name__)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    html_path = os.path.join(OUTPUT_DIR, f"{TICKER}.html")
    if not os.path.exists(html_path):
        logger.error(f"文件不存在: {html_path}")
        sys.exit(1)

    password = os.environ.get("LAMBDA_DEPLOY_PASSWORD", "")
    if not password:
        logger.error("缺少环境变量 LAMBDA_DEPLOY_PASSWORD")
        sys.exit(1)

    with open(html_path, "r", encoding="utf-8") as f:
        raw_html = f.read()
    logger.info(f"读取 {html_path} ({len(raw_html)} bytes)")

    wrapped = wrap_with_password(raw_html, password)
    logger.info(f"密码包装完成 ({len(wrapped)} bytes)")

    url = deploy_to_cloudflare(wrapped)
    send_telegram(url)
    logger.info(f"部署完成: {url}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: 创建 cli/sync.py**

```python
"""数据同步入口"""
import logging
import os
import sys

from data.sync.orchestrator import ensure_synced


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    if not api_key:
        print("警告：未设置 MASSIVE_API_KEY，跳过股票数据同步")
    ensure_synced(api_key)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Commit**

```
[feature/tqqq-only-refactor][重构] 新建 cli/ 入口模块（run/deploy/sync）
```

---

### Task 12: 迁移测试文件

**Files:**
- Create: `tests/data/__init__.py`, `tests/data/sync/__init__.py`, `tests/core/__init__.py`, `tests/output/__init__.py`
- Move + modify: 所有 test_*.py 文件

- [ ] **Step 1: 创建测试包结构**

```bash
mkdir -p tests/data/sync tests/core tests/output
touch tests/data/__init__.py tests/data/sync/__init__.py tests/core/__init__.py tests/output/__init__.py
```

- [ ] **Step 2: 迁移测试文件**

每个测试文件的迁移规则：
1. 修改 import 路径（`import data_store` → `from data.store import ...`）
2. 去掉 ticker 参数（直接用 `config.TICKER` 或硬编码 "TQQQ"）
3. 修正 `weeks=3` → `EXPIRY_WEEKS`
4. 合并相关测试

映射关系：

| 旧路径 | 新路径 | 主要 import 变化 |
|--------|--------|-----------------|
| `tests/test_data_store.py` | `tests/data/test_store.py` + `tests/data/test_queries.py` + `tests/data/test_writers.py` | `data_store` → `data.store`, `data.queries`, `data.writers` |
| `tests/test_data_sync.py` | `tests/data/sync/test_orchestrator.py` | `data_sync` → `data.sync.orchestrator` |
| `tests/test_rest_downloader.py` | `tests/data/sync/test_equity.py` + `tests/data/sync/test_splits.py` | `rest_downloader` → `data.sync.equity`, `data.sync.splits` |
| `tests/test_s3_downloader.py` + `tests/test_flat_file_fetcher.py` | `tests/data/sync/test_options.py` | `s3_downloader`+`flat_file_fetcher` → `data.sync.options` |
| `tests/test_iv.py` | `tests/data/sync/test_iv.py` | `iv` → `data.sync.iv` |
| `tests/test_indicators.py` | `tests/core/test_indicators.py` | `indicators` → `core.indicators` |
| `tests/test_strategy.py` | `tests/core/test_strategy.py` + `tests/core/test_backtest.py` | `strategy` → `core.strategy`, `core.backtest` |
| `tests/test_run.py` | `tests/core/test_backtest.py` (enrichment 部分) | `run` → `core.backtest` |
| `tests/test_deploy.py` | `tests/output/test_deploy.py` | `deploy` → `output.deploy` |

具体迁移时需要：
- 逐个文件调整 import
- 运行 `python -m pytest tests/ -v` 验证每个文件迁移后测试通过
- 使用 `patch` 的路径需要同步修改（如 `patch("data_store.xxx")` → `patch("data.queries.xxx")`）

- [ ] **Step 3: 运行全部测试验证**

```bash
python -m pytest tests/ -v
```

预期：全部通过（除 online 标记的）。

- [ ] **Step 4: Commit**

```
[feature/tqqq-only-refactor][重构] 迁移测试文件到新目录结构
```

---

### Task 13: 清理旧文件 + 更新文档

**Files:**
- Delete: 根目录下 11 个旧 Python 文件
- Modify: `CLAUDE.md`
- Modify: `conftest.py`（保留在根目录）

- [ ] **Step 1: 删除旧文件**

```bash
rm run.py deploy.py strategy.py indicators.py iv.py data_sync.py data_store.py s3_downloader.py flat_file_fetcher.py rest_downloader.py
```

注意：`conftest.py` 保留在根目录（pytest 需要）。

- [ ] **Step 2: 删除旧测试文件**

```bash
rm tests/test_data_store.py tests/test_data_sync.py tests/test_rest_downloader.py tests/test_s3_downloader.py tests/test_flat_file_fetcher.py tests/test_iv.py tests/test_indicators.py tests/test_strategy.py tests/test_run.py tests/test_deploy.py
```

- [ ] **Step 3: 最终验证**

```bash
python -m pytest tests/ -v
python -m cli.run --help  # 或直接 python -m cli.run（需要 API key）
```

- [ ] **Step 4: 更新 CLAUDE.md**

更新项目结构、常用命令、文件映射等内容，反映新的三层架构。

主要变更：
- 项目结构：改为 `config.py` + `data/` + `core/` + `output/` + `cli/`
- 常用命令：`python run.py` → `python -m cli.run`，`python deploy.py` → `python -m cli.deploy`，`python data_sync.py` → `python -m cli.sync`
- 删除多标的相关说明
- 更新数据流图

- [ ] **Step 5: Commit**

```
[feature/tqqq-only-refactor][重构] 删除旧文件，更新 CLAUDE.md
```

---

### Task 14: 最终集成验证

- [ ] **Step 1: 全量测试**

```bash
python -m pytest tests/ -v
```

- [ ] **Step 2: 端到端验证（需 API key）**

```bash
python -m cli.run
# 检查 output/TQQQ.json 和 output/TQQQ.html 生成正确
# 双击 HTML 确认报告功能完整
```

- [ ] **Step 3: Commit（如有修复）**

```
[feature/tqqq-only-refactor][修复] 集成验证修复
```
