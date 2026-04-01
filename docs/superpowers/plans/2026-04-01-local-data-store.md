# Local Data Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建本地 DuckDB 数据库，通过 S3（期权）和 REST API（股票）两条通道下载并存储历史行情，替代现有的实时 API 调用。

**Architecture:** 四个模块分离职责：`data_store.py` 封装 DuckDB；`s3_downloader.py` 下载全量期权 flat files；`rest_downloader.py` 按指定标的拉股票日K；`data_sync.py` 作 CLI 编排入口。`entry_optimizer.py` 和 `run.py` 最终改为从本地 DB 查询。

**Tech Stack:** Python 3.13, DuckDB, boto3, requests, pytest

---

## 文件结构

| 文件 | 操作 | 职责 |
|---|---|---|
| `data_store.py` | 新建 | DuckDB 连接、建表、读写查询接口 |
| `s3_downloader.py` | 新建 | S3 下载期权 flat files，写入 option_bars |
| `rest_downloader.py` | 新建 | REST API 拉股票日K，写入 equity_bars |
| `data_sync.py` | 新建 | CLI 编排：调度下载器，全量/增量逻辑 |
| `entry_optimizer.py` | 修改 | `enrich_with_flat_files()` 改为查询本地 DB |
| `run.py` | 修改 | `fetch_daily_bars()` 优先查本地 DB |
| `tests/test_data_store.py` | 新建 | data_store 单元测试 |
| `tests/test_s3_downloader.py` | 新建 | s3_downloader 单元测试 |
| `tests/test_rest_downloader.py` | 新建 | rest_downloader 单元测试 |
| `tests/test_data_sync.py` | 新建 | data_sync 集成测试 |
| `requirements.txt` | 修改 | 确认 duckdb 已包含 |

数据库文件位置：`output/market_data.duckdb`（已在 .gitignore）

---

## Task 1: data_store.py — DuckDB 建表与查询接口

**Files:**
- Create: `data_store.py`
- Test: `tests/test_data_store.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_data_store.py
import datetime
import pytest
import duckdb
from unittest.mock import patch
from pathlib import Path
import data_store


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        yield db_path


def test_init_creates_tables(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "equity_bars" in tables
    assert "option_bars" in tables
    assert "sync_log" in tables
    con.close()


def test_upsert_equity_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute("SELECT close FROM equity_bars WHERE ticker='TQQQ'").fetchone()
    con.close()
    assert result[0] == 42.5


def test_upsert_equity_bars_deduplicates(tmp_db):
    row = {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
           "high": 43.0, "low": 41.0, "close": 42.5,
           "volume": 1000000, "vwap": 42.3, "transactions": 5000}
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars([row])
        row["close"] = 99.0  # 更新值
        data_store.upsert_equity_bars([row])
    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    close = con.execute("SELECT close FROM equity_bars").fetchone()[0]
    con.close()
    assert count == 1
    assert close == 99.0


def test_upsert_option_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute(
        "SELECT close FROM option_bars WHERE symbol='O:TQQQ250131P00038500'"
    ).fetchone()
    con.close()
    assert result[0] == 0.87


def test_query_option_bars_returns_sorted(tmp_db):
    rows = [
        {"date": "2025-01-07", "symbol": "O:TQQQ250131P00038500",
         "open": 0.87, "high": 0.95, "low": 0.85, "close": 0.92,
         "volume": 5, "transactions": 2},
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
        bars = data_store.query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-07"
        )
    assert len(bars) == 2
    assert bars[0]["date"] == "2025-01-06"
    assert bars[1]["date"] == "2025-01-07"


def test_query_option_bars_filters_by_symbol(tmp_db):
    rows = [
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
        {"date": "2025-01-06", "symbol": "O:QQQ250131P00400000",
         "open": 1.0, "high": 1.5, "low": 0.9, "close": 1.2,
         "volume": 5, "transactions": 2},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
        bars = data_store.query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-06"
        )
    assert len(bars) == 1
    assert bars[0]["symbol"] == "O:TQQQ250131P00038500"


def test_query_equity_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
        {"date": "2025-01-07", "ticker": "TQQQ", "open": 42.5,
         "high": 44.0, "low": 42.0, "close": 43.8,
         "volume": 900000, "vwap": 43.1, "transactions": 4500},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows)
        bars = data_store.query_equity_bars("TQQQ", "2025-01-06", "2025-01-07")
    assert len(bars) == 2
    assert bars[0]["date"] == "2025-01-06"
    assert bars[0]["close"] == 42.5


def test_get_latest_option_date_returns_none_when_empty(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        result = data_store.get_latest_synced_date("option")
    assert result is None


def test_get_latest_option_date(tmp_db):
    rows = [
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
        {"date": "2025-01-07", "symbol": "O:TQQQ250131P00038500",
         "open": 0.87, "high": 0.95, "low": 0.85, "close": 0.92,
         "volume": 5, "transactions": 2},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
        result = data_store.get_latest_synced_date("option")
    assert result == "2025-01-07"


def test_write_sync_log(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.write_sync_log("2025-01-06", "option", 260000, "ok")
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT * FROM sync_log").fetchone()
    con.close()
    assert row is not None
    assert row[3] == datetime.date(2025, 1, 6)  # date column
```

- [ ] **Step 2: 确认测试失败**

```bash
.venv/bin/python -m pytest tests/test_data_store.py -v
```

预期：`ModuleNotFoundError: No module named 'data_store'`

- [ ] **Step 3: 安装 duckdb 并更新 requirements.txt**

```bash
.venv/bin/pip install duckdb
```

`requirements.txt` 末尾加一行：
```
duckdb>=0.10
```

- [ ] **Step 4: 实现 data_store.py**

```python
"""本地 DuckDB 数据存储：建表、upsert、查询接口。

数据库文件：output/market_data.duckdb
"""
import datetime
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "output" / "market_data.duckdb"

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
    PRIMARY KEY (date, symbol)
)
"""

_CREATE_SYNC_LOG = """
CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1;
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER  DEFAULT nextval('sync_log_seq'),
    ts           TIMESTAMP NOT NULL,
    date         DATE     NOT NULL,
    data_type    VARCHAR  NOT NULL,
    rows_written INTEGER  NOT NULL,
    status       VARCHAR  NOT NULL,
    message      VARCHAR
)
"""


def _connect() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    """建表（幂等，已存在则跳过）。"""
    con = _connect()
    con.execute(_CREATE_EQUITY)
    con.execute(_CREATE_OPTION)
    con.execute(_CREATE_SYNC_LOG)
    con.close()
    logger.info(f"DB 初始化完成: {DB_PATH}")


def upsert_equity_bars(rows: list[dict]) -> int:
    """批量写入/更新股票日K。主键冲突时覆盖。

    Args:
        rows: list of {date, ticker, open, high, low, close, volume, vwap, transactions}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = _connect()
    con.executemany(
        """
        INSERT OR REPLACE INTO equity_bars
            (date, ticker, open, high, low, close, volume, vwap, transactions)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(r["date"], r["ticker"], r["open"], r["high"], r["low"],
          r["close"], r.get("volume"), r.get("vwap"), r.get("transactions"))
         for r in rows],
    )
    con.close()
    return len(rows)


def upsert_option_bars(rows: list[dict]) -> int:
    """批量写入/更新期权日K。主键冲突时覆盖。

    Args:
        rows: list of {date, symbol, open, high, low, close, volume, transactions}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = _connect()
    con.executemany(
        """
        INSERT OR REPLACE INTO option_bars
            (date, symbol, open, high, low, close, volume, transactions)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(r["date"], r["symbol"], r["open"], r["high"], r["low"],
          r["close"], r.get("volume"), r.get("transactions"))
         for r in rows],
    )
    con.close()
    return len(rows)


def query_option_bars(symbol: str, from_date: str,
                      to_date: str) -> list[dict]:
    """查询指定期权合约在日期范围内的日K数据。

    Returns:
        [{date, symbol, open, high, low, close}] 按日期升序
    """
    con = _connect()
    rows = con.execute(
        """
        SELECT date, symbol, open, high, low, close
        FROM option_bars
        WHERE symbol = ? AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [symbol, from_date, to_date],
    ).fetchall()
    con.close()
    return [
        {"date": str(r[0]), "symbol": r[1], "open": r[2],
         "high": r[3], "low": r[4], "close": r[5]}
        for r in rows
    ]


def query_equity_bars(ticker: str, from_date: str,
                      to_date: str) -> list[dict]:
    """查询指定股票在日期范围内的日K数据。

    Returns:
        [{date, ticker, open, high, low, close, volume, vwap, transactions}] 按日期升序
    """
    con = _connect()
    rows = con.execute(
        """
        SELECT date, ticker, open, high, low, close, volume, vwap, transactions
        FROM equity_bars
        WHERE ticker = ? AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [ticker, from_date, to_date],
    ).fetchall()
    con.close()
    return [
        {"date": str(r[0]), "ticker": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6],
         "vwap": r[7], "transactions": r[8]}
        for r in rows
    ]


def get_latest_synced_date(data_type: str) -> str | None:
    """返回已同步的最新日期，无数据返回 None。

    Args:
        data_type: 'option' | 'equity'
    """
    table = "option_bars" if data_type == "option" else "equity_bars"
    con = _connect()
    result = con.execute(f"SELECT MAX(date) FROM {table}").fetchone()
    con.close()
    if result and result[0]:
        return str(result[0])
    return None


def write_sync_log(date: str, data_type: str, rows_written: int,
                   status: str, message: str = None) -> None:
    """写入一条同步记录。"""
    con = _connect()
    con.execute(
        """
        INSERT INTO sync_log (ts, date, data_type, rows_written, status, message)
        VALUES (now(), ?, ?, ?, ?, ?)
        """,
        [date, data_type, rows_written, status, message],
    )
    con.close()
```

- [ ] **Step 5: 运行测试，确认全部通过**

```bash
.venv/bin/python -m pytest tests/test_data_store.py -v
```

预期：所有测试 PASS

- [ ] **Step 6: Commit**

```bash
git add data_store.py tests/test_data_store.py requirements.txt
git commit -m "[feature/weekly-strategy][功能] 新增 data_store：DuckDB 建表与读写接口"
```

---

## Task 2: s3_downloader.py — S3 期权 Flat Files 下载

**Files:**
- Create: `s3_downloader.py`
- Test: `tests/test_s3_downloader.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_s3_downloader.py
import csv
import gzip
import io
import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

import pytest
import s3_downloader
import data_store


def _make_csv_gz(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="w") as gz:
        gz.write(buf.getvalue().encode())
    return out.getvalue()


@pytest.fixture
def mock_s3():
    return MagicMock()


def test_trading_days_excludes_weekends():
    days = s3_downloader.trading_days("2025-01-04", "2025-01-10")
    weekdays = {datetime.date.fromisoformat(d).weekday() for d in days}
    assert 5 not in weekdays  # 周六
    assert 6 not in weekdays  # 周日
    assert "2025-01-06" in days  # 周一
    assert "2025-01-10" in days  # 周五


def test_trading_days_range():
    days = s3_downloader.trading_days("2025-01-06", "2025-01-10")
    assert days == ["2025-01-06", "2025-01-07", "2025-01-08",
                    "2025-01-09", "2025-01-10"]


def test_download_and_store_day_success(tmp_path, mock_s3):
    sample_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10",
         "open": "0.85", "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
        {"ticker": "O:QQQ250131P00400000", "volume": "5",
         "open": "1.0", "close": "1.2", "high": "1.5", "low": "0.9",
         "window_start": "1000", "transactions": "2"},
    ]
    mock_s3.get_object.return_value = {
        "Body": io.BytesIO(_make_csv_gz(sample_rows))
    }

    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        count = s3_downloader.download_and_store_day("2025-01-06", mock_s3)

    assert count == 2
    import duckdb
    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT COUNT(*) FROM option_bars").fetchone()[0]
    con.close()
    assert rows == 2


def test_download_and_store_day_skips_holiday(mock_s3):
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject"
    )
    count = s3_downloader.download_and_store_day("2025-01-01", mock_s3)
    assert count == 0


def test_download_and_store_day_skips_existing(tmp_path, mock_s3):
    """已有数据的日期不重复下载。"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_option_bars([{
            "date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
            "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
            "volume": 10, "transactions": 3,
        }])
        data_store.write_sync_log("2025-01-06", "option", 1, "ok")
        count = s3_downloader.download_and_store_day("2025-01-06", mock_s3)

    mock_s3.get_object.assert_not_called()
    assert count == -1  # 跳过标记


def test_sync_options_processes_date_range(tmp_path, mock_s3):
    sample_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "5",
         "open": "0.85", "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "2"},
    ]
    mock_s3.get_object.return_value = {
        "Body": io.BytesIO(_make_csv_gz(sample_rows))
    }
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("s3_downloader.make_s3_client", return_value=mock_s3):
            s3_downloader.sync_options("2025-01-06", "2025-01-07")
    # 两个交易日各调用一次
    assert mock_s3.get_object.call_count == 2
```

- [ ] **Step 2: 确认测试失败**

```bash
.venv/bin/python -m pytest tests/test_s3_downloader.py -v
```

预期：`ModuleNotFoundError: No module named 's3_downloader'`

- [ ] **Step 3: 实现 s3_downloader.py**

```python
"""S3 期权 Flat Files 下载器：逐日下载全量期权日K并存入本地 DB。

S3 路径: us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
认证环境变量: MASSIVE_S3_ACCESS_KEY, MASSIVE_S3_SECRET_KEY
             MASSIVE_S3_ENDPOINT（默认 https://files.massive.com）
             MASSIVE_S3_BUCKET（默认 flatfiles）
"""
import csv
import datetime
import gzip
import io
import logging
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import data_store

logger = logging.getLogger(__name__)

_BUCKET = os.environ.get("MASSIVE_S3_BUCKET", "flatfiles")
_ENDPOINT = os.environ.get("MASSIVE_S3_ENDPOINT", "https://files.massive.com")
_PREFIX = "us_options_opra/day_aggs_v1"


def make_s3_client():
    """从环境变量创建 S3 客户端。"""
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["MASSIVE_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MASSIVE_S3_SECRET_KEY"],
        endpoint_url=_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )


def trading_days(from_date: str, to_date: str) -> list[str]:
    """生成日期范围内所有周一至周五的日期列表（不排除节假日）。"""
    start = datetime.date.fromisoformat(from_date)
    end = datetime.date.fromisoformat(to_date)
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=周一 … 4=周五
            days.append(str(current))
        current += datetime.timedelta(days=1)
    return days


def _already_synced(date_str: str) -> bool:
    """检查该日期是否已在 sync_log 中有 ok 记录。"""
    import duckdb
    con = duckdb.connect(str(data_store.DB_PATH))
    result = con.execute(
        "SELECT COUNT(*) FROM sync_log WHERE date=? AND data_type='option' AND status='ok'",
        [date_str],
    ).fetchone()[0]
    con.close()
    return result > 0


def download_and_store_day(date_str: str, s3_client) -> int:
    """下载指定日期的期权全量文件并写入 DB。

    Returns:
        写入行数；0 表示节假日/文件不存在；-1 表示已有数据跳过
    """
    if _already_synced(date_str):
        logger.debug(f"[s3] {date_str} 已同步，跳过")
        return -1

    d = datetime.date.fromisoformat(date_str)
    key = f"{_PREFIX}/{d.year}/{d.month:02d}/{date_str}.csv.gz"
    try:
        resp = s3_client.get_object(Bucket=_BUCKET, Key=key)
        raw = resp["Body"].read()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            logger.debug(f"[s3] {date_str} 非交易日，跳过")
            return 0
        raise

    rows = []
    with gzip.open(io.BytesIO(raw), "rt", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "date": date_str,
                "symbol": row["ticker"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]) if row["volume"] else None,
                "transactions": int(row["transactions"]) if row["transactions"] else None,
            })

    written = data_store.upsert_option_bars(rows)
    data_store.write_sync_log(date_str, "option", written, "ok")
    logger.info(f"[s3] {date_str}: {written:,} 行写入 option_bars")
    return written


def sync_options(from_date: str, to_date: str, s3_client=None) -> None:
    """同步指定日期范围内的期权数据。

    Args:
        from_date: 起始日期 "YYYY-MM-DD"
        to_date:   结束日期 "YYYY-MM-DD"
        s3_client: boto3 S3 客户端（None 则自动创建）
    """
    if s3_client is None:
        s3_client = make_s3_client()

    days = trading_days(from_date, to_date)
    logger.info(f"[s3] 同步期权数据 {from_date} ~ {to_date}，共 {len(days)} 个交易日")
    for date_str in days:
        try:
            download_and_store_day(date_str, s3_client)
        except Exception as e:
            data_store.write_sync_log(date_str, "option", 0, "error", str(e))
            logger.error(f"[s3] {date_str} 下载失败: {e}")
```

- [ ] **Step 4: 运行测试**

```bash
.venv/bin/python -m pytest tests/test_s3_downloader.py -v
```

预期：所有测试 PASS

- [ ] **Step 5: Commit**

```bash
git add s3_downloader.py tests/test_s3_downloader.py
git commit -m "[feature/weekly-strategy][功能] 新增 s3_downloader：S3 期权全量下载"
```

---

## Task 3: rest_downloader.py — REST API 股票日K 下载

**Files:**
- Create: `rest_downloader.py`
- Test: `tests/test_rest_downloader.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_rest_downloader.py
from unittest.mock import patch, MagicMock
from pathlib import Path
import pytest
import rest_downloader
import data_store


def _mock_response(results: list[dict], status=200):
    mock = MagicMock()
    mock.status_code = status
    mock.json.return_value = {"results": results, "status": "OK"}
    mock.raise_for_status = MagicMock()
    return mock


SAMPLE_BARS = [
    {"t": 1736139600000, "o": 42.0, "h": 43.0, "l": 41.0,
     "c": 42.5, "v": 1000000, "vw": 42.3, "n": 5000},
    {"t": 1736226000000, "o": 42.5, "h": 44.0, "l": 42.0,
     "c": 43.8, "v": 900000, "vw": 43.1, "n": 4500},
]


def test_fetch_and_store_equity_writes_to_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   return_value=_mock_response(SAMPLE_BARS)):
            count = rest_downloader.fetch_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_api_key"
            )
    assert count == 2
    import duckdb
    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    close = con.execute(
        "SELECT close FROM equity_bars WHERE date='2025-01-06'"
    ).fetchone()[0]
    con.close()
    assert rows == 2
    assert close == 42.5


def test_fetch_and_store_equity_returns_zero_on_empty(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   return_value=_mock_response([])):
            count = rest_downloader.fetch_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_api_key"
            )
    assert count == 0


def test_fetch_and_store_equity_handles_429(tmp_path):
    db_path = tmp_path / "test.duckdb"
    mock_429 = MagicMock()
    mock_429.status_code = 429
    ok_resp = _mock_response(SAMPLE_BARS)

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   side_effect=[mock_429, ok_resp]), \
             patch("rest_downloader.time.sleep"):
            count = rest_downloader.fetch_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_api_key"
            )
    assert count == 2


def test_sync_equity_calls_each_ticker(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.fetch_and_store_equity",
                   return_value=2) as mock_fetch:
            rest_downloader.sync_equity(
                ["TQQQ", "QQQ"], "2025-01-06", "2025-01-07", "test_key"
            )
    assert mock_fetch.call_count == 2
    calls_tickers = {c.args[0] for c in mock_fetch.call_args_list}
    assert calls_tickers == {"TQQQ", "QQQ"}
```

- [ ] **Step 2: 确认测试失败**

```bash
.venv/bin/python -m pytest tests/test_rest_downloader.py -v
```

预期：`ModuleNotFoundError: No module named 'rest_downloader'`

- [ ] **Step 3: 实现 rest_downloader.py**

```python
"""REST API 股票日K 下载器：按指定标的拉取日K并存入本地 DB。

使用 Massive /v2/aggs 端点，认证通过 MASSIVE_API_KEY 环境变量。
"""
import datetime
import logging
import os
import time

import requests

import data_store

logger = logging.getLogger(__name__)

BASE_URL = "https://api.massive.com"
MAX_RETRIES = 5
RETRY_DELAY = 15


def fetch_and_store_equity(ticker: str, from_date: str, to_date: str,
                            api_key: str) -> int:
    """拉取指定股票的日K并写入 equity_bars。

    Args:
        ticker:    股票代码，如 "TQQQ"
        from_date: 起始日期 "YYYY-MM-DD"
        to_date:   结束日期 "YYYY-MM-DD"
        api_key:   Massive REST API Key

    Returns:
        写入行数
    """
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    params = {"adjusted": "false", "sort": "asc",
              "limit": 50000, "apiKey": api_key}

    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            wait = RETRY_DELAY * (attempt + 1)
            logger.warning(f"[rest] {ticker} 限流(429)，等待 {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        logger.error(f"[rest] {ticker} 重试 {MAX_RETRIES} 次后放弃")
        return 0

    raw = resp.json().get("results", [])
    if not raw:
        logger.info(f"[rest] {ticker} {from_date}~{to_date} 无数据")
        return 0

    rows = []
    for r in raw:
        dt = datetime.datetime.fromtimestamp(
            r["t"] / 1000, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d")
        rows.append({
            "date": dt,
            "ticker": ticker,
            "open": r["o"],
            "high": r["h"],
            "low": r["l"],
            "close": r["c"],
            "volume": r.get("v"),
            "vwap": r.get("vw"),
            "transactions": r.get("n"),
        })

    written = data_store.upsert_equity_bars(rows)
    logger.info(f"[rest] {ticker} {from_date}~{to_date}: {written} 行写入 equity_bars")
    return written


def sync_equity(tickers: list[str], from_date: str, to_date: str,
                api_key: str) -> None:
    """同步多个股票标的的日K数据。

    Args:
        tickers:   股票代码列表，如 ["TQQQ", "QQQ"]
        from_date: 起始日期
        to_date:   结束日期
        api_key:   Massive REST API Key
    """
    logger.info(f"[rest] 同步股票 {tickers} {from_date}~{to_date}")
    for ticker in tickers:
        try:
            fetch_and_store_equity(ticker, from_date, to_date, api_key)
        except Exception as e:
            logger.error(f"[rest] {ticker} 同步失败: {e}")
```

- [ ] **Step 4: 运行测试**

```bash
.venv/bin/python -m pytest tests/test_rest_downloader.py -v
```

预期：所有测试 PASS

- [ ] **Step 5: Commit**

```bash
git add rest_downloader.py tests/test_rest_downloader.py
git commit -m "[feature/weekly-strategy][功能] 新增 rest_downloader：REST API 股票日K下载"
```

---

## Task 4: data_sync.py — CLI 编排

**Files:**
- Create: `data_sync.py`
- Test: `tests/test_data_sync.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_data_sync.py
import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock, call
import pytest
import data_sync
import data_store


def test_date_range_from_years():
    from_date, to_date = data_sync.date_range_from_years(2)
    today = datetime.date.today()
    assert to_date == str(today - datetime.timedelta(days=1))
    start = datetime.date.fromisoformat(from_date)
    assert (today - start).days >= 365 * 2 - 1


def test_full_sync_calls_both_downloaders(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path), \
         patch("data_sync.s3_downloader.sync_options") as mock_s3, \
         patch("data_sync.rest_downloader.sync_equity") as mock_rest:
        data_sync.full_sync(
            years=1, tickers=["TQQQ"], api_key="key"
        )
    mock_s3.assert_called_once()
    mock_rest.assert_called_once()
    # equity 调用包含 tickers 列表
    assert mock_rest.call_args.args[0] == ["TQQQ"]


def test_incremental_sync_starts_from_next_day(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_option_bars([{
            "date": "2025-06-01", "symbol": "O:TQQQ250131P00038500",
            "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
            "volume": 10, "transactions": 3,
        }])
        with patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest:
            data_sync.incremental_sync(tickers=["TQQQ"], api_key="key")

    # 起始日期是已有最新日期的次日
    s3_from = mock_s3.call_args.args[0]
    assert s3_from == "2025-06-02"


def test_incremental_sync_when_no_existing_data(tmp_path):
    """无历史数据时增量同步默认拉最近 30 天。"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity"):
            data_sync.incremental_sync(tickers=["TQQQ"], api_key="key")

    today = datetime.date.today()
    s3_from = datetime.date.fromisoformat(mock_s3.call_args.args[0])
    assert (today - s3_from).days >= 29
```

- [ ] **Step 2: 确认测试失败**

```bash
.venv/bin/python -m pytest tests/test_data_sync.py -v
```

预期：`ModuleNotFoundError: No module named 'data_sync'`

- [ ] **Step 3: 实现 data_sync.py**

```python
"""数据同步 CLI：调度 S3 期权下载和 REST 股票下载。

用法:
    python data_sync.py --years 2 --tickers TQQQ QQQ   # 全量建库
    python data_sync.py --incremental --tickers TQQQ   # 增量补齐到昨天
    python data_sync.py --incremental                  # 仅同步期权（无需 ticker）
"""
import argparse
import datetime
import logging
import os
import sys

import data_store
import s3_downloader
import rest_downloader

logger = logging.getLogger(__name__)


def date_range_from_years(years: int) -> tuple[str, str]:
    """返回 (from_date, to_date)，to_date 为昨天，from_date 为 years 年前。"""
    today = datetime.date.today()
    to_date = today - datetime.timedelta(days=1)
    from_date = today.replace(year=today.year - years)
    return str(from_date), str(to_date)


def full_sync(years: int, tickers: list[str], api_key: str) -> None:
    """全量同步：S3 期权 + REST 股票。"""
    from_date, to_date = date_range_from_years(years)
    logger.info(f"全量同步 {from_date} ~ {to_date}，标的: {tickers}")

    data_store.init_db()
    s3_downloader.sync_options(from_date, to_date)

    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)


def incremental_sync(tickers: list[str], api_key: str) -> None:
    """增量同步：从上次最新日期的次日同步到昨天。

    无历史数据时默认补最近 30 天。
    """
    data_store.init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))

    latest = data_store.get_latest_synced_date("option")
    if latest:
        from_date = str(datetime.date.fromisoformat(latest)
                        + datetime.timedelta(days=1))
    else:
        from_date = str(today - datetime.timedelta(days=30))

    if from_date > to_date:
        logger.info("数据已是最新，无需同步")
        return

    logger.info(f"增量同步 {from_date} ~ {to_date}")
    s3_downloader.sync_options(from_date, to_date)

    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Lambda 策略数据同步")
    parser.add_argument("--years", type=int, default=2,
                        help="全量同步年数（默认 2）")
    parser.add_argument("--tickers", nargs="*", default=[],
                        help="股票标的列表，如 TQQQ QQQ")
    parser.add_argument("--incremental", action="store_true",
                        help="增量同步模式")
    args = parser.parse_args()

    api_key = os.environ.get("MASSIVE_API_KEY", "")

    if args.incremental:
        incremental_sync(tickers=args.tickers, api_key=api_key)
    else:
        if not api_key and args.tickers:
            print("警告：未设置 MASSIVE_API_KEY，跳过股票数据同步")
        full_sync(years=args.years, tickers=args.tickers, api_key=api_key)

    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: 运行测试**

```bash
.venv/bin/python -m pytest tests/test_data_sync.py -v
```

预期：所有测试 PASS

- [ ] **Step 5: Commit**

```bash
git add data_sync.py tests/test_data_sync.py
git commit -m "[feature/weekly-strategy][功能] 新增 data_sync：全量/增量 CLI 编排"
```

---

## Task 5: 集成 entry_optimizer.py

**Files:**
- Modify: `entry_optimizer.py`
- Modify: `tests/test_entry_optimizer.py`

- [ ] **Step 1: 写失败测试（追加到现有测试文件）**

在 `tests/test_entry_optimizer.py` 末尾追加：

```python
# ── enrich_with_db ───────────────────────────────────────

class TestEnrichWithDb:
    def test_uses_db_query(self, tmp_path):
        """enrich_with_db 从 data_store.query_option_bars 获取数据。"""
        import data_store
        from entry_optimizer import enrich_with_db

        db_bars = [
            {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
             "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87},
            {"date": "2025-01-07", "symbol": "O:TQQQ250131P00038500",
             "open": 0.87, "high": 0.95, "low": 0.85, "close": 0.92},
            {"date": "2025-01-08", "symbol": "O:TQQQ250131P00038500",
             "open": 0.92, "high": 0.98, "low": 0.88, "close": 0.95},
            {"date": "2025-01-09", "symbol": "O:TQQQ250131P00038500",
             "open": 0.95, "high": 1.00, "low": 0.90, "close": 0.97},
            {"date": "2025-01-10", "symbol": "O:TQQQ250131P00038500",
             "open": 0.97, "high": 1.02, "low": 0.93, "close": 0.99},
        ]
        with patch("entry_optimizer.data_store.query_option_bars",
                   return_value=db_bars):
            result = enrich_with_db([COMPLETE_TRADE])

        assert len(result) == 1
        assert result[0]["data_complete"] is True
        assert result[0]["mon_close_option"] == 0.87
        assert result[0]["week_high"] == 1.02

    def test_data_incomplete_when_db_empty(self):
        from entry_optimizer import enrich_with_db
        with patch("entry_optimizer.data_store.query_option_bars",
                   return_value=[]):
            result = enrich_with_db([COMPLETE_TRADE])
        assert result[0]["data_complete"] is False
```

- [ ] **Step 2: 确认新测试失败**

```bash
.venv/bin/python -m pytest tests/test_entry_optimizer.py::TestEnrichWithDb -v
```

预期：`ImportError: cannot import name 'enrich_with_db'`

- [ ] **Step 3: 在 entry_optimizer.py 中添加 enrich_with_db 函数**

在 `enrich_with_flat_files` 函数之后添加：

```python
def enrich_with_db(trades: list[dict], ticker: str = "TQQQ") -> list[dict]:
    """通过本地 DuckDB 富化信号交易（最快，无网络开销）。

    Args:
        trades: get_signal_trades() 输出
        ticker: 标的代码（默认 TQQQ）

    Returns:
        trades 的副本，追加期权日线字段。
    """
    import data_store as _data_store

    def fetch_fn(symbol, from_date, to_date):
        bars = _data_store.query_option_bars(symbol, from_date, to_date)
        return [{"date": b["date"], "open": b["open"], "high": b["high"],
                 "low": b["low"], "close": b["close"]} for b in bars]

    return _enrich(trades, ticker, fetch_fn)
```

更新 `main()` 的数据来源优先级，在 `s3_access_key` 判断之前插入 DB 优先检查：

```python
    # 数据来源优先级：本地 DB > S3 > REST API
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "output", "market_data.duckdb")
    if os.path.exists(db_path):
        print("数据来源: 本地 DuckDB")
        enriched = enrich_with_db(trades, ticker=args.ticker.upper())
    elif os.environ.get("MASSIVE_S3_ACCESS_KEY"):
        from flat_file_fetcher import make_s3_client
        s3_client = make_s3_client()
        print("数据来源: Flat Files（S3）")
        enriched = enrich_with_flat_files(trades, s3_client, ticker=args.ticker.upper())
    else:
        api_key = os.environ.get("MASSIVE_API_KEY")
        if not api_key:
            print("错误：未找到本地 DB，且未设置 S3 或 REST API 凭据")
            return 1
        print("数据来源: REST API（近 4 个月）")
        enriched = enrich_with_option_data(trades, api_key, ticker=args.ticker.upper())
```

- [ ] **Step 4: 运行所有 optimizer 测试**

```bash
.venv/bin/python -m pytest tests/test_entry_optimizer.py -v
```

预期：所有测试 PASS（含新增 2 个）

- [ ] **Step 5: Commit**

```bash
git add entry_optimizer.py tests/test_entry_optimizer.py
git commit -m "[feature/weekly-strategy][功能] entry_optimizer：优先从本地 DB 查询期权数据"
```

---

## Task 6: 全量测试 + 文档更新

**Files:**
- Modify: `requirements.txt`（确认 duckdb 已加）
- Modify: `README.md` 或 `docs/` 对应模块文档（如有）

- [ ] **Step 1: 运行所有测试**

```bash
.venv/bin/python -m pytest tests/ -v
```

预期：全部 PASS，无 SKIP

- [ ] **Step 2: 验证 CLI 可调用（dry run）**

```bash
.venv/bin/python data_sync.py --help
```

预期：打印参数说明，无报错

- [ ] **Step 3: 验证数据库初始化**

```bash
.venv/bin/python -c "
import data_store
data_store.init_db()
import duckdb
con = duckdb.connect('output/market_data.duckdb')
print(con.execute('SHOW TABLES').fetchall())
con.close()
"
```

预期：`[('equity_bars',), ('option_bars',), ('sync_log',)]`

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "[feature/weekly-strategy][文档] data store：更新依赖和使用说明"
```
