# 拆股前复权调整 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 引入拆股数据，对 equity_bars 和 option_bars 做前复权处理，确保跨拆股周期的策略计算正确。

**Architecture:** 新增 splits 表存储拆股事件；equity 改用 `adjusted=true` 由 API 返回前复权价格；option 入库时在 SQL 层按累积因子调整价格/volume/symbol；ensure_synced 流程前置拆股检测，发现新事件时清空重拉。

**Tech Stack:** Python 3, DuckDB, Massive REST API (`/stocks/v1/splits`, `/v2/aggs`), S3 flat files

---

## 文件变更清单

| 文件 | 变更 |
|------|------|
| `data_store.py` | 新增 splits 表 DDL；新增 `upsert_splits`、`query_splits`、`compute_split_factor`、`delete_ticker_data` 方法；`insert_option_bars_from_csv` 新增 `split_factor` 参数 |
| `rest_downloader.py` | 新增 `download_splits` 函数；`download_and_store_equity` 参数 `adjusted=false` → `adjusted=true` |
| `data_sync.py` | `ensure_synced` 新增拆股同步 + 新拆股检测 + 清空重拉逻辑 |
| `s3_downloader.py` | `download_and_store_day` 和 `sync_options` 传递 split_factor 到 `insert_option_bars_from_csv` |
| `tests/test_data_store.py` | splits 表 CRUD、因子计算、期权复权入库测试 |
| `tests/test_rest_downloader.py` | download_splits 测试 |
| `tests/test_data_sync.py` | ensure_synced 拆股检测 + 清空重拉测试 |

---

### Task 1: data_store.py — splits 表与基础 CRUD

**Files:**
- Modify: `data_store.py`
- Test: `tests/test_data_store.py`

- [ ] **Step 1: 写测试 — splits 表建表**

在 `tests/test_data_store.py` 新增：

```python
def test_init_creates_splits_table(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "splits" in tables
    con.close()
```

- [ ] **Step 2: 写测试 — upsert_splits 写入和幂等性**

```python
def test_upsert_splits(tmp_db):
    rows = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2}]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute("SELECT * FROM splits").fetchall()
    con.close()
    assert len(result) == 1
    assert result[0][0] == "TQQQ"
    assert result[0][2] == 1  # split_from
    assert result[0][3] == 2  # split_to


def test_upsert_splits_idempotent(tmp_db):
    rows = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2}]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits(rows)
        data_store.upsert_splits(rows)  # 重复写入
    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM splits").fetchone()[0]
    con.close()
    assert count == 1
```

- [ ] **Step 3: 写测试 — query_splits**

```python
def test_query_splits(tmp_db):
    rows = [
        {"ticker": "TQQQ", "exec_date": "2025-11-20",
         "split_from": 1, "split_to": 2},
        {"ticker": "QQQ", "exec_date": "2025-06-01",
         "split_from": 1, "split_to": 3},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits(rows)
        result = data_store.query_splits("TQQQ")
    assert len(result) == 1
    assert result[0]["ticker"] == "TQQQ"
    assert result[0]["exec_date"] == "2025-11-20"
    assert result[0]["split_from"] == 1
    assert result[0]["split_to"] == 2


def test_query_splits_empty(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        result = data_store.query_splits("TQQQ")
    assert result == []
```

- [ ] **Step 4: 实现 splits 表 DDL + upsert_splits + query_splits**

在 `data_store.py` 新增：

```python
_CREATE_SPLITS = """
CREATE TABLE IF NOT EXISTS splits (
    ticker       VARCHAR  NOT NULL,
    exec_date    DATE     NOT NULL,
    split_from   INTEGER  NOT NULL,
    split_to     INTEGER  NOT NULL,
    PRIMARY KEY (ticker, exec_date)
)
"""
```

在 `init_db()` 中追加 `con.execute(_CREATE_SPLITS)`。

新增函数：

```python
def upsert_splits(rows: list[dict]) -> int:
    """批量写入拆股记录（主键冲突时忽略）。

    Args:
        rows: list of {ticker, exec_date, split_from, split_to}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = _connect()
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


def query_splits(ticker: str) -> list[dict]:
    """查询指定 ticker 的所有拆股记录，按执行日期升序。

    Returns:
        [{ticker, exec_date, split_from, split_to}]
    """
    con = _connect()
    try:
        rows = con.execute(
            "SELECT ticker, exec_date, split_from, split_to "
            "FROM splits WHERE ticker = ? ORDER BY exec_date",
            [ticker],
        ).fetchall()
    finally:
        con.close()
    return [
        {"ticker": r[0], "exec_date": str(r[1]),
         "split_from": r[2], "split_to": r[3]}
        for r in rows
    ]
```

- [ ] **Step 5: 用户运行测试确认通过**

```bash
python -m pytest tests/test_data_store.py -v -k "splits"
```

- [ ] **Step 6: Commit**

```bash
git add data_store.py tests/test_data_store.py
git commit -m "[feature/stock-split-adjust][功能] 新增 splits 表及 CRUD 接口"
```

---

### Task 2: data_store.py — compute_split_factor 与 delete_ticker_data

**Files:**
- Modify: `data_store.py`
- Test: `tests/test_data_store.py`

- [ ] **Step 1: 写测试 — compute_split_factor**

```python
def test_compute_split_factor_before_split(tmp_db):
    """拆股前日期，因子 = split_from/split_to = 0.5"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-11-19")
    assert factor == 0.5


def test_compute_split_factor_on_split_date(tmp_db):
    """拆股当天，不需要调整（已是新价格），因子 = 1.0"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-11-20")
    assert factor == 1.0


def test_compute_split_factor_after_split(tmp_db):
    """拆股后日期，因子 = 1.0"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-12-01")
    assert factor == 1.0


def test_compute_split_factor_multiple_splits(tmp_db):
    """多次拆股累乘：1:2 再 1:3，最早期因子 = (1/2)*(1/3) = 1/6"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-06-01",
             "split_from": 1, "split_to": 2},
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 3},
        ])
        # 两次拆股之前
        factor = data_store.compute_split_factor("TQQQ", "2025-05-01")
    assert abs(factor - 1/6) < 1e-9


def test_compute_split_factor_between_splits(tmp_db):
    """两次拆股之间，只受后面那次影响：因子 = 1/3"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-06-01",
             "split_from": 1, "split_to": 2},
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 3},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-07-01")
    assert abs(factor - 1/3) < 1e-9


def test_compute_split_factor_no_splits(tmp_db):
    """无拆股记录，因子 = 1.0"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        factor = data_store.compute_split_factor("TQQQ", "2025-11-19")
    assert factor == 1.0
```

- [ ] **Step 2: 写测试 — delete_ticker_data**

```python
def test_delete_ticker_data(tmp_db, tmp_path):
    """清空指定 ticker 的 equity_bars + option_bars + sync_log"""
    rows_eq = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows_eq)
        data_store.write_sync_log("2025-01-06", "option_month", 100, "ok")
        data_store.write_sync_log("2025-01-06", "equity", 1, "ok")

        data_store.delete_ticker_data("TQQQ")

        # equity_bars 被清空
        eq = data_store.query_equity_bars("TQQQ", "1900-01-01", "2099-12-31")
        assert eq == []
        # sync_log 被清空
        assert not data_store.is_synced("2025-01-06", "option_month")
        assert not data_store.is_synced("2025-01-06", "equity")
```

- [ ] **Step 3: 实现 compute_split_factor**

在 `data_store.py` 新增：

```python
def compute_split_factor(ticker: str, date_str: str) -> float:
    """计算指定 ticker 在指定日期的前复权累积因子。

    前复权：把旧价格调到当前价格基准。
    因子 = ∏(split_from / split_to)，对所有 exec_date > date_str 的拆股事件累乘。
    拆股当天及之后的数据无需调整，因子 = 1.0。

    Args:
        ticker:   股票代码
        date_str: 日期 "YYYY-MM-DD"

    Returns:
        累积因子（浮点数），无拆股时返回 1.0
    """
    con = _connect()
    try:
        rows = con.execute(
            "SELECT split_from, split_to FROM splits "
            "WHERE ticker = ? AND exec_date > CAST(? AS DATE) "
            "ORDER BY exec_date",
            [ticker, date_str],
        ).fetchall()
    finally:
        con.close()
    factor = 1.0
    for split_from, split_to in rows:
        factor *= split_from / split_to
    return factor
```

- [ ] **Step 4: 实现 delete_ticker_data**

在 `data_store.py` 新增：

```python
def delete_ticker_data(ticker: str) -> None:
    """清空指定 ticker 的 equity_bars、option_bars 和全部 sync_log。

    用于拆股后的全量重拉前清理。
    """
    con = _connect()
    try:
        con.execute("DELETE FROM equity_bars WHERE ticker = ?", [ticker])
        con.execute(
            "DELETE FROM option_bars WHERE symbol LIKE ?",
            [f"O:{ticker}%"],
        )
        con.execute("DELETE FROM sync_log")
    finally:
        con.close()
    logger.info(f"[data_store] 已清空 {ticker} 的 equity_bars + option_bars + sync_log")
```

- [ ] **Step 5: 用户运行测试确认通过**

```bash
python -m pytest tests/test_data_store.py -v -k "split_factor or delete_ticker"
```

- [ ] **Step 6: Commit**

```bash
git add data_store.py tests/test_data_store.py
git commit -m "[feature/stock-split-adjust][功能] 新增前复权因子计算与 ticker 数据清空"
```

---

### Task 3: data_store.py — insert_option_bars_from_csv 支持拆股调整

**Files:**
- Modify: `data_store.py:111-169`
- Test: `tests/test_data_store.py`

- [ ] **Step 1: 写测试 — 期权入库时应用拆股因子**

```python
def test_insert_option_bars_with_split_factor(tmp_db, tmp_path):
    """1:2 拆股，拆股前日期：价格减半，volume 翻倍，symbol strike 减半"""
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "100", "open": "4.00",
             "close": "3.80", "high": "4.20", "low": "3.60",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        # 写入拆股记录
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        count = data_store.insert_option_bars_from_csv(
            f, "2025-01-06", tickers=["TQQQ"],
        )

    assert count == 1
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, high, low, close, volume FROM option_bars").fetchone()
    con.close()
    # symbol: O:TQQQ250131P00038500 → strike 38500 * 0.5 = 19250 → O:TQQQ250131P00019250
    assert row[0] == "O:TQQQ250131P00019250"
    assert abs(row[1] - 2.00) < 0.01  # open: 4.00 * 0.5
    assert abs(row[2] - 2.10) < 0.01  # high: 4.20 * 0.5
    assert abs(row[3] - 1.80) < 0.01  # low:  3.60 * 0.5
    assert abs(row[4] - 1.90) < 0.01  # close: 3.80 * 0.5
    assert row[5] == 200              # volume: 100 * 2


def test_insert_option_bars_no_split(tmp_db, tmp_path):
    """无拆股时，数据不变"""
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "100", "open": "4.00",
             "close": "3.80", "high": "4.20", "low": "3.60",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        count = data_store.insert_option_bars_from_csv(f, "2025-01-06", tickers=["TQQQ"])

    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, close, volume FROM option_bars").fetchone()
    con.close()
    assert row[0] == "O:TQQQ250131P00038500"
    assert abs(row[1] - 4.00) < 0.01
    assert abs(row[2] - 3.80) < 0.01
    assert row[3] == 100


def test_insert_option_bars_after_split_date(tmp_db, tmp_path):
    """拆股后日期，数据不变"""
    rows = [{"ticker": "O:TQQQ260131P00019250", "volume": "100", "open": "2.00",
             "close": "1.90", "high": "2.10", "low": "1.80",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-12-01.csv.gz"
    f.write_bytes(_make_csv_gz_ds(rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        count = data_store.insert_option_bars_from_csv(f, "2025-12-01", tickers=["TQQQ"])

    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, close, volume FROM option_bars").fetchone()
    con.close()
    assert row[0] == "O:TQQQ260131P00019250"
    assert abs(row[1] - 2.00) < 0.01
    assert row[3] == 100
```

- [ ] **Step 2: 修改 insert_option_bars_from_csv 实现**

修改 `data_store.py` 的 `insert_option_bars_from_csv` 函数。在查询前先计算该日期各 ticker 的累积拆股因子，然后在 SQL 中应用：

```python
def insert_option_bars_from_csv(
    csv_path: "Path",
    date_str: str,
    tickers: list[str] | None = None,
) -> int:
    """从 gzip CSV 文件批量写入 option_bars，自动应用拆股前复权调整。

    根据 splits 表计算该日期的累积因子：
    - 价格类字段（open/high/low/close）乘以 price_factor
    - volume 乘以 volume_factor（= 1/price_factor）
    - OCC symbol 中的 strike 按 price_factor 调整

    Args:
        csv_path:  本地 .csv.gz 文件路径
        date_str:  交易日期 "YYYY-MM-DD"
        tickers:   标的代码列表；None 则写入全部合约

    Returns:
        写入行数
    """
    if tickers:
        where_parts = []
        for t in tickers:
            if t.isalpha() and len(t) <= 10:
                where_parts.append(f"ticker LIKE 'O:{t.upper()}%'")
        where_sql = "WHERE " + " OR ".join(where_parts) if where_parts else ""
    else:
        where_sql = ""

    # 计算各 ticker 的拆股因子
    # 若无 tickers 过滤或无 splits 表，默认因子 1.0
    split_factors = {}
    if tickers:
        for t in tickers:
            split_factors[t.upper()] = compute_split_factor(t.upper(), date_str)

    # 判断是否需要拆股调整（所有因子都是 1.0 则跳过）
    needs_adjust = any(f != 1.0 for f in split_factors.values())

    if needs_adjust and tickers and len(tickers) == 1:
        # 单 ticker 优化：直接用标量因子
        tk = tickers[0].upper()
        pf = split_factors[tk]
        vf = 1.0 / pf  # volume 反向

        select_sql = f"""
            SELECT
                CAST('{date_str}' AS DATE),
                substr(ticker, 1, length(ticker) - 8) ||
                    lpad(CAST(CAST(ROUND(
                        CAST(substr(ticker, length(ticker) - 7) AS BIGINT) * {pf}
                    ) AS BIGINT) AS VARCHAR), 8, '0'),
                ROUND(CAST(open  AS DOUBLE) * {pf}, 2),
                ROUND(CAST(high  AS DOUBLE) * {pf}, 2),
                ROUND(CAST(low   AS DOUBLE) * {pf}, 2),
                ROUND(CAST(close AS DOUBLE) * {pf}, 2),
                CAST(ROUND(TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) * {vf}) AS BIGINT),
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """
    elif needs_adjust and tickers:
        # 多 ticker：用 CASE WHEN 按 ticker 前缀分别应用因子
        case_pf_parts = []
        case_vf_parts = []
        case_strike_parts = []
        for t, pf in split_factors.items():
            if pf != 1.0:
                vf = 1.0 / pf
                case_pf_parts.append(
                    f"WHEN ticker LIKE 'O:{t}%' THEN {{col}} * {pf}")
                case_vf_parts.append(
                    f"WHEN ticker LIKE 'O:{t}%' THEN CAST(ROUND({{col}} * {vf}) AS BIGINT)")
                case_strike_parts.append(
                    f"WHEN ticker LIKE 'O:{t}%' THEN "
                    f"substr(ticker, 1, length(ticker) - 8) || "
                    f"lpad(CAST(CAST(ROUND(CAST(substr(ticker, length(ticker) - 7) AS BIGINT) * {pf}) AS BIGINT) AS VARCHAR), 8, '0')")

        def _case(col, parts, default, round_digits=None):
            expr = f"CASE {' '.join(p.format(col=col) for p in parts)} ELSE {default} END"
            if round_digits is not None:
                expr = f"ROUND({expr}, {round_digits})"
            return expr

        symbol_case = f"CASE {' '.join(case_strike_parts)} ELSE ticker END"
        price_case = lambda col: _case(col, case_pf_parts, col, round_digits=2)
        vol_case = _case("TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT)",
                         case_vf_parts,
                         "TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT)")

        select_sql = f"""
            SELECT
                CAST('{date_str}' AS DATE),
                {symbol_case},
                {price_case("CAST(open AS DOUBLE)")},
                {price_case("CAST(high AS DOUBLE)")},
                {price_case("CAST(low AS DOUBLE)")},
                {price_case("CAST(close AS DOUBLE)")},
                {vol_case},
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """
    else:
        # 无需调整，原有逻辑
        select_sql = f"""
            SELECT
                CAST('{date_str}' AS DATE),
                ticker,
                CAST(open AS DOUBLE),
                CAST(high AS DOUBLE),
                CAST(low  AS DOUBLE),
                CAST(close AS DOUBLE),
                TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT),
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """

    sql = f"INSERT OR IGNORE INTO option_bars "                "(date, symbol, open, high, low, close, volume, transactions) "                f"{select_sql}"

    con = _connect()
    try:
        con.execute("BEGIN")
        con.execute(sql)
        written = con.execute(
            "SELECT COUNT(*) FROM option_bars WHERE date = CAST(? AS DATE)",
            [date_str],
        ).fetchone()[0]
        con.execute("COMMIT")
        logger.info(f"[data_store] {date_str}: {written:,} 行写入 option_bars")
        return written
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
```

- [ ] **Step 3: 用户运行测试确认通过**

```bash
python -m pytest tests/test_data_store.py -v -k "insert_option_bars"
```

- [ ] **Step 4: Commit**

```bash
git add data_store.py tests/test_data_store.py
git commit -m "[feature/stock-split-adjust][功能] insert_option_bars_from_csv 支持拆股前复权"
```

---

### Task 4: rest_downloader.py — download_splits + adjusted=true

**Files:**
- Modify: `rest_downloader.py`
- Test: `tests/test_rest_downloader.py`

- [ ] **Step 1: 写测试 — download_splits**

在 `tests/test_rest_downloader.py` 新增：

```python
SAMPLE_SPLITS = [
    {"id": "abc123", "ticker": "TQQQ", "adjustment_type": "forward_split",
     "execution_date": "2025-11-20", "split_from": 1, "split_to": 2,
     "historical_adjustment_factor": 0.5},
]


def test_download_splits(tmp_path):
    db_path = tmp_path / "test.duckdb"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": SAMPLE_SPLITS, "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get", return_value=mock_resp):
            new_splits = rest_downloader.download_splits("TQQQ", "test_key")

    assert len(new_splits) == 1
    assert new_splits[0]["exec_date"] == "2025-11-20"
    assert new_splits[0]["split_from"] == 1
    assert new_splits[0]["split_to"] == 2

    # 验证写入 DB
    import duckdb
    con = duckdb.connect(str(db_path))
    row = con.execute("SELECT * FROM splits WHERE ticker = 'TQQQ'").fetchone()
    con.close()
    assert row is not None


def test_download_splits_detects_new(tmp_path):
    """已有记录时，只返回新增的拆股事件"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        # 预先写入一条旧记录
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])

    # API 返回同一条 + 一条新的
    api_results = SAMPLE_SPLITS + [
        {"id": "def456", "ticker": "TQQQ", "adjustment_type": "forward_split",
         "execution_date": "2026-06-01", "split_from": 1, "split_to": 3,
         "historical_adjustment_factor": 0.333},
    ]
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": api_results, "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch.object(data_store, "DB_PATH", db_path):
        with patch("rest_downloader.requests.get", return_value=mock_resp):
            new_splits = rest_downloader.download_splits("TQQQ", "test_key")

    # 只有 2026-06-01 是新增的
    assert len(new_splits) == 1
    assert new_splits[0]["exec_date"] == "2026-06-01"


def test_download_splits_empty(tmp_path):
    """无拆股记录时返回空列表"""
    db_path = tmp_path / "test.duckdb"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": [], "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get", return_value=mock_resp):
            new_splits = rest_downloader.download_splits("TQQQ", "test_key")

    assert new_splits == []
```

- [ ] **Step 2: 写测试 — adjusted=true 参数验证**

```python
def test_download_equity_uses_adjusted_true(tmp_path):
    """确认 API 请求使用 adjusted=true"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   return_value=_mock_response(SAMPLE_BARS)) as mock_get:
            rest_downloader.download_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_key"
            )
    call_params = mock_get.call_args[1]["params"]
    assert call_params["adjusted"] == "true"
```

- [ ] **Step 3: 实现 download_splits**

在 `rest_downloader.py` 新增：

```python
def download_splits(ticker: str, api_key: str) -> list[dict]:
    """从 Massive API 拉取拆股历史，写入 splits 表，返回新增的拆股事件列表。

    Args:
        ticker:  股票代码
        api_key: Massive API Key

    Returns:
        新增拆股事件列表 [{ticker, exec_date, split_from, split_to}]，
        已存在的记录不会重复返回。
    """
    # 查询 DB 中已有的拆股记录
    existing = {r["exec_date"] for r in data_store.query_splits(ticker)}

    url = f"{BASE_URL}/stocks/v1/splits"
    params = {"ticker": ticker, "limit": 1000, "apiKey": api_key}

    resp = requests.get(url, params=params)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[rest] {ticker} splits API 错误: {e}")
        return []

    results = resp.json().get("results", [])
    if not results:
        return []

    all_rows = []
    for r in results:
        all_rows.append({
            "ticker": ticker,
            "exec_date": r["execution_date"],
            "split_from": r["split_from"],
            "split_to": r["split_to"],
        })

    # 写入 DB（INSERT OR IGNORE，幂等）
    data_store.upsert_splits(all_rows)

    # 返回新增的
    new_rows = [r for r in all_rows if r["exec_date"] not in existing]
    if new_rows:
        logger.info(f"[rest] {ticker} 发现 {len(new_rows)} 个新拆股事件: "
                     f"{[r['exec_date'] for r in new_rows]}")
    return new_rows
```

- [ ] **Step 4: 修改 adjusted 参数**

在 `rest_downloader.py` 的 `download_and_store_equity` 函数中，将第 35 行：

```python
# 旧
params = {"adjusted": "false", "sort": "asc",
          "limit": 50000, "apiKey": api_key}
# 新
params = {"adjusted": "true", "sort": "asc",
          "limit": 50000, "apiKey": api_key}
```

- [ ] **Step 5: 用户运行测试确认通过**

```bash
python -m pytest tests/test_rest_downloader.py -v
```

- [ ] **Step 6: Commit**

```bash
git add rest_downloader.py tests/test_rest_downloader.py
git commit -m "[feature/stock-split-adjust][功能] 新增 download_splits 接口，equity 改用 adjusted=true"
```

---

### Task 5: data_sync.py — ensure_synced 拆股检测与清空重拉

**Files:**
- Modify: `data_sync.py`
- Test: `tests/test_data_sync.py`

- [ ] **Step 1: 写测试 — 无新拆股时流程不变**

在 `tests/test_data_sync.py` 新增：

```python
def test_ensure_synced_no_new_splits(tmp_path):
    """无新拆股事件时，正常同步，不清空数据"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("data_sync.rest_downloader.download_splits", return_value=[]) as mock_splits, \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest, \
             patch("data_sync.data_store.delete_ticker_data") as mock_delete:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_splits.assert_called_once_with("TQQQ", "key")
    mock_delete.assert_not_called()
    mock_s3.assert_called_once()
    mock_rest.assert_called_once()
```

- [ ] **Step 2: 写测试 — 发现新拆股时清空重拉**

```python
def test_ensure_synced_new_split_triggers_purge(tmp_path):
    """发现新拆股事件时，清空该 ticker 数据并全量重拉"""
    db_path = tmp_path / "test.duckdb"
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    new_split = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
                  "split_from": 1, "split_to": 2}]

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        # 预置已有数据（本来不需要同步）
        data_store.upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])

        with patch("data_sync.rest_downloader.download_splits",
                   return_value=new_split), \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest, \
             patch("data_sync.data_store.delete_ticker_data") as mock_delete:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    # 应该清空数据
    mock_delete.assert_called_once_with("TQQQ")
    # 应该全量重拉（from_date 应是近 2 年前，而非增量）
    mock_s3.assert_called_once()
    s3_from = mock_s3.call_args.args[0]
    today = datetime.date.today()
    from_date = datetime.date.fromisoformat(s3_from)
    assert (today - from_date).days >= 365 * 2 - 1
```

- [ ] **Step 3: 修改 ensure_synced 实现**

```python
def ensure_synced(tickers: list[str], api_key: str) -> None:
    """确保 DuckDB 数据最新。空库同步近 2 年，有数据增量补到昨天。

    流程：
    1. 同步 splits 表，检测新拆股事件
    2. 有新拆股 → 清空该 ticker 数据（触发全量重拉）
    3. 同步 equity_bars（adjusted=true）
    4. 同步 option_bars（入库时按因子调整）
    """
    data_store.init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))
    full_sync_from = str(today - datetime.timedelta(days=365 * _FULL_SYNC_YEARS))

    # ── 1. 同步拆股数据，检测新事件 ──
    need_purge = set()
    if tickers and api_key:
        for ticker in tickers:
            new_splits = rest_downloader.download_splits(ticker, api_key)
            if new_splits:
                need_purge.add(ticker)

    # ── 2. 有新拆股 → 清空数据 ──
    for ticker in need_purge:
        logger.info(f"[sync] {ticker} 检测到新拆股，清空数据准备全量重拉")
        data_store.delete_ticker_data(ticker)

    # ── 3. 确定同步日期范围 ──
    latest = data_store.get_latest_synced_date("equity")
    if need_purge or not latest:
        from_date = full_sync_from
    else:
        from_date = str(datetime.date.fromisoformat(latest)
                        + datetime.timedelta(days=1))

    if from_date > to_date:
        logger.info("数据已是最新，无需同步")
        return

    logger.info(f"同步 {from_date} ~ {to_date}，标的: {tickers or '全部'}")
    s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)
    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)
```

- [ ] **Step 4: 用户运行测试确认通过**

```bash
python -m pytest tests/test_data_sync.py -v
```

- [ ] **Step 5: Commit**

```bash
git add data_sync.py tests/test_data_sync.py
git commit -m "[feature/stock-split-adjust][功能] ensure_synced 集成拆股检测与清空重拉"
```

---

### Task 6: 全流程验证与文档更新

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 用户运行全量测试**

```bash
python -m pytest tests/ -v
```

- [ ] **Step 2: 更新 CLAUDE.md Gotchas 段落**

在 CLAUDE.md 的 `## Gotchas` 部分追加：

```markdown
- **equity_bars 存储前复权价格**：`adjusted=true` 由 API 返回，DB 中不是原始价格。每次新拆股事件会触发全量重拉，获取最新复权基准。
- **option_bars 入库时自动复权**：根据 splits 表计算累积因子，调整价格/volume/OCC symbol 中的 strike。拆股后的数据因子为 1.0，不调整。
- **splits 表检测新事件**：`ensure_synced` 每次先拉 splits API，发现新记录时自动清空该 ticker 的所有数据并全量重拉。无新事件时 < 1 秒。
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "[feature/stock-split-adjust][文档] 更新 CLAUDE.md 拆股相关 Gotchas"
```
