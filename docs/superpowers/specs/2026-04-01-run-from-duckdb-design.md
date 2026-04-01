# run.py 接入 DuckDB 设计文档

**日期：** 2026-04-01
**分支：** feature/run-from-duckdb
**目标：** run.py 从 DuckDB 读取股票日K，并在运行前自动同步期权和股票数据到最新。

---

## 背景

当前 `run.py` 通过 Massive REST API 拉取股票日K，以 JSON 文件做增量缓存。`data_sync.py` 独立维护 DuckDB（`option_bars` + `equity_bars`），两套数据互不关联。

本次改造：
1. 统一数据源为 DuckDB
2. run.py 运行前自动触发增量同步（空库则全量）
3. 简化 CLI 参数，去掉年限和模式选项

---

## 数据流

```
run.py
  └─ data_sync.ensure_synced(tickers, api_key)
       ├─ s3_downloader.sync_options(from, to, tickers)   # 期权，按月，有缓存跳过下载
       └─ rest_downloader.sync_equity(tickers, from, to)  # 股票，REST upsert
  └─ data_store.query_equity_bars(ticker, "1900-01-01", today) → DataFrame
  └─ 指标计算 → 策略计算 → JSON / HTML / PNG
```

---

## 模块改动

### 1. `data_sync.py` — 新增 `ensure_synced()`，简化 CLI

**新增函数：**

```python
def ensure_synced(tickers: list[str], api_key: str) -> None:
    """确保 DuckDB 数据最新。空库同步近 2 年，有数据增量补到昨天。"""
    data_store.init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))

    latest = data_store.get_latest_synced_date("equity")
    if latest:
        from_date = str(datetime.date.fromisoformat(latest)
                        + datetime.timedelta(days=1))
    else:
        from_date = str(today - datetime.timedelta(days=365 * 2))

    if from_date > to_date:
        logger.info("数据已是最新，无需同步")
        return

    logger.info(f"同步 {from_date} ~ {to_date}，标的: {tickers or '全部'}")
    s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)
    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)
```

**CLI 简化：**
- 保留 `--tickers`
- 移除 `--years`、`--incremental`、`--month`
- 移除 `full_sync()`、`incremental_sync()`、`month_sync()`（逻辑合并入 `ensure_synced()`）
- CLI 直接调用 `ensure_synced(tickers, api_key)`

```bash
python data_sync.py                      # 同步所有标的
python data_sync.py --tickers TQQQ QQQ   # 同步指定标的
```

---

### 2. `run.py` — 改 `fetch_daily_bars()`，简化 CLI

**`fetch_daily_bars()` 新逻辑：**

```python
def fetch_daily_bars(ticker: str, api_key: str) -> pd.DataFrame | None:
    from data_sync import ensure_synced
    from indicators import add_ma, add_macd, add_dynamic_pivot

    ensure_synced([ticker], api_key)

    rows = data_store.query_equity_bars(ticker, "1900-01-01",
                                        datetime.now().strftime("%Y-%m-%d"))
    if not rows:
        logger.warning(f"[{ticker}] DuckDB 无数据")
        return None

    df = pd.DataFrame(rows)
    df = add_ma(df)
    df = add_macd(df)
    df = add_dynamic_pivot(df)
    return df
```

**移除内容：**
- `--years`、`--full` CLI 参数
- `load_existing_data()` 调用（不再需要 JSON 做增量基准）
- JSON 输出中的 `daily_bars` 字段（`compute_strategy()` 不再返回 `daily_bars`）

**保留内容：**
- `save_json()`（JSON 用于 HTML 生成）
- `embed_to_html()`、`capture_screenshot()`

**CLI：**
```bash
python run.py              # 默认 TQQQ
python run.py TQQQ QQQ     # 多标的
```

---

### 3. `data_store.py` — 无需改动

`query_equity_bars(ticker, from_date, to_date)` 已支持全量查询，传 `"1900-01-01"` 即可。

---

## 行为说明

| 场景 | 行为 |
|------|------|
| 首次运行，DB 空 | 自动同步近 2 年数据（较慢，约 20 分钟） |
| DB 有数据，已最新 | ensure_synced < 1 秒返回，直接读 DB |
| DB 有数据，差几天 | 增量补齐，秒级完成 |
| 旧数据（超 2 年） | 保留不删 |
| 无 `MASSIVE_API_KEY` | 跳过股票同步，期权仍可同步 |

---

## 测试策略

- `test_data_sync.py`：更新 `test_full_sync_calls_both_downloaders` 签名；新增 `test_ensure_synced_*` 覆盖空库/增量/已最新三种情况
- `test_run.py`：mock `ensure_synced` 和 `query_equity_bars`，验证 `fetch_daily_bars` 返回正确 DataFrame
- 移除已失效的 `--years`、`--incremental`、`--full` 相关测试

---

## 不在本次范围内

- run.py 读取 `option_bars` 数据（后续功能）
- `query_equity_bars` 性能优化（当前数据量足够）
- 数据清理 / 过期策略
