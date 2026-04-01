# Entry Timing Optimizer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过历史期权日线数据，找到使卖 Put 总权利金最大的限价倍数 k（周二开盘挂单 = 周一期权收盘价 × k）。

**Architecture:** 两个新模块：`option_fetcher.py`（OCC symbol 构建 + 期权 OHLC 拉取 + 信号交易提取）和 `entry_optimizer.py`（数据富化 + k 扫描优化 + CLI 报告）。不修改现有 `run.py` / `strategy.py`；读取 `output/TQQQ.json`（先运行 `python run.py` 生成）作为输入。

**Tech Stack:** Python 3.11+, requests, numpy, unittest.mock, pytest; Massive API `/v2/aggs/ticker/{option_symbol}/range/1/day/{from}/{to}`

---

## 文件结构

```
lambda/
├── option_fetcher.py           (新建) OCC symbol 构建 + 期权 OHLC 拉取 + 信号交易提取
├── entry_optimizer.py          (新建) 数据富化 + k 扫描优化 + CLI 报告
└── tests/
    ├── test_option_fetcher.py  (新建) option_fetcher 单元测试
    └── test_entry_optimizer.py (新建) entry_optimizer 单元测试
```

---

## Task 1：OCC symbol 构建函数

**Files:**
- Create: `option_fetcher.py`（仅包含 Task 1 的函数）
- Create: `tests/test_option_fetcher.py`

- [ ] **Step 1：写失败测试**

新建 `tests/test_option_fetcher.py`：

```python
import pytest
from unittest.mock import patch, MagicMock
from option_fetcher import round_to_strike_increment, build_occ_symbol


class TestRoundToStrikeIncrement:
    def test_rounds_down(self):
        assert round_to_strike_increment(38.56) == 38.5

    def test_rounds_up(self):
        assert round_to_strike_increment(38.76) == 39.0

    def test_exact_value_unchanged(self):
        assert round_to_strike_increment(50.0) == 50.0

    def test_custom_increment(self):
        assert round_to_strike_increment(38.7, increment=1.0) == 39.0

    def test_midpoint_rounds_to_nearest(self):
        # 38.75 / 0.5 = 77.5 → rounds to 78 → 39.0
        assert round_to_strike_increment(38.75) == 39.0


class TestBuildOccSymbol:
    def test_basic_put(self):
        assert build_occ_symbol("TQQQ", "2025-01-31", 38.5) == "O:TQQQ250131P00038500"

    def test_large_strike(self):
        assert build_occ_symbol("TQQQ", "2025-04-18", 50.0) == "O:TQQQ250418P00050000"

    def test_call_type(self):
        assert build_occ_symbol("TQQQ", "2025-01-31", 50.0, contract_type="C") == "O:TQQQ250131C00050000"

    def test_fractional_strike_preserved(self):
        # 构建时不做圆整，原样转为整数千分位
        assert build_occ_symbol("TQQQ", "2025-01-31", 49.88) == "O:TQQQ250131P00049880"

    def test_strike_padded_to_8_digits(self):
        # 行权价 5.0 → 5000 → 00005000
        assert build_occ_symbol("TQQQ", "2025-01-31", 5.0) == "O:TQQQ250131P00005000"
```

- [ ] **Step 2：运行测试，确认失败**

```bash
cd /Users/fengzhen.zhang/alpha/lambda-worktrees/feature-weekly-strategy
source .venv/bin/activate
python -m pytest tests/test_option_fetcher.py -v
```

期望：`ModuleNotFoundError: No module named 'option_fetcher'`

- [ ] **Step 3：实现最小代码**

新建 `option_fetcher.py`：

```python
"""期权数据获取：OCC 合约 symbol 构建、日线 OHLC 拉取、信号交易提取"""
import datetime
import logging
import time
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.massive.com"
STRIKE_INCREMENT = 0.5  # TQQQ 期权行权价间距（美元）
MAX_RETRIES = 5
PAGE_DELAY = 15


def round_to_strike_increment(price: float, increment: float = STRIKE_INCREMENT) -> float:
    """将价格圆整到最近的行权价间距。

    Args:
        price: 目标行权价（如 38.56）
        increment: 行权价间距（默认 0.5）

    Returns:
        圆整后的行权价（如 38.5）
    """
    return round(round(price / increment) * increment, 10)


def build_occ_symbol(ticker: str, expiry_date: str, strike: float,
                     contract_type: str = "P") -> str:
    """构建 OCC 格式期权合约 symbol（含 Massive API 'O:' 前缀）。

    格式：O:{TICKER}{YYMMDD}{C|P}{8位行权价×1000，零填充}

    Args:
        ticker: 标的代码，如 "TQQQ"
        expiry_date: 到期日 "YYYY-MM-DD"
        strike: 行权价（美元），如 38.5
        contract_type: "C"（看涨）或 "P"（看跌），默认 "P"

    Returns:
        如 "O:TQQQ250131P00038500"
    """
    d = datetime.date.fromisoformat(expiry_date)
    date_str = d.strftime("%y%m%d")
    strike_int = int(round(strike * 1000))
    strike_str = f"{strike_int:08d}"
    return f"O:{ticker}{date_str}{contract_type.upper()}{strike_str}"
```

- [ ] **Step 4：运行测试，确认通过**

```bash
python -m pytest tests/test_option_fetcher.py::TestRoundToStrikeIncrement \
                 tests/test_option_fetcher.py::TestBuildOccSymbol -v
```

期望：10 个测试全部 PASS

- [ ] **Step 5：提交**

```bash
git add option_fetcher.py tests/test_option_fetcher.py
git commit -m "[feature/weekly-strategy][功能] 新增 option_fetcher：OCC symbol 构建"
```

---

## Task 2：期权日线 OHLC 拉取

**Files:**
- Modify: `option_fetcher.py`（追加 `fetch_option_bars`）
- Modify: `tests/test_option_fetcher.py`（追加测试类）

- [ ] **Step 1：写失败测试**

在 `tests/test_option_fetcher.py` 末尾追加：

```python
from option_fetcher import fetch_option_bars

# 模拟 API 响应（时间戳对应 2025-01-06 ~ 2025-01-07 UTC 零点）
OPTION_BAR_MON = {"t": 1736121600000, "o": 0.85, "h": 0.92, "l": 0.80, "c": 0.87}
OPTION_BAR_TUE = {"t": 1736208000000, "o": 0.86, "h": 0.95, "l": 0.82, "c": 0.90}


def _mock_resp(results, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"results": results, "resultsCount": len(results), "status": "OK"}
    resp.raise_for_status = MagicMock()
    return resp


class TestFetchOptionBars:
    @patch("option_fetcher.requests.get")
    def test_returns_parsed_bars(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON, OPTION_BAR_TUE])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-07", "key")
        assert len(bars) == 2
        assert bars[0]["high"] == 0.92
        assert bars[1]["high"] == 0.95

    @patch("option_fetcher.requests.get")
    def test_date_field_is_string(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        assert isinstance(bars[0]["date"], str)
        assert len(bars[0]["date"]) == 10  # YYYY-MM-DD

    @patch("option_fetcher.requests.get")
    def test_bar_has_required_fields(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        assert set(bars[0].keys()) == {"date", "open", "high", "low", "close"}

    @patch("option_fetcher.requests.get")
    def test_404_returns_empty(self, mock_get):
        resp = MagicMock()
        resp.status_code = 404
        mock_get.return_value = resp
        bars = fetch_option_bars("O:TQQQ250131P99999000", "2025-01-06", "2025-01-10", "key")
        assert bars == []

    @patch("option_fetcher.requests.get")
    def test_empty_results_returns_empty(self, mock_get):
        mock_get.return_value = _mock_resp([])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-10", "key")
        assert bars == []

    @patch("option_fetcher.time.sleep")
    @patch("option_fetcher.requests.get")
    def test_429_retries_then_succeeds(self, mock_get, mock_sleep):
        resp_429 = MagicMock()
        resp_429.status_code = 429
        mock_get.side_effect = [resp_429, _mock_resp([OPTION_BAR_MON])]
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        assert len(bars) == 1
        mock_sleep.assert_called()

    @patch("option_fetcher.requests.get")
    def test_url_contains_occ_symbol(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON])
        fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        call_url = mock_get.call_args[0][0]
        assert "O:TQQQ250131P00038500" in call_url
        assert "/v2/aggs/ticker/" in call_url
        assert "/range/1/day/" in call_url
```

- [ ] **Step 2：运行测试，确认失败**

```bash
python -m pytest tests/test_option_fetcher.py::TestFetchOptionBars -v
```

期望：`ImportError: cannot import name 'fetch_option_bars'`

- [ ] **Step 3：实现最小代码**

在 `option_fetcher.py` 末尾追加：

```python
def fetch_option_bars(symbol: str, from_date: str, to_date: str,
                      api_key: str) -> list[dict]:
    """拉取期权合约日线 OHLC 数据。

    使用与 fetch_client.py 相同的 /v2/aggs 端点，合约 symbol 为 OCC 格式（O: 前缀）。
    返回空列表表示无数据或合约不存在，调用方应跳过此交易。

    Args:
        symbol: OCC 合约 symbol，如 "O:TQQQ250131P00038500"
        from_date: 起始日期 "YYYY-MM-DD"
        to_date:   结束日期 "YYYY-MM-DD"
        api_key:   Massive API Key

    Returns:
        每条包含 {date, open, high, low, close} 的列表，按日期升序
    """
    from datetime import timezone

    url = (f"{BASE_URL}/v2/aggs/ticker/{symbol}"
           f"/range/1/day/{from_date}/{to_date}")
    params = {"adjusted": "false", "sort": "asc", "limit": 50000, "apiKey": api_key}

    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            wait = PAGE_DELAY * (attempt + 1)
            logger.warning(f"[{symbol}] 限流(429)，等待 {wait}s 后重试")
            time.sleep(wait)
            continue
        if resp.status_code == 404:
            logger.warning(f"[{symbol}] 合约不存在(404)")
            return []
        resp.raise_for_status()
        break
    else:
        logger.error(f"[{symbol}] 重试 {MAX_RETRIES} 次后放弃")
        return []

    raw = resp.json().get("results", [])
    if not raw:
        logger.info(f"[{symbol}] 无数据")
        return []

    bars = []
    for r in raw:
        dt = (datetime.datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc)
              .strftime("%Y-%m-%d"))
        bars.append({"date": dt, "open": r["o"], "high": r["h"],
                     "low": r["l"], "close": r["c"]})
    return bars
```

- [ ] **Step 4：运行测试，确认通过**

```bash
python -m pytest tests/test_option_fetcher.py::TestFetchOptionBars -v
```

期望：7 个测试全部 PASS

- [ ] **Step 5：提交**

```bash
git add option_fetcher.py tests/test_option_fetcher.py
git commit -m "[feature/weekly-strategy][功能] 新增 option_fetcher：期权日线 OHLC 拉取"
```

---

## Task 3：信号交易提取

**Files:**
- Modify: `option_fetcher.py`（追加 `get_signal_trades`）
- Modify: `tests/test_option_fetcher.py`（追加测试类）

- [ ] **Step 1：写失败测试**

在 `tests/test_option_fetcher.py` 末尾追加：

```python
from option_fetcher import get_signal_trades

SAMPLE_WEEKS = [
    {
        "date": "2025-01-06", "tier": "A", "close": 42.84,
        "strike": 38.56, "otm": 10, "expiry_date": "2025-01-31",
        "pending": False, "safe_expiry": True,
    },
    {
        "date": "2025-01-13", "tier": "C", "close": 40.0,
        "strike": 30.0, "otm": 25, "expiry_date": "2025-02-07",
        "pending": False, "safe_expiry": True,
    },
    {
        "date": "2025-01-20", "tier": "B1", "close": 38.0,
        "strike": 32.3, "otm": 15, "expiry_date": "2025-02-14",
        "pending": True, "safe_expiry": None,
    },
    {
        "date": "2024-12-30", "tier": "B3", "close": 55.0,
        "strike": 46.75, "otm": 15, "expiry_date": "2025-01-24",
        "pending": False, "safe_expiry": True,
    },
]


class TestGetSignalTrades:
    def test_filters_out_c_tier(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        tiers = [t["layer"] for t in trades]
        assert "C" not in tiers

    def test_filters_out_pending(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        assert all(not t.get("pending") for t in trades)

    def test_returns_two_valid_trades(self):
        # A (non-C, non-pending) + B3 (non-C, non-pending) = 2
        trades = get_signal_trades(SAMPLE_WEEKS)
        assert len(trades) == 2

    def test_rounds_strike_to_half_dollar(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        a_trade = next(t for t in trades if t["layer"] == "A")
        # 38.56 → 38.5
        assert a_trade["strike"] == 38.5

    def test_otm_pct_correct(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        a_trade = next(t for t in trades if t["layer"] == "A")
        assert a_trade["otm_pct"] == pytest.approx(0.10)

    def test_sorted_by_date_ascending(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        dates = [t["week_start"] for t in trades]
        assert dates == sorted(dates)

    def test_output_fields(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        for t in trades:
            assert set(t.keys()) >= {"week_start", "layer", "mon_close",
                                     "strike", "expiry", "otm_pct"}
```

- [ ] **Step 2：运行测试，确认失败**

```bash
python -m pytest tests/test_option_fetcher.py::TestGetSignalTrades -v
```

期望：`ImportError: cannot import name 'get_signal_trades'`

- [ ] **Step 3：实现最小代码**

在 `option_fetcher.py` 末尾追加：

```python
def get_signal_trades(weeks: list[dict]) -> list[dict]:
    """从 backtest_weeks() 输出中提取有效信号交易（非 C 层、非 pending）。

    行权价圆整到最近 $0.50 行权价间距，otm 整数字段转为小数。

    Args:
        weeks: strategy.backtest_weeks() 的返回值

    Returns:
        信号交易列表，每条包含:
            week_start, layer, mon_close, strike, expiry, otm_pct
        按 week_start 升序排列。
    """
    trades = []
    for w in weeks:
        if w.get("pending"):
            continue
        if w.get("tier") == "C":
            continue
        trades.append({
            "week_start": w["date"],
            "layer": w["tier"],
            "mon_close": w["close"],
            "strike": round_to_strike_increment(w["strike"]),
            "expiry": w["expiry_date"],
            "otm_pct": w["otm"] / 100.0,
        })
    trades.sort(key=lambda t: t["week_start"])
    return trades
```

- [ ] **Step 4：运行测试，确认通过**

```bash
python -m pytest tests/test_option_fetcher.py -v
```

期望：所有 option_fetcher 测试 PASS（含 Task 1、2、3）

- [ ] **Step 5：提交**

```bash
git add option_fetcher.py tests/test_option_fetcher.py
git commit -m "[feature/weekly-strategy][功能] 新增 option_fetcher：信号交易提取"
```

---

## Task 4：期权数据富化

**Files:**
- Create: `entry_optimizer.py`（仅 `enrich_with_option_data`）
- Create: `tests/test_entry_optimizer.py`

- [ ] **Step 1：写失败测试**

新建 `tests/test_entry_optimizer.py`：

```python
import pytest
from unittest.mock import patch
from entry_optimizer import enrich_with_option_data

SAMPLE_TRADE = {
    "week_start": "2025-01-06",   # Monday
    "layer": "A",
    "mon_close": 42.84,
    "strike": 38.5,
    "expiry": "2025-01-31",
    "otm_pct": 0.10,
}

# date 字段已由 fetch_option_bars 解析好
MOCK_BARS = [
    {"date": "2025-01-06", "open": 0.85, "high": 0.92, "low": 0.80, "close": 0.87},  # Mon
    {"date": "2025-01-07", "open": 0.86, "high": 0.95, "low": 0.82, "close": 0.90},  # Tue
    {"date": "2025-01-08", "open": 0.88, "high": 0.93, "low": 0.83, "close": 0.89},  # Wed
    {"date": "2025-01-09", "open": 0.87, "high": 0.91, "low": 0.81, "close": 0.88},  # Thu
    {"date": "2025-01-10", "open": 0.85, "high": 0.89, "low": 0.79, "close": 0.85},  # Fri
]


class TestEnrichWithOptionData:
    def test_mon_close_option_extracted(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["mon_close_option"] == pytest.approx(0.87)

    def test_day_highs_extracted(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["tue_high"] == pytest.approx(0.95)
        assert t["wed_high"] == pytest.approx(0.93)
        assert t["thu_high"] == pytest.approx(0.91)
        assert t["fri_high"] == pytest.approx(0.89)

    def test_week_high_is_max_of_tue_to_fri(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["week_high"] == pytest.approx(0.95)  # max(0.95, 0.93, 0.91, 0.89)

    def test_data_complete_true_when_all_days_present(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["data_complete"] is True

    def test_data_complete_false_when_monday_missing(self):
        bars_no_mon = [b for b in MOCK_BARS if b["date"] != "2025-01-06"]
        with patch("entry_optimizer.fetch_option_bars", return_value=bars_no_mon):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["mon_close_option"] is None
        assert t["data_complete"] is False

    def test_data_complete_false_when_empty_bars(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=[]):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["data_complete"] is False

    def test_tolerates_one_missing_weekday(self):
        # 只缺周五，仍有 3 天数据（周二/三/四），应视为 complete
        bars_no_fri = [b for b in MOCK_BARS if b["date"] != "2025-01-10"]
        with patch("entry_optimizer.fetch_option_bars", return_value=bars_no_fri):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["fri_high"] is None
        assert t["data_complete"] is True  # 仍有 3 天 >= 3

    def test_option_symbol_is_correct_occ(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        # TQQQ put 行权价 38.5 到期 2025-01-31
        assert result[0]["option_symbol"] == "O:TQQQ250131P00038500"

    def test_preserves_original_trade_fields(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["week_start"] == "2025-01-06"
        assert t["layer"] == "A"
        assert t["strike"] == pytest.approx(38.5)
```

- [ ] **Step 2：运行测试，确认失败**

```bash
python -m pytest tests/test_entry_optimizer.py -v
```

期望：`ModuleNotFoundError: No module named 'entry_optimizer'`

- [ ] **Step 3：实现最小代码**

新建 `entry_optimizer.py`：

```python
"""入场限价优化器：通过历史期权数据找到最优限价倍数 k

用法:
    python entry_optimizer.py                       # 默认 TQQQ，读取 output/TQQQ.json
    python entry_optimizer.py --ticker TQQQ
    python entry_optimizer.py --output report.txt   # 保存报告到文件

前置条件:
    1. 已运行 python run.py 生成 output/TQQQ.json
    2. 设置 MASSIVE_API_KEY 环境变量
"""
import datetime
import json
import logging
import os

import numpy as np

from option_fetcher import build_occ_symbol, fetch_option_bars, get_signal_trades

logger = logging.getLogger(__name__)


def enrich_with_option_data(trades: list[dict], api_key: str,
                             ticker: str = "TQQQ") -> list[dict]:
    """为每笔信号交易拉取信号周（周一至周五）期权日线数据。

    通过 Massive API 获取期权 OHLC，提取:
    - mon_close_option: 周一收盘价（限价单参考价）
    - tue_high/wed_high/thu_high/fri_high: 周二至周五各日最高价
    - week_high: max(周二至周五最高价)
    - option_symbol: 合约 OCC symbol
    - data_complete: True 当且仅当 mon_close_option 非空
                     且至少 3 天有高价数据

    Args:
        trades: get_signal_trades() 输出
        api_key: Massive API Key
        ticker: 标的代码（默认 TQQQ）

    Returns:
        trades 的副本，每条追加上述字段。
    """
    enriched = []
    for trade in trades:
        symbol = build_occ_symbol(ticker, trade["expiry"], trade["strike"], "P")

        # 信号周：周一（week_start）到周五（+4 天）
        mon = datetime.date.fromisoformat(trade["week_start"])
        fri = mon + datetime.timedelta(days=4)
        bars = fetch_option_bars(symbol, str(mon), str(fri), api_key)
        bar_by_date = {b["date"]: b for b in bars}

        def _close(offset: int):
            d = str(mon + datetime.timedelta(days=offset))
            b = bar_by_date.get(d)
            return b["close"] if b else None

        def _high(offset: int):
            d = str(mon + datetime.timedelta(days=offset))
            b = bar_by_date.get(d)
            return b["high"] if b else None

        mon_close_option = _close(0)
        tue_high = _high(1)
        wed_high = _high(2)
        thu_high = _high(3)
        fri_high = _high(4)

        week_high_vals = [v for v in [tue_high, wed_high, thu_high, fri_high]
                          if v is not None]
        week_high = max(week_high_vals) if week_high_vals else None

        data_complete = (mon_close_option is not None
                         and len(week_high_vals) >= 3)

        if not data_complete:
            logger.warning(
                f"[{symbol}] 数据不完整: "
                f"mon_close={mon_close_option}, 有效天数={len(week_high_vals)}"
            )

        enriched.append({
            **trade,
            "option_symbol": symbol,
            "mon_close_option": mon_close_option,
            "tue_high": tue_high,
            "wed_high": wed_high,
            "thu_high": thu_high,
            "fri_high": fri_high,
            "week_high": week_high,
            "data_complete": data_complete,
        })
        if mon_close_option and week_high:
            logger.info(f"[{symbol}] mon_close={mon_close_option:.4f}, week_high={week_high:.4f}")
        else:
            logger.info(f"[{symbol}] 数据不完整")

    return enriched
```

- [ ] **Step 4：运行测试，确认通过**

```bash
python -m pytest tests/test_entry_optimizer.py::TestEnrichWithOptionData -v
```

期望：8 个测试全部 PASS

- [ ] **Step 5：提交**

```bash
git add entry_optimizer.py tests/test_entry_optimizer.py
git commit -m "[feature/weekly-strategy][功能] 新增 entry_optimizer：期权数据富化"
```

---

## Task 5：k 扫描优化

**Files:**
- Modify: `entry_optimizer.py`（追加 `sweep_k`、`find_optimal_k`）
- Modify: `tests/test_entry_optimizer.py`（追加测试类）

- [ ] **Step 1：写失败测试**

在 `tests/test_entry_optimizer.py` 末尾追加：

```python
from entry_optimizer import sweep_k, find_optimal_k

# 参考价 0.87，整周最高 0.95
COMPLETE_TRADE = {
    "week_start": "2025-01-06", "layer": "A",
    "mon_close": 42.84, "strike": 38.5,
    "expiry": "2025-01-31", "otm_pct": 0.10,
    "option_symbol": "O:TQQQ250131P00038500",
    "mon_close_option": 0.87, "week_high": 0.95,
    "data_complete": True,
}

INCOMPLETE_TRADE = {**COMPLETE_TRADE, "data_complete": False}


class TestSweepK:
    def test_all_fill_at_low_k(self):
        # limit = 0.87 × 0.5 = 0.435 < 0.95 → 成交
        results = sweep_k([COMPLETE_TRADE], k_min=0.5, k_max=0.5, k_step=0.1)
        assert results[0]["fill_count"] == 1
        assert results[0]["total_premium"] == pytest.approx(0.87 * 0.5, rel=1e-4)

    def test_none_fill_at_high_k(self):
        # limit = 0.87 × 2.0 = 1.74 > 0.95 → 不成交
        results = sweep_k([COMPLETE_TRADE], k_min=2.0, k_max=2.0, k_step=0.1)
        assert results[0]["fill_count"] == 0
        assert results[0]["total_premium"] == pytest.approx(0.0)

    def test_fill_rate_at_market_price(self):
        # limit = 0.87 × 1.0 = 0.87 ≤ 0.95 → 成交，fill_rate = 100%
        results = sweep_k([COMPLETE_TRADE], k_min=1.0, k_max=1.0, k_step=0.1)
        assert results[0]["fill_rate"] == pytest.approx(1.0)

    def test_skips_incomplete_trades(self):
        results = sweep_k([INCOMPLETE_TRADE, COMPLETE_TRADE],
                          k_min=1.0, k_max=1.0, k_step=0.1)
        # 只有 COMPLETE_TRADE 计入，fill_rate = 1/1
        assert results[0]["fill_rate"] == pytest.approx(1.0)
        assert results[0]["fill_count"] == 1

    def test_premium_at_boundary_k(self):
        # limit = 0.87 × (0.95/0.87) ≈ 0.95 = week_high → 刚好成交
        k_boundary = round(0.95 / 0.87, 10)
        results = sweep_k([COMPLETE_TRADE], k_min=k_boundary,
                          k_max=k_boundary, k_step=0.1)
        assert results[0]["fill_count"] == 1

    def test_returns_empty_for_all_incomplete(self):
        results = sweep_k([INCOMPLETE_TRADE])
        assert results == []

    def test_result_structure(self):
        results = sweep_k([COMPLETE_TRADE], k_min=1.0, k_max=1.0, k_step=0.1)
        r = results[0]
        assert set(r.keys()) >= {"k", "total_premium", "fill_count", "fill_rate"}


class TestFindOptimalK:
    def test_selects_max_total_premium(self):
        sweep = [
            {"k": 1.0, "total_premium": 0.87, "fill_count": 1, "fill_rate": 1.0},
            {"k": 1.1, "total_premium": 0.96, "fill_count": 1, "fill_rate": 1.0},
            {"k": 1.5, "total_premium": 0.50, "fill_count": 1, "fill_rate": 0.5},
        ]
        best = find_optimal_k(sweep)
        assert best["k"] == pytest.approx(1.1)

    def test_raises_on_empty(self):
        with pytest.raises(ValueError, match="sweep_results 为空"):
            find_optimal_k([])
```

- [ ] **Step 2：运行测试，确认失败**

```bash
python -m pytest tests/test_entry_optimizer.py::TestSweepK \
                 tests/test_entry_optimizer.py::TestFindOptimalK -v
```

期望：`ImportError: cannot import name 'sweep_k'`

- [ ] **Step 3：实现最小代码**

在 `entry_optimizer.py` 末尾追加：

```python
def sweep_k(trades: list[dict],
            k_min: float = 0.5,
            k_max: float = 3.0,
            k_step: float = 0.05) -> list[dict]:
    """扫描不同 k 值，计算每个 k 下的总权利金和成交率。

    限价单 = mon_close_option × k；成交条件：week_high >= 限价单。
    仅使用 data_complete=True 的交易。

    Args:
        trades:          enrich_with_option_data() 输出
        k_min, k_max:    扫描范围（含端点）
        k_step:          步长

    Returns:
        每个 k 对应 {k, total_premium, fill_count, fill_rate} 的列表
    """
    valid = [t for t in trades if t.get("data_complete")]
    if not valid:
        return []

    results = []
    for k in np.arange(k_min, k_max + k_step / 2, k_step):
        k = round(float(k), 10)
        total_premium = 0.0
        fill_count = 0
        for trade in valid:
            limit = trade["mon_close_option"] * k
            if trade["week_high"] >= limit:
                total_premium += limit
                fill_count += 1
        results.append({
            "k": round(k, 4),
            "total_premium": round(total_premium, 6),
            "fill_count": fill_count,
            "fill_rate": round(fill_count / len(valid), 4),
        })
    return results


def find_optimal_k(sweep_results: list[dict]) -> dict:
    """从扫描结果中找出 total_premium 最大的 k 对应的结果行。

    Args:
        sweep_results: sweep_k() 输出

    Returns:
        最优 {k, total_premium, fill_count, fill_rate}

    Raises:
        ValueError: sweep_results 为空时
    """
    if not sweep_results:
        raise ValueError("sweep_results 为空")
    return max(sweep_results, key=lambda r: r["total_premium"])
```

- [ ] **Step 4：运行测试，确认通过**

```bash
python -m pytest tests/test_entry_optimizer.py -v
```

期望：所有 entry_optimizer 测试 PASS

- [ ] **Step 5：提交**

```bash
git add entry_optimizer.py tests/test_entry_optimizer.py
git commit -m "[feature/weekly-strategy][功能] 新增 entry_optimizer：k 扫描优化"
```

---

## Task 6：报告生成与 CLI 主程序

**Files:**
- Modify: `entry_optimizer.py`（追加 `print_report`、`main()`）

（此任务为输出与 wiring，逻辑已在前序任务中测试；这里做最小集成验证。）

- [ ] **Step 1：实现 `print_report` 和 `main()`**

在 `entry_optimizer.py` 末尾追加：

```python
def print_report(trades: list[dict], sweep_results: list[dict],
                 k_star: dict) -> str:
    """生成人类可读的优化报告。

    Args:
        trades:        enrich_with_option_data() 输出
        sweep_results: sweep_k() 输出（全量）
        k_star:        find_optimal_k() 输出

    Returns:
        报告字符串（可直接 print 或写入文件）
    """
    valid = [t for t in trades if t.get("data_complete")]
    n_valid = len(valid)
    n_skip = len(trades) - n_valid

    baseline = next((r for r in sweep_results if abs(r["k"] - 1.0) < 0.001), None)

    # 分层最优 k
    layer_lines = []
    for layer in ["A", "B1", "B2", "B3", "B4"]:
        layer_trades = [t for t in valid if t["layer"] == layer]
        if not layer_trades:
            continue
        layer_sweep = sweep_k(layer_trades)
        lk = find_optimal_k(layer_sweep) if layer_sweep else None
        if lk:
            layer_lines.append(
                f"  {layer:<4} ({len(layer_trades)} 笔)  "
                f"k={lk['k']:.2f}  "
                f"成交率={lk['fill_rate']*100:.0f}%  "
                f"总权利金={lk['total_premium']:.4f}"
            )

    lines = [
        "=" * 60,
        "Lambda 策略 · 入场限价优化报告",
        "=" * 60,
        f"有效交易: {n_valid} 笔  （跳过 {n_skip} 笔数据不完整）",
        "",
        "── 全局最优 ──",
        f"k_star        : {k_star['k']:.2f}",
        f"成交率        : {k_star['fill_rate']*100:.1f}%"
        f"  ({k_star['fill_count']}/{n_valid} 笔)",
        f"总权利金      : {k_star['total_premium']:.4f}",
    ]

    if baseline:
        gain = k_star["total_premium"] - baseline["total_premium"]
        pct = gain / baseline["total_premium"] * 100 if baseline["total_premium"] else 0
        miss = baseline["fill_count"] - k_star["fill_count"]
        lines += [
            "",
            "── 对比市价单 (k=1.0) ──",
            f"市价总权利金  : {baseline['total_premium']:.4f}",
            f"额外收益      : +{gain:.4f}  (+{pct:.1f}%)",
            f"损失成交笔数  : {miss} 笔",
        ]

    if layer_lines:
        lines += ["", "── 分层最优 k ──"] + layer_lines

    lines += [
        "",
        "── 实盘操作规则 ──",
        f"周一收盘查期权收盘价 P，",
        f"周二开盘挂限价卖单 = P × {k_star['k']:.2f}",
        "GTC 持续至周五收盘；整周未成交则放弃本周。",
        "=" * 60,
    ]

    return "\n".join(lines)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Lambda 策略入场限价优化")
    parser.add_argument("--ticker", default="TQQQ", help="标的代码（默认 TQQQ）")
    parser.add_argument("--output", default=None, help="报告输出路径（默认打印到控制台）")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        print("错误：未设置 MASSIVE_API_KEY 环境变量")
        return 1

    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "output", f"{args.ticker.upper()}.json")
    if not os.path.exists(json_path):
        print(f"错误：{json_path} 不存在，请先运行: python run.py {args.ticker}")
        return 1

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    trades = get_signal_trades(data["weeks"])
    print(f"信号交易: {len(trades)} 笔")

    enriched = enrich_with_option_data(trades, api_key, ticker=args.ticker.upper())
    valid_count = sum(1 for t in enriched if t["data_complete"])
    print(f"数据完整: {valid_count} / {len(enriched)} 笔")

    results = sweep_k(enriched)
    if not results:
        print("无有效数据，无法优化")
        return 1

    k_star = find_optimal_k(results)
    report = print_report(enriched, results, k_star)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"报告已保存: {args.output}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
```

- [ ] **Step 2：运行所有测试，确认通过**

```bash
python -m pytest tests/ -v
```

期望：所有测试 PASS，无新失败

- [ ] **Step 3：冒烟测试（无 API Key 的错误路径）**

```bash
unset MASSIVE_API_KEY
python entry_optimizer.py
```

期望输出：`错误：未设置 MASSIVE_API_KEY 环境变量`（退出码 1）

- [ ] **Step 4：检查 import 无循环依赖**

```bash
python -c "import option_fetcher; import entry_optimizer; print('OK')"
```

期望：`OK`

- [ ] **Step 5：提交**

```bash
git add entry_optimizer.py
git commit -m "[feature/weekly-strategy][功能] 新增 entry_optimizer：报告生成与 CLI 主程序"
```

---

## 运行方式

全部任务完成后，按以下顺序运行：

```bash
# 1. 确保 output/TQQQ.json 存在（含完整回测数据）
python run.py TQQQ --full

# 2. 运行优化分析（需设置 API Key）
export MASSIVE_API_KEY=your_key
python entry_optimizer.py

# 3. 保存报告
python entry_optimizer.py --output output/entry_timing_report.txt
```

预期输出示例：
```
信号交易: 48 笔
数据完整: 45 / 48 笔
==================================================
Lambda 策略 · 入场限价优化报告
==================================================
有效交易: 45 笔  （跳过 3 笔数据不完整）

── 全局最优 ──
k_star        : 1.XX
成交率        : XX.X%  (XX/45 笔)
总权利金      : X.XXXX
...
```
