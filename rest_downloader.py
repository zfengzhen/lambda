"""REST API 股票日K 下载器：按指定标的拉取日K并存入本地 DB。

使用 Massive /v2/aggs 端点，认证通过 apiKey 参数。
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


def download_and_store_equity(ticker: str, from_date: str, to_date: str,
                               api_key: str) -> int:
    """从 Massive REST API 下载指定股票日K并写入 equity_bars。

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

    resp = None
    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            wait = RETRY_DELAY * (attempt + 1)
            logger.warning(f"[rest] {ticker} 限流(429)，等待 {wait}s")
            time.sleep(wait)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"[rest] {ticker} HTTP 错误: {e}")
            return 0
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
        try:
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
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"[rest] {ticker} 跳过异常行: {e} — {r}")

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
            download_and_store_equity(ticker, from_date, to_date, api_key)
        except Exception as e:
            logger.error(f"[rest] {ticker} 同步失败: {e}")
