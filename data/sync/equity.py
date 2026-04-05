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
