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
