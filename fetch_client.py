"""Massive API 客户端 — 拉取股票多周期K线数据"""
import json
import logging
import time
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.massive.com"
MAX_RETRIES = 5
PAGE_DELAY = 15  # 翻页间隔秒数，避免触发限流

# 周期名称 → API timespan 映射
TIMEFRAME_MAP = {
    "hourly": "hour",
    "daily": "day",
    "weekly": "week",
    "monthly": "month",
    "quarterly": "quarter",
    "yearly": "year",
}


def fetch_bars(ticker: str, timeframe: str, from_date: str, to_date: str, api_key: str) -> list[dict]:
    """
    拉取指定 ticker 在 [from_date, to_date] 范围内的K线数据。
    支持多周期：hourly/daily/weekly/monthly/quarterly/yearly。
    自动处理分页（next_url）。

    Args:
        ticker: 股票代码，如 "TQQQ"
        timeframe: 周期名称，如 "hourly"、"daily"、"weekly" 等
        from_date: 起始日期 "YYYY-MM-DD"
        to_date: 结束日期 "YYYY-MM-DD"
        api_key: Massive API Key

    Returns:
        K线数据列表，每条包含 o/h/l/c/v/vw/n/t 字段

    Raises:
        ValueError: timeframe 不在 TIMEFRAME_MAP 中时抛出
    """
    if timeframe not in TIMEFRAME_MAP:
        raise ValueError(f"不支持的周期 '{timeframe}'，可选: {', '.join(TIMEFRAME_MAP.keys())}")

    api_timespan = TIMEFRAME_MAP[timeframe]
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/{api_timespan}/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }

    all_bars = []
    page = 0

    while url:
        page += 1
        # 记录请求详情（隐藏 apiKey）
        safe_params = {k: v for k, v in params.items() if k != "apiKey"} if params else {}
        logger.info(f"[第{page}页] 请求 URL: {url.split('apiKey')[0]}...")
        logger.debug(f"[第{page}页] 请求参数: {json.dumps(safe_params, ensure_ascii=False)}")

        # 遇到 429 限流时自动等待重试
        for attempt in range(MAX_RETRIES):
            logger.debug(f"[第{page}页] 第{attempt+1}次尝试")
            resp = requests.get(url, params=params)
            logger.debug(f"[第{page}页] 响应状态码: {resp.status_code}, 响应头: {dict(resp.headers)}")

            if resp.status_code == 429:
                wait = PAGE_DELAY * (attempt + 1)
                logger.warning(f"[第{page}页] 触发限流(429)，等待{wait}秒后重试 (第{attempt+1}/{MAX_RETRIES}次)")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            logger.error(f"[第{page}页] 重试{MAX_RETRIES}次后仍被限流，放弃")
            resp.raise_for_status()

        data = resp.json()

        # 记录响应元数据（不含 results 原始数据，太大）
        resp_meta = {k: v for k, v in data.items() if k != "results"}
        logger.debug(f"[第{page}页] 响应元数据: {json.dumps(resp_meta, ensure_ascii=False)}")

        results = data.get("results", [])
        count = len(results)
        all_bars.extend(results)

        logger.info(f"[第{page}页] 获取 {count} 条，累计 {len(all_bars)} 条")

        if results:
            from datetime import datetime, timezone
            first_t = datetime.fromtimestamp(results[0]["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            last_t = datetime.fromtimestamp(results[-1]["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            logger.info(f"[第{page}页] 时间范围: {first_t} ~ {last_t}")
            # 记录首尾两条K线原始数据，用于排查数据问题
            logger.debug(f"[第{page}页] 首条数据: {json.dumps(results[0])}")
            logger.debug(f"[第{page}页] 末条数据: {json.dumps(results[-1])}")

        # 分页：next_url 已包含完整查询参数
        next_url = data.get("next_url")
        if next_url:
            logger.info(f"已拉取 {len(all_bars)} 条，等待 {PAGE_DELAY}秒后翻页...")
            time.sleep(PAGE_DELAY)
            url = f"{next_url}&apiKey={api_key}"
            params = {}  # next_url 自带参数，清空 params
        else:
            url = None

    logger.info(f"拉取完成，共 {len(all_bars)} 条K线，{page} 页")
    return all_bars


def fetch_hourly_bars(ticker: str, from_date: str, to_date: str, api_key: str) -> list[dict]:
    """向后兼容包装：等价于 fetch_bars(ticker, "hourly", ...)"""
    return fetch_bars(ticker, "hourly", from_date, to_date, api_key)
