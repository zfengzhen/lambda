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

from config import S3_ENDPOINT, S3_BUCKET
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
