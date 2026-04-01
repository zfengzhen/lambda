"""S3 期权 Flat Files 下载器：逐日下载全量期权日K并存入本地 DB。

S3 路径: us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
认证环境变量: MASSIVE_S3_ACCESS_KEY, MASSIVE_S3_SECRET_KEY
             MASSIVE_S3_ENDPOINT（默认 https://files.massive.com）
             MASSIVE_S3_BUCKET（默认 flatfiles）
"""
import datetime
import logging
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import data_store
from flat_file_fetcher import download_day_file as _download_day_file

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
    return data_store.is_synced(date_str, "option")


def download_and_store_day(date_str: str, s3_client,
                            tickers: list[str] | None = None) -> int:
    """下载（或读取缓存）指定日期的期权全量文件并写入 DB。

    Returns:
        写入行数；0 表示节假日/文件不存在；-1 表示已有数据跳过
    """
    if _already_synced(date_str):
        logger.debug(f"[s3] {date_str} 已同步，跳过")
        return -1

    cache_path = _download_day_file(date_str, s3_client)
    if cache_path is None:
        return 0  # 非交易日

    written = data_store.insert_option_bars_from_csv(cache_path, date_str, tickers)
    data_store.write_sync_log(date_str, "option", written, "ok")
    logger.info(f"[s3] {date_str}: {written:,} 行写入 option_bars")
    return written


_SENTINEL = object()


def sync_options(from_date: str, to_date: str,
                 tickers: list[str] | None = None,
                 s3_client=None) -> None:
    """同步指定日期范围内的期权数据。

    下载线程与写入主线程并行：下载下一天时同步写入当前天。
    已缓存到 output/flat_files_cache/ 的文件直接读取，无需重新下载。

    Args:
        from_date: 起始日期 "YYYY-MM-DD"
        to_date:   结束日期 "YYYY-MM-DD"
        tickers:   标的代码过滤，如 ["TQQQ", "QQQ"]；None 则写入全部合约
        s3_client: boto3 S3 客户端（None 则自动创建）
    """
    import queue
    import threading

    if s3_client is None:
        s3_client = make_s3_client()

    days = trading_days(from_date, to_date)
    logger.info(
        f"[s3] 同步期权数据 {from_date} ~ {to_date}，共 {len(days)} 个交易日"
        + (f"，标的: {tickers}" if tickers else "")
    )

    q = queue.Queue(maxsize=3)

    def producer():
        for date_str in days:
            if _already_synced(date_str):
                q.put((date_str, None))
                continue
            try:
                cache_path = _download_day_file(date_str, s3_client)
                q.put((date_str, cache_path))
            except Exception as e:
                data_store.write_sync_log(date_str, "option", 0, "error", str(e))
                logger.error(f"[s3] {date_str} 下载失败: {e}")
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
                written = data_store.insert_option_bars_from_csv(
                    cache_path, date_str, tickers
                )
                data_store.write_sync_log(date_str, "option", written, "ok")
                logger.info(f"[s3] {date_str}: {written:,} 行写入 option_bars")
            except Exception as e:
                data_store.write_sync_log(date_str, "option", 0, "error", str(e))
                logger.error(f"[s3] {date_str} 写入失败: {e}")

    t.join()
