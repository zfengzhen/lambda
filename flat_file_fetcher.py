"""Flat Files 期权数据获取：通过 S3 下载每日期权聚合数据并提取指定合约的 OHLC。

S3 路径格式: us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
CSV 列:      ticker, volume, open, close, high, low, window_start, transactions

认证环境变量:
    MASSIVE_S3_ACCESS_KEY — S3 Access Key ID（必须）
    MASSIVE_S3_SECRET_KEY — S3 Secret Access Key（必须）
    MASSIVE_S3_ENDPOINT   — 可选，默认 https://files.massive.com
    MASSIVE_S3_BUCKET     — 可选，默认 flatfiles
"""
import csv
import gzip
import logging
import os
import datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_ENDPOINT = os.environ.get("MASSIVE_S3_ENDPOINT", "https://files.massive.com")
_BUCKET = os.environ.get("MASSIVE_S3_BUCKET", "flatfiles")
_PREFIX = "us_options_opra/day_aggs_v1"
_CACHE_DIR = Path(__file__).parent / "output" / "flat_files_cache"


def make_s3_client():
    """从环境变量 MASSIVE_S3_ACCESS_KEY / MASSIVE_S3_SECRET_KEY 创建 S3 客户端。

    Raises:
        KeyError: 环境变量未设置时
    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["MASSIVE_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MASSIVE_S3_SECRET_KEY"],
        endpoint_url=_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )


def _s3_key(date_str: str) -> str:
    """将日期字符串转为 S3 对象键。

    '2025-01-06' → 'us_options_opra/day_aggs_v1/2025/01/2025-01-06.csv.gz'
    """
    d = datetime.date.fromisoformat(date_str)
    return f"{_PREFIX}/{d.year}/{d.month:02d}/{date_str}.csv.gz"


def _cache_path(date_str: str) -> Path:
    """返回本地缓存文件路径。"""
    return _CACHE_DIR / f"{date_str}.csv.gz"


def download_day_file(date_str: str, s3_client=None) -> Path | None:
    """下载指定日期的全市场期权日线聚合文件到本地缓存（约 2 MB/天）。

    已缓存则直接返回路径，跳过下载。
    非交易日（S3 文件不存在）返回 None。

    Args:
        date_str:  日期 "YYYY-MM-DD"
        s3_client: boto3 S3 客户端（None 则自动创建）

    Returns:
        本地缓存路径；若为非交易日则返回 None
    """
    cache = _cache_path(date_str)
    if cache.exists():
        logger.debug(f"[flat_file] 缓存命中 {date_str}")
        return cache

    if s3_client is None:
        s3_client = make_s3_client()

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = _s3_key(date_str)
    try:
        s3_client.download_file(_BUCKET, key, str(cache))
        size_kb = cache.stat().st_size // 1024 if cache.exists() else 0
        logger.info(f"[flat_file] 已下载 {date_str} ({size_kb} KB)")
        return cache if cache.exists() else None
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            logger.debug(f"[flat_file] {date_str} 非交易日，跳过")
            return None
        raise


def _read_symbol_from_file(cache_path: Path, symbol: str) -> list[dict]:
    """从 CSV.gz 中读取指定 ticker 的所有行。

    Args:
        cache_path: 本地 .csv.gz 文件路径
        symbol:     OCC symbol，如 "O:TQQQ250131P00038500"

    Returns:
        原始字符串字典列表（含 open/high/low/close 等列）
    """
    rows = []
    with gzip.open(cache_path, "rt", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("ticker") == symbol:
                rows.append(row)
    return rows


def fetch_option_bars_flat(symbol: str, from_date: str, to_date: str,
                            s3_client=None) -> list[dict]:
    """通过 Flat Files 拉取期权合约日线 OHLC，输出格式与 fetch_option_bars() 相同。

    逐日下载全市场聚合文件（有本地缓存则跳过），从中过滤目标合约。

    Args:
        symbol:    OCC symbol，如 "O:TQQQ250131P00038500"
        from_date: 起始日期 "YYYY-MM-DD"（含）
        to_date:   结束日期 "YYYY-MM-DD"（含）
        s3_client: boto3 S3 客户端（None 则自动创建）

    Returns:
        [{date, open, high, low, close}] 按日期升序；无数据返回空列表
    """
    if s3_client is None:
        s3_client = make_s3_client()

    start = datetime.date.fromisoformat(from_date)
    end = datetime.date.fromisoformat(to_date)
    bars = []

    current = start
    while current <= end:
        date_str = str(current)
        cache = download_day_file(date_str, s3_client)
        if cache is not None:
            for row in _read_symbol_from_file(cache, symbol):
                bars.append({
                    "date": date_str,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                })
        current += datetime.timedelta(days=1)

    bars.sort(key=lambda b: b["date"])
    logger.info(f"[{symbol}] flat file: {len(bars)} 条日线")
    return bars
