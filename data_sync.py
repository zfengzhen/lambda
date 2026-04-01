"""数据同步：调度 S3 期权下载和 REST 股票下载。

用法:
    python data_sync.py                      # 同步所有标的
    python data_sync.py --tickers TQQQ QQQ   # 同步指定标的
"""
import argparse
import datetime
import logging
import os
import sys

import data_store
import s3_downloader
import rest_downloader

logger = logging.getLogger(__name__)

_FULL_SYNC_YEARS = 2


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
        from_date = str(today - datetime.timedelta(days=365 * _FULL_SYNC_YEARS))

    if from_date > to_date:
        logger.info("数据已是最新，无需同步")
        return

    logger.info(f"同步 {from_date} ~ {to_date}，标的: {tickers or '全部'}")
    s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)
    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Lambda 策略数据同步")
    parser.add_argument("--tickers", nargs="*", default=[],
                        help="股票标的列表，如 TQQQ QQQ")
    args = parser.parse_args()

    api_key = os.environ.get("MASSIVE_API_KEY", "")
    if not api_key and args.tickers:
        print("警告：未设置 MASSIVE_API_KEY，跳过股票数据同步")

    ensure_synced(tickers=args.tickers, api_key=api_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
