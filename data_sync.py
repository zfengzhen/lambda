"""数据同步 CLI：调度 S3 期权下载和 REST 股票下载。

用法:
    python data_sync.py --tickers TQQQ QQQ             # 全量建库（近 2 年）
    python data_sync.py --incremental --tickers TQQQ   # 增量补齐到昨天
    python data_sync.py --incremental                  # 仅同步期权（无需 ticker）
    python data_sync.py --month 2024-04 --tickers TQQQ # 同步指定月份
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


def date_range_from_years(years: int) -> tuple[str, str]:
    """返回 (from_date, to_date)，to_date 为昨天，from_date 为 years 年前。"""
    today = datetime.date.today()
    to_date = today - datetime.timedelta(days=1)
    from_date = today - datetime.timedelta(days=years * 365)
    return str(from_date), str(to_date)


_FULL_SYNC_YEARS = 2


def full_sync(tickers: list[str], api_key: str) -> None:
    """全量同步：S3 期权 + REST 股票（近 2 年）。"""
    from_date, to_date = date_range_from_years(_FULL_SYNC_YEARS)
    logger.info(f"全量同步 {from_date} ~ {to_date}，标的: {tickers}")

    data_store.init_db()
    s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)

    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)


def month_sync(month: str, tickers: list[str], api_key: str) -> None:
    """单月同步：仅同步指定月份的期权（+ 股票）数据。

    Args:
        month: "YYYY-MM" 格式
    """
    import calendar
    try:
        year, mon = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        raise ValueError(f"--month 格式错误，应为 YYYY-MM，收到: {month!r}")

    last_day = calendar.monthrange(year, mon)[1]
    from_date = f"{year}-{mon:02d}-01"
    to_date = f"{year}-{mon:02d}-{last_day:02d}"
    logger.info(f"单月同步 {from_date} ~ {to_date}，标的: {tickers or '全部'}")

    data_store.init_db()
    s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)

    if tickers and api_key:
        rest_downloader.sync_equity(tickers, from_date, to_date, api_key)


def incremental_sync(tickers: list[str], api_key: str) -> None:
    """增量同步：从上次最新日期的次日同步到昨天。

    无历史数据时默认补最近 30 天。
    """
    data_store.init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))

    latest = data_store.get_latest_synced_date("option")
    if latest:
        from_date = str(datetime.date.fromisoformat(latest)
                        + datetime.timedelta(days=1))
    else:
        from_date = str(today - datetime.timedelta(days=30))

    if from_date > to_date:
        logger.info("数据已是最新，无需同步")
        return

    logger.info(f"增量同步 {from_date} ~ {to_date}")
    s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)

    if tickers and not api_key:
        logger.warning("未设置 MASSIVE_API_KEY，跳过股票数据同步")
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
    parser.add_argument("--incremental", action="store_true",
                        help="增量同步模式")
    parser.add_argument("--month", type=str, default=None,
                        help="单月同步，格式 YYYY-MM，如 2024-04")
    args = parser.parse_args()

    api_key = os.environ.get("MASSIVE_API_KEY", "")

    if args.month:
        month_sync(month=args.month, tickers=args.tickers, api_key=api_key)
    elif args.incremental:
        incremental_sync(tickers=args.tickers, api_key=api_key)
    else:
        if not api_key and args.tickers:
            print("警告：未设置 MASSIVE_API_KEY，跳过股票数据同步")
        full_sync(tickers=args.tickers, api_key=api_key)

    return 0


if __name__ == "__main__":
    sys.exit(main())
