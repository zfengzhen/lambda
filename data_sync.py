"""数据同步：调度 S3 期权下载和 REST 股票下载。

用法:
    python data_sync.py                      # 同步所有标的
    python data_sync.py --tickers TQQQ QQQ   # 同步指定标的
"""
import argparse
import datetime
import logging
import math
import os
import sys

import data_store
import iv
import s3_downloader
import rest_downloader

logger = logging.getLogger(__name__)

_FULL_SYNC_YEARS = 2


def ensure_synced(tickers: list[str], api_key: str) -> None:
    """确保 DuckDB 数据最新。空库同步近 2 年，有数据增量补到昨天。

    流程：
    1. 同步 splits 表，检测新拆股事件
    2. 有新拆股 → 清空该 ticker 数据（触发全量重拉）
    3. 同步 equity_bars（adjusted=true）
    4. 同步 option_bars（入库时按因子调整）
    5. 计算 ticker IV（增量/全量）
    """
    data_store.init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))
    full_sync_from = str(today - datetime.timedelta(days=365 * _FULL_SYNC_YEARS))

    # ── 1. 同步拆股数据，检测新事件 ──
    need_purge = set()
    if tickers and api_key:
        for ticker in tickers:
            new_splits = rest_downloader.download_splits(ticker, api_key)
            if new_splits:
                need_purge.add(ticker)

    # ── 2. 有新拆股 → 清空数据 ──
    for ticker in need_purge:
        logger.info(f"[sync] {ticker} 检测到新拆股，清空数据准备全量重拉")
        data_store.delete_ticker_data(ticker)

    # ── 3. 确定同步日期范围 ──
    latest = data_store.get_latest_synced_date("equity")
    if need_purge or not latest:
        from_date = full_sync_from
    else:
        from_date = str(datetime.date.fromisoformat(latest)
                        + datetime.timedelta(days=1))

    if from_date <= to_date:
        logger.info(f"同步 {from_date} ~ {to_date}，标的: {tickers or '全部'}")
        s3_downloader.sync_options(from_date, to_date, tickers=tickers or None)
        if tickers and api_key:
            rest_downloader.sync_equity(tickers, from_date, to_date, api_key)
    else:
        logger.info("数据已是最新，无需同步")

    # ── 4. 计算 IV（无论是否同步了新数据） ──
    if tickers:
        sync_ticker_iv(tickers)


def sync_ticker_iv(tickers: list[str]) -> None:
    """计算并存储各 ticker 的 IV。空表全量，有数据增量。"""
    for ticker in tickers:
        latest_iv = data_store.get_latest_iv_date(ticker)
        latest_opt = data_store.get_latest_synced_date("option")

        if not latest_opt:
            logger.info(f"[iv] {ticker} 无 option_bars 数据，跳过 IV 计算")
            continue

        if latest_iv:
            from_date = str(datetime.date.fromisoformat(latest_iv)
                            + datetime.timedelta(days=1))
        else:
            # 全量：从 option_bars 最早日期开始
            con = data_store._connect()
            try:
                result = con.execute(
                    "SELECT MIN(date) FROM option_bars WHERE symbol LIKE ?",
                    [f"O:{ticker}%"],
                ).fetchone()
            finally:
                con.close()
            if not result or result[0] is None:
                continue
            from_date = str(result[0])

        to_date = latest_opt
        if from_date > to_date:
            logger.info(f"[iv] {ticker} IV 已是最新")
            continue

        logger.info(f"[iv] {ticker} 计算 IV: {from_date} ~ {to_date}")

        # 获取日期列表
        con = data_store._connect()
        try:
            dates = [str(r[0]) for r in con.execute(
                "SELECT DISTINCT date FROM option_bars "
                "WHERE symbol LIKE ? AND date BETWEEN ? AND ? ORDER BY date",
                [f"O:{ticker}%", from_date, to_date],
            ).fetchall()]
        finally:
            con.close()

        iv_rows = []
        for d in dates:
            # 查当天 option_bars（含结构化字段）
            con = data_store._connect()
            try:
                bars = con.execute(
                    "SELECT date, symbol, open, high, low, close, volume, "
                    "transactions, strike, expiration, option_type "
                    "FROM option_bars "
                    "WHERE symbol LIKE ? AND date = ?",
                    [f"O:{ticker}%", d],
                ).fetchall()
            finally:
                con.close()
            option_bars = [
                {"date": str(r[0]), "symbol": r[1], "open": r[2], "high": r[3],
                 "low": r[4], "close": r[5], "volume": r[6], "transactions": r[7],
                 "strike": r[8], "expiration": str(r[9]) if r[9] else None,
                 "option_type": r[10]}
                for r in bars
            ]

            # 查当天 spot
            eq = data_store.query_equity_bars(ticker, d, d)
            if not eq:
                continue
            spot = eq[0]["close"]

            result = iv.compute_ticker_iv(option_bars, spot, d)
            if not math.isnan(result):
                iv_rows.append({"date": d, "ticker": ticker, "iv": result})

        if iv_rows:
            data_store.upsert_ticker_iv(iv_rows)
            logger.info(f"[iv] {ticker} 写入 {len(iv_rows)} 天 IV")


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
