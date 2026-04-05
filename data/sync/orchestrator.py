"""数据同步编排：ensure_synced 统一入口。"""
import datetime
import logging

from config import TICKER, FULL_SYNC_YEARS
from data.store import init_db
from data.queries import get_latest_equity_date
from data.writers import delete_all_data
from data.sync.splits import download_splits
from data.sync.equity import download_and_store
from data.sync.options import sync_options
from data.sync.iv import sync_ticker_iv

logger = logging.getLogger(__name__)


def ensure_synced(api_key: str) -> None:
    """确保 DuckDB 数据最新。空库同步近 2 年，有数据增量补到昨天。

    流程：splits检测 → 清空重拉 → equity增量 → options按月 → IV增量
    """
    init_db()
    today = datetime.date.today()
    to_date = str(today - datetime.timedelta(days=1))
    full_sync_from = str(today - datetime.timedelta(days=365 * FULL_SYNC_YEARS))

    # 1. 同步拆股数据，检测新事件
    need_purge = False
    if api_key:
        new_splits = download_splits(api_key)
        if new_splits:
            need_purge = True

    # 2. 有新拆股 → 清空数据
    if need_purge:
        logger.info(f"[sync] {TICKER} 检测到新拆股，清空数据准备全量重拉")
        delete_all_data()

    # 3. 同步 equity
    if api_key:
        if need_purge:
            eq_from = full_sync_from
        else:
            latest = get_latest_equity_date()
            if not latest:
                eq_from = full_sync_from
            else:
                eq_from = str(datetime.date.fromisoformat(latest)
                              + datetime.timedelta(days=1))
        if eq_from <= to_date:
            logger.info(f"[sync] equity 同步 {eq_from} ~ {to_date}")
            download_and_store(eq_from, to_date, api_key)
        else:
            logger.info("[sync] equity 已是最新")

    # 4. 同步 option
    sync_options(full_sync_from, to_date)

    # 5. 计算 IV
    sync_ticker_iv()
