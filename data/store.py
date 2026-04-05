"""DuckDB 连接管理。"""
import logging

import duckdb

from config import DB_PATH

logger = logging.getLogger(__name__)


def get_connection() -> duckdb.DuckDBPyConnection:
    """打开数据库连接，自动创建 output 目录。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    """建表 + 迁移 + 存量回填，程序启动时调用一次。"""
    from data.schema import create_tables, run_migrations
    from data.writers import backfill_option_bars_columns

    con = get_connection()
    try:
        create_tables(con)
        run_migrations(con)
    finally:
        con.close()
    backfill_option_bars_columns()
    logger.info(f"DB 初始化完成: {DB_PATH}")
