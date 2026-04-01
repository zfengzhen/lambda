# tests/test_s3_downloader.py
import csv
import gzip
import io
import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

import pytest
import s3_downloader
import data_store


def _make_csv_gz(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="w") as gz:
        gz.write(buf.getvalue().encode())
    return out.getvalue()


@pytest.fixture
def mock_s3():
    return MagicMock()


def test_trading_days_excludes_weekends():
    days = s3_downloader.trading_days("2025-01-04", "2025-01-10")
    weekdays = {datetime.date.fromisoformat(d).weekday() for d in days}
    assert 5 not in weekdays  # 周六
    assert 6 not in weekdays  # 周日
    assert "2025-01-06" in days  # 周一
    assert "2025-01-10" in days  # 周五


def test_trading_days_range():
    days = s3_downloader.trading_days("2025-01-06", "2025-01-10")
    assert days == ["2025-01-06", "2025-01-07", "2025-01-08",
                    "2025-01-09", "2025-01-10"]


def test_download_and_store_day_success(tmp_path, mock_s3):
    sample_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10",
         "open": "0.85", "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
        {"ticker": "O:QQQ250131P00400000", "volume": "5",
         "open": "1.0", "close": "1.2", "high": "1.5", "low": "0.9",
         "window_start": "1000", "transactions": "2"},
    ]
    mock_s3.get_object.return_value = {
        "Body": io.BytesIO(_make_csv_gz(sample_rows))
    }

    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        count = s3_downloader.download_and_store_day("2025-01-06", mock_s3)

    assert count == 2
    import duckdb
    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT COUNT(*) FROM option_bars").fetchone()[0]
    con.close()
    assert rows == 2


def test_download_and_store_day_skips_holiday(mock_s3):
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject"
    )
    count = s3_downloader.download_and_store_day("2025-01-01", mock_s3)
    assert count == 0


def test_download_and_store_day_skips_existing(tmp_path, mock_s3):
    """已有数据的日期不重复下载。"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_option_bars([{
            "date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
            "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
            "volume": 10, "transactions": 3,
        }])
        data_store.write_sync_log("2025-01-06", "option", 1, "ok")
        count = s3_downloader.download_and_store_day("2025-01-06", mock_s3)

    mock_s3.get_object.assert_not_called()
    assert count == -1  # 跳过标记


def test_sync_options_processes_date_range(tmp_path, mock_s3):
    sample_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "5",
         "open": "0.85", "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "2"},
    ]
    gz_data = _make_csv_gz(sample_rows)
    mock_s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(gz_data)}
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("s3_downloader.make_s3_client", return_value=mock_s3):
            s3_downloader.sync_options("2025-01-06", "2025-01-07")
    # 两个交易日各调用一次
    assert mock_s3.get_object.call_count == 2
