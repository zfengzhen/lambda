# tests/data/sync/test_options.py
"""data.sync.options 单元测试：S3 下载 + 期权数据同步。
合并原 test_s3_downloader.py + test_flat_file_fetcher.py。
"""
import csv
import gzip
import io
import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data.store import init_db
from data.sync.options import (
    download_day_file, sync_options, _trading_days, _CACHE_DIR,
)
from data.queries import is_synced
from data.writers import write_sync_log


def _make_csv_gz(rows: list[dict]) -> bytes:
    """辅助函数：将字典列表写成 gzip CSV bytes。"""
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


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


# ── _trading_days ───────────────────────────────────────────

def test_trading_days_excludes_weekends():
    days = _trading_days("2025-01-04", "2025-01-10")
    weekdays = {datetime.date.fromisoformat(d).weekday() for d in days}
    assert 5 not in weekdays  # 周六
    assert 6 not in weekdays  # 周日
    assert "2025-01-06" in days  # 周一
    assert "2025-01-10" in days  # 周五


def test_trading_days_range():
    days = _trading_days("2025-01-06", "2025-01-10")
    assert days == ["2025-01-06", "2025-01-07", "2025-01-08",
                    "2025-01-09", "2025-01-10"]


# ── download_day_file ─────────────────────────────────────

def test_download_day_file_cache_hit(tmp_path):
    """缓存命中时不调用 S3。"""
    with patch("data.sync.options._CACHE_DIR", tmp_path):
        cache = tmp_path / "2025-01-06.csv.gz"
        cache.write_bytes(b"data")
        mock_s3 = MagicMock()
        result = download_day_file("2025-01-06", mock_s3)
        assert result == cache
        mock_s3.download_file.assert_not_called()


def test_download_day_file_downloads_and_caches(tmp_path):
    """S3 下载成功时写入缓存并返回路径。"""
    with patch("data.sync.options._CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()

        def fake_download(bucket, key, local_path):
            Path(local_path).write_bytes(b"fake_data")

        mock_s3.download_file.side_effect = fake_download
        result = download_day_file("2025-01-06", mock_s3)
        assert result == tmp_path / "2025-01-06.csv.gz"
        assert result.exists()


def test_download_day_file_not_trading_day(tmp_path):
    """S3 返回 404 时视为非交易日，返回 None。"""
    from botocore.exceptions import ClientError

    with patch("data.sync.options._CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject"
        )
        result = download_day_file("2025-01-04", mock_s3)  # 周六
        assert result is None


def test_download_day_file_reraises_non_404(tmp_path):
    """非 404 的 S3 错误应向上抛出。"""
    from botocore.exceptions import ClientError

    with patch("data.sync.options._CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "GetObject"
        )
        with pytest.raises(ClientError):
            download_day_file("2025-01-06", mock_s3)


# ── sync_options ────────────────────────────────────────────

def test_sync_options_processes_date_range(tmp_db, tmp_path, mock_s3):
    """同月两天都被下载写入，sync_log 写月级记录。"""
    gz_data = _make_csv_gz([
        {"ticker": "O:TQQQ250131P00038500", "volume": "5",
         "open": "0.85", "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "2"},
    ])
    call_count = {"n": 0}

    def fake_download(date_str, s3_client=None):
        call_count["n"] += 1
        f = tmp_path / f"{date_str}.csv.gz"
        f.write_bytes(gz_data)
        return f

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.options.download_day_file", side_effect=fake_download):
        sync_options("2025-01-06", "2025-01-07", s3_client=mock_s3)

    assert call_count["n"] == 2
    # 月级 sync_log（键为月份第一天）
    import duckdb
    con = duckdb.connect(str(tmp_db))
    row = con.execute(
        "SELECT status FROM sync_log WHERE date='2025-01-01' AND data_type='option_month'"
    ).fetchone()
    con.close()
    assert row is not None and row[0] == "ok"


def test_sync_options_skips_synced_month(tmp_db, tmp_path, mock_s3):
    """月级 sync_log 存在时整月跳过，不触发下载。"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        # 预先写入 2025-01 月级同步记录
        write_sync_log("2025-01-01", "option_month", 100, "ok")

        with patch("data.sync.options.download_day_file") as mock_dl:
            sync_options("2025-01-06", "2025-01-10", s3_client=mock_s3)

    mock_dl.assert_not_called()
