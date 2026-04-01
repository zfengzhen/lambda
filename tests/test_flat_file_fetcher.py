"""flat_file_fetcher 单元测试"""
import csv
import gzip
import io
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import flat_file_fetcher as ff


# ── _s3_key ────────────────────────────────────────────────

def test_s3_key_format():
    assert ff._s3_key("2025-01-06") == "us_options_opra/day_aggs_v1/2025/01/2025-01-06.csv.gz"


def test_s3_key_zero_padded_month():
    assert ff._s3_key("2024-03-07") == "us_options_opra/day_aggs_v1/2024/03/2024-03-07.csv.gz"


def test_s3_key_december():
    assert ff._s3_key("2023-12-29") == "us_options_opra/day_aggs_v1/2023/12/2023-12-29.csv.gz"


# ── download_day_file ─────────────────────────────────────

def test_download_day_file_cache_hit(tmp_path):
    """缓存命中时不调用 S3。"""
    with patch.object(ff, "_CACHE_DIR", tmp_path):
        cache = tmp_path / "2025-01-06.csv.gz"
        cache.write_bytes(b"data")
        mock_s3 = MagicMock()
        result = ff.download_day_file("2025-01-06", mock_s3)
        assert result == cache
        mock_s3.download_file.assert_not_called()


def test_download_day_file_downloads_and_caches(tmp_path):
    """S3 下载成功时写入缓存并返回路径。"""
    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()

        def fake_download(bucket, key, local_path):
            Path(local_path).write_bytes(b"fake_data")

        mock_s3.download_file.side_effect = fake_download
        result = ff.download_day_file("2025-01-06", mock_s3)
        assert result == tmp_path / "2025-01-06.csv.gz"
        assert result.exists()


def test_download_day_file_not_trading_day(tmp_path):
    """S3 返回 404 时视为非交易日，返回 None。"""
    from botocore.exceptions import ClientError

    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject"
        )
        result = ff.download_day_file("2025-01-04", mock_s3)  # 周六
        assert result is None


def test_download_day_file_reraises_non_404(tmp_path):
    """非 404 的 S3 错误应向上抛出。"""
    from botocore.exceptions import ClientError

    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "GetObject"
        )
        with pytest.raises(ClientError):
            ff.download_day_file("2025-01-06", mock_s3)


# ── _read_symbol_from_file ───────────────────────────────

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


def test_read_symbol_filters_correct_rows(tmp_path):
    data = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "5"},
        {"ticker": "O:TQQQ250131P00040000", "volume": "3", "open": "0.10",
         "close": "0.12", "high": "0.15", "low": "0.09",
         "window_start": "1000", "transactions": "2"},
    ]
    cache = tmp_path / "test.csv.gz"
    cache.write_bytes(_make_csv_gz(data))

    rows = ff._read_symbol_from_file(cache, "O:TQQQ250131P00038500")
    assert len(rows) == 1
    assert rows[0]["close"] == "0.87"


def test_read_symbol_no_match(tmp_path):
    data = [{"ticker": "O:SPY250131P00500000", "volume": "1", "open": "1.0",
             "close": "1.0", "high": "1.0", "low": "1.0",
             "window_start": "1000", "transactions": "1"}]
    cache = tmp_path / "test.csv.gz"
    cache.write_bytes(_make_csv_gz(data))

    rows = ff._read_symbol_from_file(cache, "O:TQQQ250131P00038500")
    assert rows == []


# ── fetch_option_bars_flat ───────────────────────────────

def _make_day_cache(tmp_path: Path, date_str: str, symbol: str,
                    open_: float, high: float, low: float, close: float):
    """在 tmp_path 创建指定日期的缓存文件。"""
    data = [{"ticker": symbol, "volume": "5", "open": str(open_),
             "close": str(close), "high": str(high), "low": str(low),
             "window_start": "0", "transactions": "1"}]
    cache = tmp_path / f"{date_str}.csv.gz"
    cache.write_bytes(_make_csv_gz(data))
    return cache


def test_fetch_option_bars_flat_basic(tmp_path):
    """正常情况：连续两个交易日各有一条数据。"""
    symbol = "O:TQQQ250131P00038500"
    _make_day_cache(tmp_path, "2025-01-06", symbol, 0.85, 0.90, 0.80, 0.87)
    _make_day_cache(tmp_path, "2025-01-07", symbol, 0.87, 0.95, 0.85, 0.92)

    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        bars = ff.fetch_option_bars_flat(symbol, "2025-01-06", "2025-01-07", mock_s3)

    assert len(bars) == 2
    assert bars[0] == {"date": "2025-01-06", "open": 0.85, "high": 0.90,
                        "low": 0.80, "close": 0.87}
    assert bars[1]["date"] == "2025-01-07"
    # 无缓存命中也不应调用 S3（缓存已由上面创建）
    mock_s3.download_file.assert_not_called()


def test_fetch_option_bars_flat_skips_weekends(tmp_path):
    """周六和周日没有文件，应跳过而非报错。"""
    symbol = "O:TQQQ250131P00038500"
    _make_day_cache(tmp_path, "2025-01-06", symbol, 0.85, 0.90, 0.80, 0.87)
    # 2025-01-04（周六）和 2025-01-05（周日）无文件 → download_day_file 返回 None

    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = __import__("botocore.exceptions",
            fromlist=["ClientError"]).ClientError(
            {"Error": {"Code": "404", "Message": ""}}, "GetObject"
        )
        bars = ff.fetch_option_bars_flat(symbol, "2025-01-04", "2025-01-06", mock_s3)

    # 只有 2025-01-06 有数据
    assert len(bars) == 1
    assert bars[0]["date"] == "2025-01-06"


def test_fetch_option_bars_flat_empty_range(tmp_path):
    """空范围（无匹配 symbol）返回空列表。"""
    symbol = "O:TQQQ250131P00038500"
    # 创建文件但里面是其他 symbol
    other_symbol = "O:QQQ250131P00400000"
    _make_day_cache(tmp_path, "2025-01-06", other_symbol, 1.0, 1.5, 0.9, 1.2)

    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        bars = ff.fetch_option_bars_flat(symbol, "2025-01-06", "2025-01-06", mock_s3)

    assert bars == []


def test_fetch_option_bars_flat_sorted_ascending(tmp_path):
    """返回结果按日期升序；中间的 2025-01-07 有其他 symbol，不影响排序。"""
    symbol = "O:TQQQ250131P00038500"
    _make_day_cache(tmp_path, "2025-01-08", symbol, 0.9, 1.0, 0.85, 0.95)
    _make_day_cache(tmp_path, "2025-01-07", "O:OTHER", 1.0, 1.5, 0.9, 1.2)  # 无目标 symbol
    _make_day_cache(tmp_path, "2025-01-06", symbol, 0.85, 0.90, 0.80, 0.87)

    with patch.object(ff, "_CACHE_DIR", tmp_path):
        mock_s3 = MagicMock()
        bars = ff.fetch_option_bars_flat(symbol, "2025-01-06", "2025-01-08", mock_s3)

    assert len(bars) == 2
    dates = [b["date"] for b in bars]
    assert dates == sorted(dates)
