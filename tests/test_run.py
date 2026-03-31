"""run.py 单元测试"""
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from run import bars_to_daily_df, load_existing_data, fetch_daily_bars, embed_to_html


class TestBarsToDailyDf:
    """API 原始数据转 DataFrame"""

    def test_basic_conversion(self):
        """原始 bar dict 正确转为 DataFrame"""
        bars = [
            {"o": 50.0, "h": 52.0, "l": 49.0, "c": 51.0,
             "v": 100000, "vw": 50.8, "n": 500, "t": 1711627200000},
        ]
        df = bars_to_daily_df(bars)
        assert list(df.columns) == ["date", "open", "high", "low", "close", "volume", "vwap", "transactions"]
        assert len(df) == 1
        assert df.iloc[0]["open"] == 50.0
        assert isinstance(df.iloc[0]["date"], str)

    def test_sorted_by_date(self):
        """输出按日期正序排列"""
        bars = [
            {"o": 1, "h": 2, "l": 0, "c": 1.5, "v": 100, "vw": 1.2, "n": 10, "t": 1711713600000},
            {"o": 1, "h": 2, "l": 0, "c": 1.5, "v": 100, "vw": 1.2, "n": 10, "t": 1711627200000},
        ]
        df = bars_to_daily_df(bars)
        dates = df["date"].tolist()
        assert dates == sorted(dates)


class TestLoadExistingData:
    """读取已有 JSON"""

    def test_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("run.DATA_DIR", str(tmp_path))
        assert load_existing_data("TQQQ") is None

    def test_returns_dict_when_exists(self, tmp_path, monkeypatch):
        monkeypatch.setattr("run.DATA_DIR", str(tmp_path))
        data = {"ticker": "TQQQ", "daily_bars": [], "data_range": ["2026-01-01", "2026-03-01"]}
        (tmp_path / "TQQQ.json").write_text(json.dumps(data))
        result = load_existing_data("TQQQ")
        assert result["ticker"] == "TQQQ"


class TestFetchDailyBars:
    """增量/全量拉取逻辑"""

    @patch("fetch_client.fetch_bars")
    @patch("run.load_existing_data")
    def test_incremental_merge(self, mock_load, mock_fetch):
        """增量拉取时合并新老数据"""
        old_bars = [
            {"date": "2026-03-01", "open": 50, "high": 52, "low": 49, "close": 51, "volume": 100, "vwap": 50.5, "transactions": 10},
        ]
        mock_load.return_value = {
            "data_range": ["2026-01-01", "2026-03-01"],
            "daily_bars": old_bars,
        }
        new_api_bars = [
            {"o": 55, "h": 57, "l": 54, "c": 56, "v": 200, "vw": 55.5, "n": 20, "t": 1741046400000},
        ]
        mock_fetch.return_value = new_api_bars

        df = fetch_daily_bars("TQQQ", 10, "test-key", full=False)
        assert df is not None
        assert len(df) >= 2
        assert "ma5" in df.columns

    @patch("fetch_client.fetch_bars")
    def test_full_fetch(self, mock_fetch):
        """全量拉取"""
        bars = [
            {"o": 50, "h": 52, "l": 49, "c": 51, "v": 100, "vw": 50.5, "n": 10, "t": 1711627200000},
        ]
        mock_fetch.return_value = bars
        df = fetch_daily_bars("TQQQ", 10, "test-key", full=True)
        assert df is not None
        assert "ma5" in df.columns

    @patch("fetch_client.fetch_bars")
    def test_full_fetch_no_data(self, mock_fetch):
        """全量拉取无数据返回 None"""
        mock_fetch.return_value = []
        df = fetch_daily_bars("TQQQ", 10, "test-key", full=True)
        assert df is None


class TestEmbedToHtml:
    """HTML 生成"""

    def test_embed_excludes_daily_bars(self, tmp_path, monkeypatch):
        """嵌入 HTML 时不包含 daily_bars"""
        monkeypatch.setattr("run.OUTPUT_DIR", str(tmp_path))
        result = {
            "ticker": "TQQQ",
            "daily_bars": [{"date": "2026-01-01", "close": 100}],
            "summary": {"total": 1},
        }
        template = '<script>var EMBEDDED_DATA = {};\n/* EMBEDDED_DATA_PLACEHOLDER */</script>'
        embed_to_html("TQQQ", result, template)

        html_content = (tmp_path / "TQQQ.html").read_text()
        assert "daily_bars" not in html_content
        assert '"summary"' in html_content
