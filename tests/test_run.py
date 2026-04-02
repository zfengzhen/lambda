"""run.py 单元测试"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from run import fetch_equity_bars, embed_to_html


class TestFetchEquityBars:
    """从 DuckDB 读取股票日K（含增量同步）"""

    @patch("data_sync.ensure_synced")
    @patch("data_store.query_equity_bars")
    def test_returns_dataframe_with_indicators(self, mock_query, mock_sync):
        """成功读取时返回含指标的 DataFrame。"""
        mock_query.return_value = [
            {"date": f"2026-0{i}-01", "ticker": "TQQQ",
             "open": 50.0, "high": 52.0, "low": 49.0, "close": float(50 + i),
             "volume": 100000, "vwap": 50.5, "transactions": 500}
            for i in range(1, 9)  # 8 行，足够计算 ma5
        ]
        df = fetch_equity_bars("TQQQ", "test-key")
        assert df is not None
        assert "ma5" in df.columns
        mock_sync.assert_called_once_with(["TQQQ"], "test-key")

    @patch("data_sync.ensure_synced")
    @patch("data_store.query_equity_bars")
    def test_returns_none_when_no_data(self, mock_query, mock_sync):
        """DuckDB 无数据时返回 None。"""
        mock_query.return_value = []
        df = fetch_equity_bars("TQQQ", "test-key")
        assert df is None

    @patch("data_sync.ensure_synced")
    @patch("data_store.query_equity_bars")
    def test_calls_ensure_synced_with_ticker(self, mock_query, mock_sync):
        """每次调用都触发 ensure_synced 以保证数据最新。"""
        mock_query.return_value = []
        fetch_equity_bars("QQQ", "my-api-key")
        mock_sync.assert_called_once_with(["QQQ"], "my-api-key")


class TestEmbedToHtml:
    """HTML 生成"""

    def test_embed_includes_daily_bars(self, tmp_path, monkeypatch):
        """嵌入 HTML 时包含 daily_bars（供图表渲染）"""
        monkeypatch.setattr("run.OUTPUT_DIR", str(tmp_path))
        result = {
            "ticker": "TQQQ",
            "daily_bars": [{"date": "2026-01-01", "close": 100}],
            "summary": {"total": 1},
        }
        template = '<script>var EMBEDDED_DATA = {};\n/* EMBEDDED_DATA_PLACEHOLDER */</script>'
        embed_to_html("TQQQ", result, template)

        html_content = (tmp_path / "TQQQ.html").read_text()
        assert "daily_bars" in html_content
        assert '"summary"' in html_content
