# tests/output/test_report.py
"""output.report 单元测试：HTML 嵌入。
迁移自 test_run.py 中的 HTML embedding 测试。
"""
import json
import os

import pytest

from output.report import build_report_data, render_html, save_json


class TestBuildReportData:
    """JSON 组装"""

    def test_contains_all_keys(self):
        data = build_report_data(
            tiers=[], summary={"total": 1}, tier_stats={},
            latest={}, weeks=[], daily_bars=[],
            market=None, data_range=["2026-01-01", "2026-03-01"],
            generated="2026-03-01T12:00:00", otm_config={},
        )
        assert "ticker" in data
        assert "summary" in data
        assert "daily_bars" in data
        assert "market" in data
        assert data["ticker"] == "TQQQ"


class TestRenderHtml:
    """HTML 渲染"""

    def test_embed_includes_daily_bars(self, tmp_path, monkeypatch):
        """嵌入 HTML 时包含 daily_bars"""
        # 使 output.report 的 _THIS_DIR 指向 tmp_path
        monkeypatch.setattr("output.report._THIS_DIR", str(tmp_path))
        # 创建模板文件
        template = '<script>var EMBEDDED_DATA = {};\n/* EMBEDDED_DATA_PLACEHOLDER */</script>'
        (tmp_path / "template.html").write_text(template, encoding="utf-8")

        data = build_report_data(
            tiers=[], summary={"total": 1}, tier_stats={},
            latest={}, weeks=[], daily_bars=[{"date": "2026-01-01", "close": 100}],
            market=None, data_range=["2026-01-01", "2026-03-01"],
            generated="2026-03-01T12:00:00", otm_config={},
        )
        html_path = render_html(data)

        assert html_path is not None
        html_content = open(html_path, encoding="utf-8").read()
        assert "daily_bars" in html_content
        assert '"summary"' in html_content


class TestMarketField:
    """market 行情快照字段"""

    def test_market_field_from_json(self):
        """已生成的 JSON 应包含 market 字段且涨跌幅计算正确。"""
        json_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "output", "TQQQ.json")
        if not os.path.exists(json_path):
            pytest.skip("TQQQ.json 不存在，需先运行 run.py")
        with open(json_path) as f:
            result = json.load(f)
        m = result.get("market")
        assert m is not None, "market 字段缺失"
        for key in ("date", "close", "change_pct"):
            assert key in m, f"market 缺少 {key}"
        assert isinstance(m["change_pct"], (int, float))
