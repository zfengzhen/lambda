# tests/core/test_backtest.py
"""core.backtest 单元测试：回测、汇总、层级统计、最近决策。"""
import datetime

import pandas as pd
import pytest

from config import DEFAULT_OTM
from core.backtest import (
    backtest_weeks,
    compute_summary,
    compute_tiers,
    compute_latest,
)


# ---------------------------------------------------------------------------
# backtest_weeks
# ---------------------------------------------------------------------------
class TestBacktestWeeks:
    """小场景回测：3 周数据"""

    @pytest.fixture()
    def scenario(self):
        """构造 3 周的 weekly_rows 和对应 daily_df"""
        weekly_rows = [
            dict(date=datetime.date(2026, 1, 5), close=100, macd=-3, prev_macd=-5,
                 pivot_5_pp=90, pivot_30_pp=90, ma20=95, ma60=85, dif=1, hist_vol=60),
            dict(date=datetime.date(2026, 1, 12), close=95, macd=-10, prev_macd=-5,
                 pivot_5_pp=120, pivot_30_pp=120, ma20=100, ma60=90, dif=2, hist_vol=60),
            dict(date=datetime.date(2026, 1, 19), close=90, macd=-10, prev_macd=-5,
                 pivot_5_pp=120, pivot_30_pp=120, ma20=100, ma60=90, dif=-2, hist_vol=70),
        ]
        dates = pd.date_range("2026-01-05", "2026-02-28", freq="B")
        daily_df = pd.DataFrame({
            "date": dates,
            "close": [100 + i * 0.1 for i in range(len(dates))],
        })
        return weekly_rows, daily_df

    def test_backtest_length(self, scenario):
        """回测结果条数 = 输入周数"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        assert len(result) == 3

    def test_backtest_reverse_order(self, scenario):
        """结果按时间倒序排列"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        dates = [r["date"] for r in result]
        assert all(isinstance(d, str) for d in dates)
        assert dates == sorted(dates, reverse=True)

    def test_backtest_tier_assigned(self, scenario):
        """每条结果都有 tier 字段"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert "tier" in r
            assert r["tier"] in ("A", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4")

    def test_backtest_new_fields(self, scenario):
        """回测结果包含 settle_diff / safe_expiry，已移除的展示字段不应存在"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert "settle_diff" in r
            assert "safe_expiry" in r
            assert "pct_change" not in r
            assert "period_low" not in r
            assert "low_vs_strike" not in r
            assert "prem_pct" not in r
            assert "pnl_pct" not in r
            assert "cum_pnl" not in r
            assert "premium" not in r
            assert "pnl" not in r

    def test_backtest_otm_is_int(self, scenario):
        """otm 字段为整数"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert isinstance(r["otm"], int)

    def test_backtest_date_is_string(self, scenario):
        """date 和 expiry_date 为字符串"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert isinstance(r["date"], str)
            if r["expiry_date"] is not None:
                assert isinstance(r["expiry_date"], str)


# ---------------------------------------------------------------------------
# compute_summary
# ---------------------------------------------------------------------------
class TestComputeSummary:
    """汇总统计"""

    @staticmethod
    def _make_weeks():
        return [
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "B1", "pending": False, "safe_expiry": False},
            {"tier": "B2", "pending": True,  "safe_expiry": None},
            {"tier": "C1", "pending": False, "safe_expiry": False},
            {"tier": "C1", "pending": True,  "safe_expiry": None},
        ]

    def test_summary_keys_exist(self):
        result = compute_summary(self._make_weeks())
        expected_keys = {
            "total_weeks", "settled", "pending",
            "safe_count", "safe_rate",
        }
        assert expected_keys == set(result.keys())

    def test_total_weeks(self):
        result = compute_summary(self._make_weeks())
        assert result["total_weeks"] == 5

    def test_settled_and_pending(self):
        result = compute_summary(self._make_weeks())
        assert result["settled"] == 3
        assert result["pending"] == 2

    def test_safe_count(self):
        result = compute_summary(self._make_weeks())
        assert result["safe_count"] == 1

    def test_safe_rate(self):
        result = compute_summary(self._make_weeks())
        assert result["safe_rate"] == 33.3

    def test_empty_weeks(self):
        result = compute_summary([])
        assert result["total_weeks"] == 0
        assert result["safe_count"] == 0
        assert result["safe_rate"] == 0.0


# ---------------------------------------------------------------------------
# compute_tiers
# ---------------------------------------------------------------------------
class TestComputeTiers:
    """按层级统计"""

    @staticmethod
    def _make_weeks():
        return [
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "B1", "pending": False, "safe_expiry": False},
            {"tier": "C1", "pending": False, "safe_expiry": None},
        ]

    def test_only_traded_tiers(self):
        result = compute_tiers(self._make_weeks())
        assert set(result.keys()) == {"A", "B1", "C1"}

    def test_tier_keys_structure(self):
        result = compute_tiers(self._make_weeks())
        for tier_val in result.values():
            assert {"name", "otm", "count", "settled", "safe_count", "safe_rate"} == set(tier_val.keys())

    def test_tier_a_count(self):
        result = compute_tiers(self._make_weeks())
        assert result["A"]["count"] == 2

    def test_tier_a_safe(self):
        result = compute_tiers(self._make_weeks())
        assert result["A"]["safe_count"] == 2
        assert result["A"]["safe_rate"] == 100.0

    def test_tier_b1_safe(self):
        result = compute_tiers(self._make_weeks())
        assert result["B1"]["safe_count"] == 0
        assert result["B1"]["safe_rate"] == 0.0

    def test_tier_otm_values(self):
        result = compute_tiers(self._make_weeks())
        assert result["A"]["otm"] == 8
        assert result["B1"]["otm"] == 8

    def test_pending_excluded_from_settled(self):
        weeks = [
            {"tier": "A", "pending": True,  "safe_expiry": None},
            {"tier": "A", "pending": False, "safe_expiry": True},
        ]
        result = compute_tiers(weeks)
        assert result["A"]["count"] == 2
        assert result["A"]["settled"] == 1
        assert result["A"]["safe_count"] == 1

    def test_empty_weeks(self):
        assert compute_tiers([]) == {}


# ---------------------------------------------------------------------------
# compute_latest
# ---------------------------------------------------------------------------
class TestComputeLatest:
    """最近一周决策详情"""

    @staticmethod
    def _make_inputs(tier_close: float = 100.0, ma20: float = 95.0,
                     ma60: float = 85.0, macd: float = -3.0,
                     prev_macd: float = -5.0):
        weekly_rows = [
            dict(
                date=datetime.date(2026, 3, 2),
                close=tier_close,
                macd=macd,
                prev_macd=prev_macd,
                pivot_5_pp=90.0,
                pivot_30_pp=90.0,
                ma20=ma20,
                ma60=ma60,
                dif=1.0,
                hist_vol=60.0,
            )
        ]
        dates = pd.date_range("2026-03-02", "2026-04-30", freq="B")
        daily_df = pd.DataFrame({
            "date": dates,
            "close": [tier_close + i * 0.1 for i in range(len(dates))],
        })
        return weekly_rows, daily_df

    def test_required_keys(self):
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        expected_keys = {
            "date", "close", "tier", "rules",
            "strikes", "otm", "expiry_date",
        }
        assert expected_keys.issubset(set(result.keys()))

    def test_rules_keys(self):
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        rule_keys = {
            "macd_today", "macd_yesterday", "macd_narrow",
            "p5_pp", "above_p5", "p30_pp", "above_p30",
            "ma20", "ma60", "dif", "hist_vol", "ma20_dist",
            "above_ma60",
        }
        assert rule_keys == set(result["rules"].keys())

    def test_tier_a_assigned(self):
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        assert result["tier"] == "A"

    def test_strikes_dict(self):
        weekly_rows, daily_df = self._make_inputs(tier_close=100.0)
        result = compute_latest(weekly_rows, daily_df)
        assert "strikes" in result
        assert result["strikes"]["A"] == pytest.approx(92.0, abs=0.01)   # 8% OTM
        assert result["strikes"]["C2"] == pytest.approx(85.0, abs=0.01)  # 15% OTM

    def test_expiry_date_is_trading_day(self):
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        assert isinstance(result["expiry_date"], str)
        expiry = datetime.date.fromisoformat(result["expiry_date"])
        assert expiry.weekday() < 5

    def test_date_is_string(self):
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        assert isinstance(result["date"], str)

    def test_empty_weekly_rows(self):
        dates = pd.date_range("2026-03-02", "2026-04-30", freq="B")
        daily_df = pd.DataFrame({"date": dates, "close": [100.0] * len(dates)})
        result = compute_latest([], daily_df)
        assert result == {}

    def test_tier_b1_assigned(self):
        """close < MA20 且 close > MA60 -> B1"""
        weekly_rows, daily_df = self._make_inputs(
            tier_close=95.0, ma20=100.0, ma60=85.0,
            macd=-10.0, prev_macd=-5.0,
        )
        result = compute_latest(weekly_rows, daily_df)
        assert result["tier"] == "B1"
        assert result["strikes"]["A"] == pytest.approx(95.0 * 0.92, abs=0.01)
        assert result["strikes"]["B1"] == pytest.approx(95.0 * 0.92, abs=0.01)
