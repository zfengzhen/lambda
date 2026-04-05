# tests/core/test_strategy.py
"""core.strategy 单元测试：周分组、分层判定、历史波动率、到期日。"""
import datetime

import pandas as pd
import pytest

from core.strategy import (
    group_by_week,
    classify_tier,
    compute_hist_vol,
    find_expiry_date,
    extract_rules,
)


# ---------------------------------------------------------------------------
# group_by_week
# ---------------------------------------------------------------------------
class TestGroupByWeek:
    """按 ISO 周分组，取每周第一个交易日"""

    def test_basic_grouping(self):
        """连续两周数据，每周取第一个交易日"""
        dates = pd.to_datetime(
            ["2026-03-02", "2026-03-03", "2026-03-04",  # 周一~周三（W10）
             "2026-03-09", "2026-03-10"]                  # 周一~周二（W11）
        )
        df = pd.DataFrame({
            "date": dates,
            "close": [100, 101, 102, 110, 111],
        })
        result = group_by_week(df)
        assert len(result) == 2
        assert result[0]["date"] == datetime.date(2026, 3, 2)
        assert result[0]["close"] == 100
        assert result[1]["date"] == datetime.date(2026, 3, 9)
        assert result[1]["close"] == 110

    def test_midweek_start(self):
        """数据从周三开始，仍归入该周"""
        dates = pd.to_datetime(
            ["2026-03-04", "2026-03-05",  # 周三~周四（W10）
             "2026-03-09"]                 # 周一（W11）
        )
        df = pd.DataFrame({"date": dates, "close": [50, 51, 60]})
        result = group_by_week(df)
        assert len(result) == 2
        assert result[0]["date"] == datetime.date(2026, 3, 4)


# ---------------------------------------------------------------------------
# classify_tier
# ---------------------------------------------------------------------------
class TestClassifyTier:
    """分层决策树"""

    @staticmethod
    def _base_row(**overrides):
        row = dict(
            close=100, macd=-5, prev_macd=-10,
            pivot_5_pp=80, pivot_30_pp=80,
            ma20=100, ma60=90, dif=1, hist_vol=70,
        )
        row.update(overrides)
        return row

    def test_tier_a(self):
        """A 企稳双撑"""
        row = self._base_row(macd=-3, prev_macd=-5,
                             pivot_5_pp=90, pivot_30_pp=90, close=100)
        assert classify_tier(row) == "A"

    def test_tier_b1(self):
        """B1 回调均线"""
        row = self._base_row(close=95, ma20=100, ma60=90,
                             macd=-10, prev_macd=-5)
        assert classify_tier(row) == "B1"

    def test_tier_b2(self):
        """B2 超跌支撑"""
        row = self._base_row(close=100, dif=-2, pivot_30_pp=90,
                             macd=-10, prev_macd=-5,
                             ma20=90, ma60=80,
                             hist_vol=70)
        assert classify_tier(row) == "B2"

    def test_tier_b3(self):
        """B3 趋势动能弱"""
        row = self._base_row(close=80, dif=-2, ma20=110, ma60=90,
                             macd=-10, prev_macd=-5,
                             pivot_30_pp=120,
                             hist_vol=70)
        assert classify_tier(row) == "B3"

    def test_tier_b4(self):
        """B4 低波整理"""
        row = self._base_row(close=100, ma20=102, hist_vol=40,
                             macd=-10, prev_macd=-5,
                             ma60=110)
        assert classify_tier(row) == "B4"

    def test_tier_c1(self):
        """C1 跌势减速"""
        row = self._base_row(close=70, ma20=75, ma60=80,
                             macd=-3, prev_macd=-5,
                             dif=-2, hist_vol=80,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C1"

    def test_tier_c2(self):
        """C2 趋势延续"""
        row = self._base_row(close=105, ma20=100, ma60=90,
                             macd=-10, prev_macd=-5,
                             dif=1, hist_vol=60,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C2"

    def test_tier_c3(self):
        """C3 过热追涨"""
        row = self._base_row(close=115, ma20=100, ma60=90,
                             macd=-10, prev_macd=-5,
                             dif=1, hist_vol=60,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C3"

    def test_tier_c4(self):
        """C4 加速下杀"""
        row = self._base_row(close=70, ma20=75, ma60=80,
                             macd=-10, prev_macd=-5,
                             dif=-2, hist_vol=80,
                             pivot_5_pp=120, pivot_30_pp=120)
        assert classify_tier(row) == "C4"

    def test_tier_c_returns_subtype(self):
        """原 C 兜底现在返回 C1-C4 子类"""
        row = self._base_row(close=100, dif=5, ma20=80, ma60=120,
                             macd=-10, prev_macd=-5,
                             pivot_5_pp=120, pivot_30_pp=120,
                             hist_vol=70)
        result = classify_tier(row)
        assert result.startswith("C"), f"期望 C 子类，实际 {result}"
        assert result in ("C1", "C2", "C3", "C4")


# ---------------------------------------------------------------------------
# extract_rules
# ---------------------------------------------------------------------------
class TestExtractRules:
    """决策规则提取"""

    def test_rules_keys(self):
        row = dict(
            close=100, macd=-3, prev_macd=-5,
            pivot_5_pp=90, pivot_30_pp=90,
            ma20=95, ma60=85, dif=1, hist_vol=60,
        )
        rules = extract_rules(row)
        expected_keys = {
            "macd_today", "macd_yesterday", "macd_narrow",
            "p5_pp", "above_p5", "p30_pp", "above_p30",
            "ma20", "ma60", "dif", "hist_vol", "ma20_dist",
            "above_ma60",
        }
        assert expected_keys == set(rules.keys())


# ---------------------------------------------------------------------------
# compute_hist_vol
# ---------------------------------------------------------------------------
class TestComputeHistVol:
    """历史波动率计算"""

    def test_constant_price(self):
        """恒定价格序列 -> 波动率为 0"""
        closes = pd.Series([100.0] * 25)
        assert compute_hist_vol(closes, window=20) == 0.0

    def test_positive_vol(self):
        """有波动的序列 -> 正值"""
        closes = pd.Series([100, 102, 99, 103, 97, 101, 98, 104,
                            96, 100, 105, 95, 102, 98, 103, 97,
                            101, 99, 104, 96, 100])
        vol = compute_hist_vol(closes, window=20)
        assert vol > 0


# ---------------------------------------------------------------------------
# find_expiry_date（EXPIRY_WEEKS=4，无需 weeks 参数）
# ---------------------------------------------------------------------------
class TestFindExpiryDate:
    """从 entry_date 找 EXPIRY_WEEKS 周后最后一个美股交易日"""

    def test_normal_week(self):
        """普通周（无假日），返回周五"""
        entry = datetime.date(2026, 1, 5)   # 周一
        expiry = find_expiry_date(entry)
        # EXPIRY_WEEKS=4 → 4 周后周五 = 2026-02-06
        assert expiry.weekday() < 5  # 工作日

    def test_wednesday_entry_same_as_monday(self):
        """周三入场，仍以该周一起算"""
        entry_mon = datetime.date(2026, 1, 5)   # 周一
        entry_wed = datetime.date(2026, 1, 7)   # 周三
        expiry_mon = find_expiry_date(entry_mon)
        expiry_wed = find_expiry_date(entry_wed)
        assert expiry_mon == expiry_wed
