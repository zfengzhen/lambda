"""策略核心模块单元测试：周分组、分层判定、BS 期权费、回测计算"""
import datetime
import math
import sys
from pathlib import Path

# lambda-strategy 目录含连字符，无法作为 Python 包名直接 import，
# 将其加入 sys.path 后按模块名导入
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from strategy import (
    group_by_week,
    classify_tier,
    compute_hist_vol,
    find_expiry_friday,
    backtest_weeks,
    compute_summary,
    compute_tiers,
    compute_latest,
    get_otm_for_ticker,
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
    """分层决策树——每个层级各一条测试"""

    @staticmethod
    def _base_row(**overrides):
        """构造默认 row，所有值设为不触发任何规则"""
        row = dict(
            close=100, macd=-5, prev_macd=-10,
            pivot_5_pp=80, pivot_30_pp=80,
            ma20=100, ma60=90, dif=1, hist_vol=70,
        )
        row.update(overrides)
        return row

    def test_tier_a(self):
        """A 企稳双撑: |MACD_today| < |MACD_yesterday| AND close > P5_PP AND close > P30_PP"""
        row = self._base_row(macd=-3, prev_macd=-5,
                             pivot_5_pp=90, pivot_30_pp=90, close=100)
        assert classify_tier(row) == "A"

    def test_tier_b1(self):
        """B1 回调均线: close < MA20 AND close > MA60"""
        row = self._base_row(close=95, ma20=100, ma60=90,
                             # 确保 A 不命中：|macd| >= |prev_macd|
                             macd=-10, prev_macd=-5)
        assert classify_tier(row) == "B1"

    def test_tier_b2(self):
        """B2 低波整理: hist_vol < 50 AND |MA20距离| <= 5%"""
        row = self._base_row(close=100, ma20=102, hist_vol=40,
                             macd=-10, prev_macd=-5,
                             ma60=110)  # close < ma60 → 排除 B1（B1 要求 close > ma60）
        assert classify_tier(row) == "B2"

    def test_tier_b3(self):
        """B3 超跌支撑: DIF < 0 AND close > P30_PP"""
        row = self._base_row(close=100, dif=-2, pivot_30_pp=90,
                             macd=-10, prev_macd=-5,
                             ma20=90, ma60=80,  # close > ma20 → 排除 B1
                             hist_vol=70)        # hist_vol >= 50 → 排除 B2
        assert classify_tier(row) == "B3"

    def test_tier_b4(self):
        """B4 趋势动能弱: MA20 > MA60 AND DIF < 0"""
        row = self._base_row(close=80, dif=-2, ma20=110, ma60=90,
                             macd=-10, prev_macd=-5,
                             pivot_30_pp=120,  # close < P30_PP → 排除 B3
                             hist_vol=70)      # close < ma60 → 排除 B1
        assert classify_tier(row) == "B4"

    def test_tier_c(self):
        """C skip: 不满足任何条件"""
        row = self._base_row(close=100, dif=5, ma20=80, ma60=120,
                             macd=-10, prev_macd=-5,
                             pivot_5_pp=120, pivot_30_pp=120,
                             hist_vol=70)
        assert classify_tier(row) == "C"


# ---------------------------------------------------------------------------
# compute_hist_vol
# ---------------------------------------------------------------------------
class TestComputeHistVol:
    """历史波动率计算"""

    def test_constant_price(self):
        """恒定价格序列 → 波动率为 0"""
        closes = pd.Series([100.0] * 25)
        assert compute_hist_vol(closes, window=20) == 0.0

    def test_positive_vol(self):
        """有波动的序列 → 正值"""
        closes = pd.Series([100, 102, 99, 103, 97, 101, 98, 104,
                            96, 100, 105, 95, 102, 98, 103, 97,
                            101, 99, 104, 96, 100])
        vol = compute_hist_vol(closes, window=20)
        assert vol > 0


# ---------------------------------------------------------------------------
# find_expiry_friday
# ---------------------------------------------------------------------------
class TestFindExpiryFriday:
    """从 entry_date 找 N 周后的周五"""

    def test_monday_entry(self):
        """周一入场，3 整周后周五"""
        entry = datetime.date(2026, 1, 5)   # 周一
        friday = find_expiry_friday(entry, weeks=3)
        assert friday == datetime.date(2026, 1, 30)  # 01-05 + 25天
        assert friday.weekday() == 4  # 周五

    def test_wednesday_entry(self):
        """周三入场，仍以该周一起算"""
        entry = datetime.date(2026, 1, 7)   # 周三
        friday = find_expiry_friday(entry, weeks=3)
        assert friday == datetime.date(2026, 1, 30)  # 同周一入场结果一致
        assert friday.weekday() == 4


# ---------------------------------------------------------------------------
# backtest_weeks
# ---------------------------------------------------------------------------
class TestBacktestWeeks:
    """小场景回测：3 周数据"""

    @pytest.fixture()
    def scenario(self):
        """构造 3 周的 weekly_rows 和对应 daily_df"""
        # 三周各一条 weekly row（模拟 group_by_week 输出）
        weekly_rows = [
            dict(date=datetime.date(2026, 1, 5), close=100, macd=-3, prev_macd=-5,
                 pivot_5_pp=90, pivot_30_pp=90, ma20=95, ma60=85, dif=1, hist_vol=60),
            dict(date=datetime.date(2026, 1, 12), close=95, macd=-10, prev_macd=-5,
                 pivot_5_pp=120, pivot_30_pp=120, ma20=100, ma60=90, dif=2, hist_vol=60),
            dict(date=datetime.date(2026, 1, 19), close=90, macd=-10, prev_macd=-5,
                 pivot_5_pp=120, pivot_30_pp=120, ma20=100, ma60=90, dif=-2, hist_vol=70),
        ]

        # daily_df 需要覆盖到到期日（约 3 周后）
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
        """结果按时间倒序排列（date 为字符串）"""
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
            assert r["tier"] in ("A", "B1", "B2", "B3", "B4", "C")

    def test_backtest_portions_rotate(self, scenario):
        """份额按 1-4 整数循环"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        # 正序检查份额轮转
        forward = list(reversed(result))
        for i, r in enumerate(forward):
            expected = (i % 4) + 1
            assert r["portion"] == expected
            assert isinstance(r["portion"], int)

    def test_backtest_new_fields(self, scenario):
        """回测结果包含 settle_diff / safe_expiry / pct_change / period_low / low_vs_strike"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            assert "pct_change" in r
            assert "period_low" in r
            assert "low_vs_strike" in r
            assert "settle_diff" in r
            assert "safe_expiry" in r
            # 旧字段不应存在
            assert "prem_pct" not in r
            assert "pnl_pct" not in r
            assert "cum_pnl" not in r
            assert "premium" not in r
            assert "pnl" not in r

    def test_backtest_otm_is_int(self, scenario):
        """otm 字段为整数（10 或 15）或 None（C 层）"""
        weekly_rows, daily_df = scenario
        result = backtest_weeks(weekly_rows, daily_df)
        for r in result:
            if r["tier"] != "C":
                assert r["otm"] in (10, 15)
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
    """汇总统计：settled / pending / skip 混合场景"""

    @staticmethod
    def _make_weeks():
        """构造 5 条 week dict：A/B1/B2/C 各层均为正式策略分支"""
        return [
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "B1", "pending": False, "safe_expiry": False},
            {"tier": "B2", "pending": True,  "safe_expiry": None},
            {"tier": "C",  "pending": False, "safe_expiry": False},
            {"tier": "C",  "pending": True,  "safe_expiry": None},
        ]

    def test_summary_keys_exist(self):
        """返回字典包含所有预期键"""
        result = compute_summary(self._make_weeks())
        expected_keys = {
            "total_weeks", "sell_count", "settled", "pending",
            "safe_count", "safe_rate",
        }
        assert expected_keys == set(result.keys())

    def test_total_weeks(self):
        result = compute_summary(self._make_weeks())
        assert result["total_weeks"] == 5

    def test_sell_count(self):
        result = compute_summary(self._make_weeks())
        assert result["sell_count"] == 5

    def test_settled_and_pending(self):
        result = compute_summary(self._make_weeks())
        assert result["settled"] == 3
        assert result["pending"] == 2

    def test_safe_count(self):
        """safe_count = 已结算中 safe_expiry=True 的数量"""
        result = compute_summary(self._make_weeks())
        assert result["safe_count"] == 1

    def test_safe_rate(self):
        """safe_rate = safe_count / settled * 100"""
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
    """按层级统计：只统计出现过的层级，C skip 不出现"""

    @staticmethod
    def _make_weeks():
        """构造含 A*2、B1*1、C*1 的 week 列表"""
        return [
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "A",  "pending": False, "safe_expiry": True},
            {"tier": "B1", "pending": False, "safe_expiry": False},
            {"tier": "C",  "pending": False, "safe_expiry": None},
        ]

    def test_only_traded_tiers(self):
        result = compute_tiers(self._make_weeks())
        assert set(result.keys()) == {"A", "B1", "C"}

    def test_tier_keys_structure(self):
        result = compute_tiers(self._make_weeks())
        for tier_val in result.values():
            assert {"name", "otm", "count", "settled", "safe_count", "safe_rate"} == set(tier_val.keys())

    def test_tier_a_count(self):
        result = compute_tiers(self._make_weeks())
        assert result["A"]["count"] == 2

    def test_tier_a_safe(self):
        """A 层 2/2 平稳到期"""
        result = compute_tiers(self._make_weeks())
        assert result["A"]["safe_count"] == 2
        assert result["A"]["safe_rate"] == 100.0

    def test_tier_b1_safe(self):
        """B1 层 0/1 平稳到期"""
        result = compute_tiers(self._make_weeks())
        assert result["B1"]["safe_count"] == 0
        assert result["B1"]["safe_rate"] == 0.0

    def test_tier_otm_values(self):
        result = compute_tiers(self._make_weeks())
        assert result["A"]["otm"] == 10
        assert result["B1"]["otm"] == 15

    def test_pending_excluded_from_settled(self):
        """pending 周不计入 settled"""
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
        """
        构造单条 weekly_rows 及覆盖到期日的 daily_df。
        默认参数使 tier = A（|macd| < |prev_macd| 且 close > P5_PP/P30_PP）。
        """
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
        """返回字典含所有必要顶层键"""
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        expected_keys = {
            "date", "close", "tier", "rules",
            "strike_a", "strike_b", "expiry_date",
        }
        assert expected_keys.issubset(set(result.keys()))
        # 旧字段不应存在
        assert "sigma" not in result
        assert "premium" not in result
        assert "otm" not in result
        assert "strike" not in result

    def test_rules_keys(self):
        """rules 子字典含所有判断字段及原始指标值"""
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        rule_keys = {
            "macd_today", "macd_yesterday", "macd_narrow",
            "p5_pp", "above_p5", "p30_pp", "above_p30",
            "ma20", "ma60", "dif", "hist_vol", "ma20_dist",
        }
        assert rule_keys == set(result["rules"].keys())

    def test_tier_a_assigned(self):
        """默认参数命中 A 层"""
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        assert result["tier"] == "A"

    def test_strike_a_and_b(self):
        """strike_a = close * 0.90，strike_b = close * 0.85"""
        weekly_rows, daily_df = self._make_inputs(tier_close=100.0)
        result = compute_latest(weekly_rows, daily_df)
        assert result["strike_a"] == pytest.approx(90.0, abs=0.01)
        assert result["strike_b"] == pytest.approx(85.0, abs=0.01)

    def test_expiry_date_is_friday_string(self):
        """到期日为字符串格式的周五"""
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        assert isinstance(result["expiry_date"], str)
        expiry = datetime.date.fromisoformat(result["expiry_date"])
        assert expiry.weekday() == 4

    def test_date_is_string(self):
        """date 为字符串"""
        weekly_rows, daily_df = self._make_inputs()
        result = compute_latest(weekly_rows, daily_df)
        assert isinstance(result["date"], str)

    def test_empty_weekly_rows(self):
        """空 weekly_rows 返回空字典"""
        dates = pd.date_range("2026-03-02", "2026-04-30", freq="B")
        daily_df = pd.DataFrame({"date": dates, "close": [100.0] * len(dates)})
        result = compute_latest([], daily_df)
        assert result == {}

    def test_tier_b1_assigned(self):
        """close < MA20 且 close > MA60 → B1"""
        # 排除 A：|macd| >= |prev_macd|
        weekly_rows, daily_df = self._make_inputs(
            tier_close=95.0, ma20=100.0, ma60=85.0,
            macd=-10.0, prev_macd=-5.0,
        )
        result = compute_latest(weekly_rows, daily_df)
        assert result["tier"] == "B1"
        # strike_a 和 strike_b 始终存在
        assert result["strike_a"] == pytest.approx(95.0 * 0.90, abs=0.01)
        assert result["strike_b"] == pytest.approx(95.0 * 0.85, abs=0.01)


# ---------------------------------------------------------------------------
# get_otm_for_ticker
# ---------------------------------------------------------------------------
class TestGetOtmForTicker:
    """OTM 按杠杆倍数自动推导"""

    def test_3x_tqqq(self):
        """TQQQ 3倍杠杆 → A=10%, B=15%, C=25%（基准值不变）"""
        a, b, c = get_otm_for_ticker("TQQQ")
        assert a == 0.10
        assert b == 0.15
        assert c == 0.25

    def test_3x_soxl(self):
        """SOXL 3倍杠杆 → 同 TQQQ"""
        a, b, c = get_otm_for_ticker("SOXL")
        assert a == 0.10
        assert b == 0.15
        assert c == 0.25

    def test_2x_qld(self):
        """QLD 2倍杠杆 → A=6%, B=10%, C=16%"""
        a, b, c = get_otm_for_ticker("QLD")
        assert a == 0.06
        assert b == 0.10
        assert c == 0.16

    def test_1x_qqq(self):
        """QQQ 普通股票 → A=3%, B=5%, C=8%"""
        a, b, c = get_otm_for_ticker("QQQ")
        assert a == 0.03
        assert b == 0.05
        assert c == 0.08

    def test_unknown_ticker(self):
        """未知标的默认 1 倍"""
        a, b, c = get_otm_for_ticker("AAPL")
        assert a == 0.03
        assert b == 0.05
        assert c == 0.08
