import pytest
from unittest.mock import patch
from entry_optimizer import enrich_with_option_data, sweep_k, find_optimal_k

SAMPLE_TRADE = {
    "week_start": "2025-01-06",   # Monday
    "layer": "A",
    "mon_close": 42.84,
    "strike": 38.5,
    "expiry": "2025-01-31",
    "otm_pct": 0.10,
}

# date 字段已由 fetch_option_bars 解析好
MOCK_BARS = [
    {"date": "2025-01-06", "open": 0.85, "high": 0.92, "low": 0.80, "close": 0.87},  # Mon
    {"date": "2025-01-07", "open": 0.86, "high": 0.95, "low": 0.82, "close": 0.90},  # Tue
    {"date": "2025-01-08", "open": 0.88, "high": 0.93, "low": 0.83, "close": 0.89},  # Wed
    {"date": "2025-01-09", "open": 0.87, "high": 0.91, "low": 0.81, "close": 0.88},  # Thu
    {"date": "2025-01-10", "open": 0.85, "high": 0.89, "low": 0.79, "close": 0.85},  # Fri
]


class TestEnrichWithOptionData:
    def test_mon_close_option_extracted(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["mon_close_option"] == pytest.approx(0.87)

    def test_day_highs_extracted(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["tue_high"] == pytest.approx(0.95)
        assert t["wed_high"] == pytest.approx(0.93)
        assert t["thu_high"] == pytest.approx(0.91)
        assert t["fri_high"] == pytest.approx(0.89)

    def test_week_high_is_max_of_tue_to_fri(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["week_high"] == pytest.approx(0.95)  # max(0.95, 0.93, 0.91, 0.89)

    def test_data_complete_true_when_all_days_present(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["data_complete"] is True

    def test_data_complete_false_when_monday_missing(self):
        bars_no_mon = [b for b in MOCK_BARS if b["date"] != "2025-01-06"]
        with patch("entry_optimizer.fetch_option_bars", return_value=bars_no_mon):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["mon_close_option"] is None
        assert t["data_complete"] is False

    def test_data_complete_false_when_empty_bars(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=[]):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        assert result[0]["data_complete"] is False

    def test_tolerates_one_missing_weekday(self):
        # 只缺周五，仍有 3 天数据（周二/三/四），应视为 complete
        bars_no_fri = [b for b in MOCK_BARS if b["date"] != "2025-01-10"]
        with patch("entry_optimizer.fetch_option_bars", return_value=bars_no_fri):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["fri_high"] is None
        assert t["data_complete"] is True  # 仍有 3 天 >= 3

    def test_option_symbol_is_correct_occ(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        # TQQQ put 行权价 38.5 到期 2025-01-31
        assert result[0]["option_symbol"] == "O:TQQQ250131P00038500"

    def test_preserves_original_trade_fields(self):
        with patch("entry_optimizer.fetch_option_bars", return_value=MOCK_BARS):
            result = enrich_with_option_data([SAMPLE_TRADE], api_key="key")
        t = result[0]
        assert t["week_start"] == "2025-01-06"
        assert t["layer"] == "A"
        assert t["strike"] == pytest.approx(38.5)


# 参考价 0.87，整周最高 0.95
COMPLETE_TRADE = {
    "week_start": "2025-01-06", "layer": "A",
    "mon_close": 42.84, "strike": 38.5,
    "expiry": "2025-01-31", "otm_pct": 0.10,
    "option_symbol": "O:TQQQ250131P00038500",
    "mon_close_option": 0.87, "week_high": 0.95,
    "data_complete": True,
}

INCOMPLETE_TRADE = {**COMPLETE_TRADE, "data_complete": False}


class TestSweepK:
    def test_all_fill_at_low_k(self):
        # limit = 0.87 × 0.5 = 0.435 < 0.95 → 成交
        results = sweep_k([COMPLETE_TRADE], k_min=0.5, k_max=0.5, k_step=0.1)
        assert results[0]["fill_count"] == 1
        assert results[0]["total_premium"] == pytest.approx(0.87 * 0.5, rel=1e-4)

    def test_none_fill_at_high_k(self):
        # limit = 0.87 × 2.0 = 1.74 > 0.95 → 不成交
        results = sweep_k([COMPLETE_TRADE], k_min=2.0, k_max=2.0, k_step=0.1)
        assert results[0]["fill_count"] == 0
        assert results[0]["total_premium"] == pytest.approx(0.0)

    def test_fill_rate_at_market_price(self):
        # limit = 0.87 × 1.0 = 0.87 ≤ 0.95 → 成交，fill_rate = 100%
        results = sweep_k([COMPLETE_TRADE], k_min=1.0, k_max=1.0, k_step=0.1)
        assert results[0]["fill_rate"] == pytest.approx(1.0)

    def test_skips_incomplete_trades(self):
        results = sweep_k([INCOMPLETE_TRADE, COMPLETE_TRADE],
                          k_min=1.0, k_max=1.0, k_step=0.1)
        # 只有 COMPLETE_TRADE 计入，fill_rate = 1/1
        assert results[0]["fill_rate"] == pytest.approx(1.0)
        assert results[0]["fill_count"] == 1

    def test_premium_at_boundary_k(self):
        # limit = 0.87 × (0.95/0.87) ≈ 0.95 = week_high → 刚好成交
        k_boundary = round(0.95 / 0.87, 10)
        results = sweep_k([COMPLETE_TRADE], k_min=k_boundary,
                          k_max=k_boundary, k_step=0.1)
        assert results[0]["fill_count"] == 1

    def test_returns_empty_for_all_incomplete(self):
        results = sweep_k([INCOMPLETE_TRADE])
        assert results == []

    def test_result_structure(self):
        results = sweep_k([COMPLETE_TRADE], k_min=1.0, k_max=1.0, k_step=0.1)
        r = results[0]
        assert set(r.keys()) >= {"k", "total_premium", "fill_count", "fill_rate"}


class TestFindOptimalK:
    def test_selects_max_total_premium(self):
        sweep = [
            {"k": 1.0, "total_premium": 0.87, "fill_count": 1, "fill_rate": 1.0},
            {"k": 1.1, "total_premium": 0.96, "fill_count": 1, "fill_rate": 1.0},
            {"k": 1.5, "total_premium": 0.50, "fill_count": 1, "fill_rate": 0.5},
        ]
        best = find_optimal_k(sweep)
        assert best["k"] == pytest.approx(1.1)

    def test_raises_on_empty(self):
        with pytest.raises(ValueError, match="sweep_results 为空"):
            find_optimal_k([])
