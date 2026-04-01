import pytest
from unittest.mock import patch
from entry_optimizer import enrich_with_option_data

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
