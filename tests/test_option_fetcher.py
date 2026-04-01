import pytest
from unittest.mock import patch, MagicMock
from option_fetcher import (
    round_to_strike_increment,
    build_occ_symbol,
    fetch_option_bars,
    get_signal_trades,
)


class TestRoundToStrikeIncrement:
    def test_rounds_down(self):
        assert round_to_strike_increment(38.56) == 38.5

    def test_rounds_up(self):
        assert round_to_strike_increment(38.76) == 39.0

    def test_exact_value_unchanged(self):
        assert round_to_strike_increment(50.0) == 50.0

    def test_custom_increment(self):
        assert round_to_strike_increment(38.7, increment=1.0) == 39.0

    def test_midpoint_rounds_to_nearest(self):
        # 38.75 / 0.5 = 77.5 → rounds to 78 → 39.0
        assert round_to_strike_increment(38.75) == 39.0


class TestBuildOccSymbol:
    def test_basic_put(self):
        assert build_occ_symbol("TQQQ", "2025-01-31", 38.5) == "O:TQQQ250131P00038500"

    def test_large_strike(self):
        assert build_occ_symbol("TQQQ", "2025-04-18", 50.0) == "O:TQQQ250418P00050000"

    def test_call_type(self):
        assert build_occ_symbol("TQQQ", "2025-01-31", 50.0, contract_type="C") == "O:TQQQ250131C00050000"

    def test_fractional_strike_preserved(self):
        # 构建时不做圆整，原样转为整数千分位
        assert build_occ_symbol("TQQQ", "2025-01-31", 49.88) == "O:TQQQ250131P00049880"

    def test_strike_padded_to_8_digits(self):
        # 行权价 5.0 → 5000 → 00005000
        assert build_occ_symbol("TQQQ", "2025-01-31", 5.0) == "O:TQQQ250131P00005000"


# 模拟 API 响应（时间戳对应 2025-01-06 ~ 2025-01-07 UTC 零点）
OPTION_BAR_MON = {"t": 1736121600000, "o": 0.85, "h": 0.92, "l": 0.80, "c": 0.87}
OPTION_BAR_TUE = {"t": 1736208000000, "o": 0.86, "h": 0.95, "l": 0.82, "c": 0.90}


def _mock_resp(results, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"results": results, "resultsCount": len(results), "status": "OK"}
    resp.raise_for_status = MagicMock()
    return resp


class TestFetchOptionBars:
    @patch("option_fetcher.requests.get")
    def test_returns_parsed_bars(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON, OPTION_BAR_TUE])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-07", "key")
        assert len(bars) == 2
        assert bars[0]["high"] == 0.92
        assert bars[1]["high"] == 0.95

    @patch("option_fetcher.requests.get")
    def test_date_field_is_string(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        assert isinstance(bars[0]["date"], str)
        assert len(bars[0]["date"]) == 10  # YYYY-MM-DD

    @patch("option_fetcher.requests.get")
    def test_bar_has_required_fields(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        assert set(bars[0].keys()) == {"date", "open", "high", "low", "close"}

    @patch("option_fetcher.requests.get")
    def test_404_returns_empty(self, mock_get):
        resp = MagicMock()
        resp.status_code = 404
        mock_get.return_value = resp
        bars = fetch_option_bars("O:TQQQ250131P99999000", "2025-01-06", "2025-01-10", "key")
        assert bars == []

    @patch("option_fetcher.requests.get")
    def test_empty_results_returns_empty(self, mock_get):
        mock_get.return_value = _mock_resp([])
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-10", "key")
        assert bars == []

    @patch("option_fetcher.time.sleep")
    @patch("option_fetcher.requests.get")
    def test_429_retries_then_succeeds(self, mock_get, mock_sleep):
        resp_429 = MagicMock()
        resp_429.status_code = 429
        mock_get.side_effect = [resp_429, _mock_resp([OPTION_BAR_MON])]
        bars = fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        assert len(bars) == 1
        mock_sleep.assert_called()

    @patch("option_fetcher.requests.get")
    def test_url_contains_occ_symbol(self, mock_get):
        mock_get.return_value = _mock_resp([OPTION_BAR_MON])
        fetch_option_bars("O:TQQQ250131P00038500", "2025-01-06", "2025-01-06", "key")
        call_url = mock_get.call_args[0][0]
        assert "O:TQQQ250131P00038500" in call_url
        assert "/v2/aggs/ticker/" in call_url
        assert "/range/1/day/" in call_url


SAMPLE_WEEKS = [
    {
        "date": "2025-01-06", "tier": "A", "close": 42.84,
        "strike": 38.56, "otm": 10, "expiry_date": "2025-01-31",
        "pending": False, "safe_expiry": True,
    },
    {
        "date": "2025-01-13", "tier": "C", "close": 40.0,
        "strike": 30.0, "otm": 25, "expiry_date": "2025-02-07",
        "pending": False, "safe_expiry": True,
    },
    {
        "date": "2025-01-20", "tier": "B1", "close": 38.0,
        "strike": 32.3, "otm": 15, "expiry_date": "2025-02-14",
        "pending": True, "safe_expiry": None,
    },
    {
        "date": "2024-12-30", "tier": "B3", "close": 55.0,
        "strike": 46.75, "otm": 15, "expiry_date": "2025-01-24",
        "pending": False, "safe_expiry": True,
    },
]


class TestGetSignalTrades:
    def test_filters_out_c_tier(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        tiers = [t["layer"] for t in trades]
        assert "C" not in tiers

    def test_filters_out_pending(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        week_starts = {t["week_start"] for t in trades}
        # The B1 week (2025-01-20) has pending=True and must be excluded
        assert "2025-01-20" not in week_starts

    def test_returns_two_valid_trades(self):
        # A (non-C, non-pending) + B3 (non-C, non-pending) = 2
        trades = get_signal_trades(SAMPLE_WEEKS)
        assert len(trades) == 2

    def test_rounds_strike_to_half_dollar(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        a_trade = next(t for t in trades if t["layer"] == "A")
        # 38.56 → 38.5
        assert a_trade["strike"] == 38.5

    def test_otm_pct_correct(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        a_trade = next(t for t in trades if t["layer"] == "A")
        assert a_trade["otm_pct"] == pytest.approx(0.10)

    def test_sorted_by_date_ascending(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        dates = [t["week_start"] for t in trades]
        assert dates == sorted(dates)

    def test_output_fields(self):
        trades = get_signal_trades(SAMPLE_WEEKS)
        for t in trades:
            assert set(t.keys()) >= {"week_start", "layer", "mon_close",
                                     "strike", "expiry", "otm_pct"}
