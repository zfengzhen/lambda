import pytest
from unittest.mock import patch, MagicMock
from option_fetcher import round_to_strike_increment, build_occ_symbol


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
