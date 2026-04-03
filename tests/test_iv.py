# tests/test_iv.py
import pytest
from iv import parse_occ_symbol


class TestParseOccSymbol:
    def test_standard_put(self):
        result = parse_occ_symbol("O:TQQQ260424P00030000")
        assert result == {
            "ticker": "TQQQ",
            "expiration": "2026-04-24",
            "option_type": "P",
            "strike": 30.0,
        }

    def test_standard_call(self):
        result = parse_occ_symbol("O:QQQ260515C00450000")
        assert result == {
            "ticker": "QQQ",
            "expiration": "2026-05-15",
            "option_type": "C",
            "strike": 450.0,
        }

    def test_fractional_strike(self):
        """strike 38.5 → 00038500"""
        result = parse_occ_symbol("O:TQQQ250131P00038500")
        assert result == {
            "ticker": "TQQQ",
            "expiration": "2025-01-31",
            "option_type": "P",
            "strike": 38.5,
        }

    def test_adjusted_symbol_after_split(self):
        """拆股调整后 strike 19.25 → 00019250"""
        result = parse_occ_symbol("O:TQQQ250131P00019250")
        assert result == {
            "ticker": "TQQQ",
            "expiration": "2025-01-31",
            "option_type": "P",
            "strike": 19.25,
        }

    def test_single_char_ticker(self):
        """单字母 ticker（如 F）"""
        result = parse_occ_symbol("O:F260320C00012000")
        assert result == {
            "ticker": "F",
            "expiration": "2026-03-20",
            "option_type": "C",
            "strike": 12.0,
        }

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_occ_symbol("INVALID")

    def test_missing_prefix_raises(self):
        with pytest.raises(ValueError):
            parse_occ_symbol("TQQQ260424P00030000")
