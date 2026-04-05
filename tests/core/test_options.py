# tests/core/test_options.py
"""core.options 单元测试：OCC 解析、构建、strike 提取。"""
import pytest

from core.options import (
    parse_occ_symbol,
    build_occ_symbol,
    extract_strike,
    extract_expiry,
    format_strike_str,
)


class TestParseOccSymbol:
    """OCC symbol 解析"""

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
        """strike 38.5 -> 00038500"""
        result = parse_occ_symbol("O:TQQQ250131P00038500")
        assert result["strike"] == 38.5

    def test_single_char_ticker(self):
        """单字母 ticker（如 F）"""
        result = parse_occ_symbol("O:F260320C00012000")
        assert result["ticker"] == "F"
        assert result["strike"] == 12.0

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_occ_symbol("INVALID")

    def test_missing_prefix_raises(self):
        with pytest.raises(ValueError):
            parse_occ_symbol("TQQQ260424P00030000")


class TestBuildOccSymbol:
    """OCC symbol 构建（TQQQ 专用）"""

    def test_basic_put(self):
        result = build_occ_symbol("2026-04-24", 30.0, "P")
        assert result == "O:TQQQ260424P00030000"

    def test_basic_call(self):
        result = build_occ_symbol("2026-05-15", 45.0, "C")
        assert result == "O:TQQQ260515C00045000"

    def test_fractional_strike(self):
        result = build_occ_symbol("2025-01-31", 38.5, "P")
        assert result == "O:TQQQ250131P00038500"

    def test_roundtrip_with_parse(self):
        """构建后解析，strike 和日期应一致"""
        symbol = build_occ_symbol("2026-04-24", 30.0, "P")
        parsed = parse_occ_symbol(symbol)
        assert parsed["strike"] == 30.0
        assert parsed["expiration"] == "2026-04-24"
        assert parsed["option_type"] == "P"


class TestExtractStrike:
    """从 symbol 末 8 位提取 strike"""

    def test_integer_strike(self):
        assert extract_strike("O:TQQQ260424P00030000") == 30.0

    def test_fractional_strike(self):
        assert extract_strike("O:TQQQ250131P00038500") == 38.5

    def test_high_strike(self):
        assert extract_strike("O:QQQ260515C00450000") == 450.0


class TestExtractExpiry:
    """从 symbol 提取到期日"""

    def test_put_expiry(self):
        assert extract_expiry("O:TQQQ260424P00030000") == "2026-04-24"

    def test_call_expiry(self):
        assert extract_expiry("O:QQQ260515C00450000") == "2026-05-15"


class TestFormatStrikeStr:
    """strike 显示格式"""

    def test_integer_strike(self):
        assert format_strike_str(50.0) == "50"

    def test_fractional_strike(self):
        assert format_strike_str(50.5) == "50.5"

    def test_quarter_strike(self):
        assert format_strike_str(19.25) == "19.25"
