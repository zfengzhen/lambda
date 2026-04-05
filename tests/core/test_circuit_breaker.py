# tests/core/test_circuit_breaker.py
"""core.circuit_breaker 单元测试：连续弱势熔断。"""
from core.circuit_breaker import apply_circuit_breaker


def _make_week(date: str, tier: str) -> dict:
    """构造最小化 week dict。"""
    return {"date": date, "tier": tier}


class TestApplyCircuitBreaker:
    """熔断机制测试"""

    def test_no_skip_when_less_than_3_weeks(self):
        """不足 3 周数据时不触发熔断"""
        weeks = [_make_week("2026-01-05", "C4"),
                 _make_week("2026-01-12", "C4")]
        apply_circuit_breaker(weeks)
        for w in weeks:
            assert w["skip"] is False

    def test_a_tier_never_skipped(self):
        """A 类分层始终放行"""
        weeks = [_make_week("2026-01-05", "C4"),
                 _make_week("2026-01-12", "C4"),
                 _make_week("2026-01-19", "A")]
        apply_circuit_breaker(weeks)
        assert weeks[2]["skip"] is False

    def test_b_tier_never_skipped(self):
        """B 类分层始终放行"""
        weeks = [_make_week("2026-01-05", "C3"),
                 _make_week("2026-01-12", "C4"),
                 _make_week("2026-01-19", "B1")]
        apply_circuit_breaker(weeks)
        assert weeks[2]["skip"] is False

    def test_three_consecutive_c_triggers_skip(self):
        """连续 3 周 C 类触发熔断"""
        weeks = [_make_week("2026-01-05", "C3"),
                 _make_week("2026-01-12", "C4"),
                 _make_week("2026-01-19", "C2")]
        apply_circuit_breaker(weeks)
        w = next(w for w in weeks if w["date"] == "2026-01-19")
        assert w["skip"] is True
        assert "skip_reason" in w

    def test_c1_with_prior_c1_continues(self):
        """本周 C1 且前 2 周含 C1 -> 继续卖出（减速信号）"""
        weeks = [_make_week("2026-01-05", "C1"),
                 _make_week("2026-01-12", "C4"),
                 _make_week("2026-01-19", "C1")]
        apply_circuit_breaker(weeks)
        w = next(w for w in weeks if w["date"] == "2026-01-19")
        assert w["skip"] is False

    def test_c1_without_prior_c1_skips(self):
        """本周 C1 但前 2 周无 C1 -> 暂停"""
        weeks = [_make_week("2026-01-05", "C3"),
                 _make_week("2026-01-12", "C4"),
                 _make_week("2026-01-19", "C1")]
        apply_circuit_breaker(weeks)
        w = next(w for w in weeks if w["date"] == "2026-01-19")
        assert w["skip"] is True

    def test_mixed_tiers_no_skip(self):
        """中间有非 C 类打断连续，不触发"""
        weeks = [_make_week("2026-01-05", "C4"),
                 _make_week("2026-01-12", "A"),
                 _make_week("2026-01-19", "C4")]
        apply_circuit_breaker(weeks)
        w = next(w for w in weeks if w["date"] == "2026-01-19")
        assert w["skip"] is False
