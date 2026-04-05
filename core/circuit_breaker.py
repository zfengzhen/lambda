"""熔断机制：连续弱势暂停卖出。"""

_C_TIERS = {"C1", "C2", "C3", "C4"}


def apply_circuit_breaker(weeks: list[dict]) -> None:
    """检测连续 C 类分层，标记 skip=True 暂停卖出。

    规则：
    - 前 2 周都是 C 类 + 本周也是 C 类时才可能暂停（A/B 类始终放行）
    - 本周 C1 且前 2 周含 C1 → 继续卖出（跌势已有减速信号）
    - 本周 C1 但前 2 周无 C1 → 暂停（纯下杀后首次减速，不够安全）
    - 其余 C 类连续 3 周 → 暂停

    就地修改 weeks，添加 skip / skip_reason 字段。
    """
    weeks_asc = sorted(weeks, key=lambda w: w["date"])
    for i, w in enumerate(weeks_asc):
        if i >= 2:
            p1 = weeks_asc[i - 1]["tier"]
            p2 = weeks_asc[i - 2]["tier"]
            if p1 in _C_TIERS and p2 in _C_TIERS and w["tier"] in _C_TIERS:
                if w["tier"] == "C1" and (p1 == "C1" or p2 == "C1"):
                    w["skip"] = False
                else:
                    w["skip"] = True
                    w["skip_reason"] = f"前2周 {p2}→{p1}，本周 {w['tier']}，连续弱势暂停"
                continue
        w["skip"] = False
