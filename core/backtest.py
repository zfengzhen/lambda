"""回测引擎：逐周回测、期权 enrichment、汇总统计。"""
from datetime import datetime

import pandas as pd

from config import DEFAULT_OTM, ALL_TIERS, TIER_NAMES, TICKER
from core.strategy import classify_tier, extract_rules, find_expiry_date


def backtest_weeks(weekly_rows: list[dict],
                   daily_df: pd.DataFrame) -> list[dict]:
    """逐周回测：分层 → 定行权价 → 找到期日价格 → 判断是否平稳到期。

    weekly_rows: group_by_week 输出（正序）
    daily_df: 日线数据，含 date / close 列

    返回倒序（最新一周在前）的 list[dict]。
    """
    daily = daily_df.copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    last_data_date = daily["date"].max()

    results = []
    for row in weekly_rows:
        tier = classify_tier(row)
        entry_date = row["date"]
        close = row["close"]
        otm_frac = DEFAULT_OTM.get(tier, 0.10)
        otm_pct = int(otm_frac * 100)
        strike = round(close * (1 - otm_frac), 2)
        rules = extract_rules(row)
        expiry_date = find_expiry_date(entry_date)

        # 查到期日收盘价
        pending = False
        expiry_close = None
        settle_diff = None
        safe_expiry = None

        if expiry_date > last_data_date:
            pending = True
        else:
            expiry_row = daily[daily["date"] == expiry_date]
            if expiry_row.empty:
                before = daily[daily["date"] <= expiry_date].sort_values("date")
                if not before.empty:
                    expiry_close = float(before.iloc[-1]["close"])
                else:
                    pending = True
            else:
                expiry_close = float(expiry_row.iloc[0]["close"])

        if not pending and expiry_close is not None:
            # 结算差比：(到期价 - 行权价) / 行权价 × 100%
            settle_diff = round((expiry_close - strike) / strike * 100, 2)
            safe_expiry = settle_diff > 0

        # 未平稳到期时，计算恢复天数
        recovery_days = None
        recovery_gap = None
        if safe_expiry is False:
            after = daily[daily["date"] > expiry_date].sort_values("date")
            recovered = after[after["close"] > strike]
            if not recovered.empty:
                recovery_date = recovered.iloc[0]["date"]
                recovery_days = (recovery_date - expiry_date).days
            else:
                latest_close = float(daily.iloc[-1]["close"])
                recovery_gap = round((latest_close - strike) / strike * 100, 1)

        results.append({
            "date": str(entry_date),
            "close": close,
            "tier": tier,
            "rules": rules,
            "otm": otm_pct,
            "strike": strike,
            "expiry_date": str(expiry_date),
            "expiry_close": expiry_close,
            "settle_diff": settle_diff,
            "safe_expiry": safe_expiry,
            "recovery_days": recovery_days,
            "recovery_gap": recovery_gap,
            "pending": pending,
        })

    results.sort(key=lambda r: r["date"], reverse=True)
    return results


def enrich_with_options(weeks: list[dict], daily: pd.DataFrame) -> None:
    """为每周回测数据补充期权合约信息，并用 OCC 真实 strike 重算结算指标。"""
    from core.options import match_option_contract

    for w in weeks:
        strike = w.get("strike")
        expiry = w.get("expiry_date")
        entry = w.get("date")
        if not strike or not expiry or not entry:
            continue

        contract = match_option_contract(entry, expiry, strike)
        if contract:
            occ_strike = contract["occ_strike"]
            w["option_symbol"] = contract["display_symbol"]
            w["option_strike"] = occ_strike
            w["option_dte"] = contract["dte"]
            w["option_price"] = contract["price"]
            w["option_vwap"] = contract["vwap"]
            # 用合约真实 strike 重算结算差比和平稳到期
            if w.get("expiry_close") is not None and occ_strike > 0:
                w["settle_diff"] = round(
                    (w["expiry_close"] - occ_strike) / occ_strike * 100, 2)
                w["safe_expiry"] = w["settle_diff"] > 0
                if w["safe_expiry"]:
                    w["recovery_days"] = None
                    w["recovery_gap"] = None
                else:
                    expiry_str = w["expiry_date"]
                    after = daily[daily["date"] > expiry_str].sort_values("date")
                    recovered = after[after["close"] > occ_strike]
                    if not recovered.empty:
                        rec_date_str = str(recovered.iloc[0]["date"])
                        delta = (datetime.strptime(rec_date_str, "%Y-%m-%d")
                                 - datetime.strptime(expiry_str, "%Y-%m-%d"))
                        w["recovery_days"] = delta.days
                        w["recovery_gap"] = None
                    else:
                        w["recovery_days"] = None
                        latest_close = float(daily.iloc[-1]["close"])
                        w["recovery_gap"] = round(
                            (latest_close - occ_strike) / occ_strike * 100, 1)
        else:
            w["option_symbol"] = None
            w["option_strike"] = None
            w["option_dte"] = None
            w["option_price"] = None
            w["option_vwap"] = None


def compute_summary(weeks: list[dict]) -> dict:
    """汇总统计。"""
    settled = [w for w in weeks if not w["pending"]]
    safe_count = sum(1 for w in settled if w.get("safe_expiry") is True)
    safe_rate = round(safe_count / len(settled) * 100, 1) if settled else 0.0
    return {
        "total_weeks": len(weeks),
        "settled": len(settled),
        "pending": len(weeks) - len(settled),
        "safe_count": safe_count,
        "safe_rate": safe_rate,
    }


def compute_tiers(weeks: list[dict]) -> dict:
    """按层级统计，包含平稳到期比例。"""
    result = {}
    for tier_key in ALL_TIERS:
        items = [w for w in weeks if w["tier"] == tier_key]
        if not items:
            continue
        settled = [w for w in items if not w["pending"]]
        safe_count = sum(1 for w in settled if w.get("safe_expiry") is True)
        safe_rate = round(safe_count / len(settled) * 100, 1) if settled else 0.0
        result[tier_key] = {
            "name": TIER_NAMES[tier_key],
            "otm": int(DEFAULT_OTM.get(tier_key, 0.10) * 100),
            "count": len(items),
            "settled": len(settled),
            "safe_count": safe_count,
            "safe_rate": safe_rate,
        }
    return result


def compute_latest(weekly_rows: list[dict],
                   daily_df: pd.DataFrame) -> dict:
    """最近一周的完整决策详情。"""
    if not weekly_rows:
        return {}

    row = weekly_rows[-1]
    tier = classify_tier(row)
    close = row["close"]
    rules = extract_rules(row)
    strikes = {t: round(close * (1 - o), 2) for t, o in DEFAULT_OTM.items()}
    expiry_date = find_expiry_date(row["date"])
    otm_frac = DEFAULT_OTM.get(tier, 0.10)

    result = {
        "date": str(row["date"]),
        "close": close,
        "tier": tier,
        "rules": rules,
        "otm": int(otm_frac * 100),
        "strikes": strikes,
        "expiry_date": str(expiry_date),
    }

    # 查询当周期权合约
    from core.options import match_option_contract
    lt_strike = strikes.get(tier)
    contract = match_option_contract(
        result["date"], result["expiry_date"], lt_strike or 0)
    if contract:
        result["option_symbol"] = contract["display_symbol"]
        result["option_dte"] = contract["dte"]
        result["option_price"] = contract["price"]
        result["option_strike"] = contract["occ_strike"]
        result["option_expiry"] = contract["occ_expiry"]

    return result
