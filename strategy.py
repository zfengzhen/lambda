"""
策略核心模块：周分组、分层判定、回测计算

TQQQ Sell Put 策略的核心逻辑，包含：
- 历史波动率计算
- 按 ISO 周分组取首交易日
- 分类决策树 (A / B1-B4 / C1-C4)
- 到期日计算
- 周级回测引擎（结算差比、平稳到期判定）
- 汇总统计
"""
import datetime
import math
import numpy as np
import pandas as pd

# ---- 策略常量 ----
# 基准 OTM（3 倍杠杆标的的默认值）
# A/B1/B2 为 8%，B3/C1 为 12%，B4/C2/C3 为 15%，C4 为 20%
DEFAULT_OTM = {
    "A": 0.08,
    "B1": 0.08, "B2": 0.08, "B3": 0.12, "B4": 0.15,
    "C1": 0.12, "C2": 0.15, "C3": 0.15, "C4": 0.20,
}

# 已知杠杆 ETF 倍数映射；不在此表中的标的默认 1 倍
LEVERAGE_MAP = {
    "TQQQ": 3, "SOXL": 3, "UPRO": 3, "SPXL": 3, "TECL": 3,
    "FNGU": 3, "BULZ": 3, "TNA": 3,
    "QLD": 2, "SSO": 2,
}

# 层级中文名
TIER_NAMES = {
    "A": "企稳双撑",
    "B1": "回调均线", "B2": "超跌支撑", "B3": "趋势动能弱", "B4": "低波整理",
    "C1": "跌势减速", "C2": "趋势延续", "C3": "过热追涨", "C4": "加速下杀",
}

ALL_TIERS = ["A", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4"]


def get_otm_for_ticker(ticker: str) -> dict[str, float]:
    """根据标的杠杆倍数推导各层 OTM。

    基准值为 3 倍杠杆下的 DEFAULT_OTM。
    公式：floor(基准% × leverage / 3) / 100，结果为整数百分比。
    """
    leverage = LEVERAGE_MAP.get(ticker, 1)
    return {
        tier: math.floor(otm * 100 * leverage / 3) / 100
        for tier, otm in DEFAULT_OTM.items()
    }


EXPIRY_WEEKS = 4      # 到期周数
TRADING_DAYS_YEAR = 252

# ---- NYSE 交易日历（懒加载） ----
_nyse_calendar = None

def _get_nyse_calendar():
    global _nyse_calendar
    if _nyse_calendar is None:
        import exchange_calendars as xcals
        _nyse_calendar = xcals.get_calendar("XNYS")
    return _nyse_calendar


def compute_hist_vol(closes: pd.Series, window: int = 20) -> float:
    """
    计算年化历史波动率（百分比）。
    使用最近 window 个交易日的对数收益率标准差 * sqrt(252) * 100。
    """
    if len(closes) < window + 1:
        return 0.0
    log_returns = np.log(closes / closes.shift(1)).dropna()
    recent = log_returns.iloc[-window:]
    std = recent.std(ddof=1)
    if std == 0 or np.isnan(std):
        return 0.0
    return float(std * math.sqrt(TRADING_DAYS_YEAR) * 100)


def group_by_week(df: pd.DataFrame) -> list[dict]:
    """
    按 ISO 年+周分组，取每周第一个交易日的行。
    输入 df 至少有 date、close 列（date 为 datetime 或可解析字符串）。
    返回 list[dict]，date 转为 datetime.date。
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    # ISO 年 + 周号作为分组键
    df["_iso_yw"] = df["date"].apply(lambda d: (d.isocalendar()[0], d.isocalendar()[1]))
    df = df.sort_values("date")

    result = []
    for _, group in df.groupby("_iso_yw", sort=False):
        group = group.sort_values("date")
        first = group.iloc[0]
        row = first.drop(labels=["_iso_yw"]).to_dict()
        row["date"] = first["date"].date()
        result.append(row)

    # 按 date 正序排列
    result.sort(key=lambda r: r["date"])
    return result


def classify_tier(row: dict) -> str:
    """
    分层决策树，按优先级依次判定，首个命中即返回。

    所需字段：close, macd, prev_macd, pivot_5_pp, pivot_30_pp,
              ma20, ma60, dif, hist_vol

    层级（按判定优先级排列）：
      A  企稳双撑    |MACD_today| < |MACD_yesterday| AND Close > P5_PP AND Close > P30_PP
      B1 回调均线    Close < MA20 AND Close > MA60
      B4 低波整理    hist_vol < 50 AND |MA20距离| <= 4.5%
      B2 超跌支撑    DIF < 0 AND Close > P30_PP
      B3 趋势动能弱  MA20 > MA60 AND DIF < 0
      C2 趋势延续    Close >= MA20 AND |MA20偏离| <= 10%
      C3 过热追涨    Close >= MA20 AND |MA20偏离| > 10%
      C1 跌势减速    Close < MA60 AND |MACD| < |prev_MACD|（MACD 收窄）
      C4 加速下杀    Close < MA60 AND |MACD| >= |prev_MACD|（MACD 放大）
    """
    close = row["close"]
    macd = row["macd"]
    prev_macd = row["prev_macd"]
    p5_pp = row["pivot_5_pp"]
    p30_pp = row["pivot_30_pp"]
    ma20 = row["ma20"]
    ma60 = row["ma60"]
    dif = row["dif"]
    hist_vol = row["hist_vol"]

    # A 企稳双撑
    if abs(macd) < abs(prev_macd) and close > p5_pp and close > p30_pp:
        return "A"

    # B1 回调均线
    if close < ma20 and close > ma60:
        return "B1"

    # B4 低波整理
    ma20_dist = abs((close - ma20) / ma20 * 100)
    if hist_vol < 50 and ma20_dist <= 4.5:
        return "B4"

    # B2 超跌支撑
    if dif < 0 and close > p30_pp:
        return "B2"

    # B3 趋势动能弱
    if ma20 > ma60 and dif < 0:
        return "B3"

    # ── C 类细分 ──
    if close >= ma20:
        if ma20_dist > 10:
            return "C3"  # 过热追涨：价远超 MA20，超买回调风险大
        return "C2"  # 趋势延续：价在 MA20 上方但偏离合理
    if close < ma60:
        if abs(macd) < abs(prev_macd):
            return "C1"  # 跌势减速：MACD 收窄，空头力度递减
        return "C4"  # 加速下杀：MACD 放大，下行动能增强
    return "C2"  # 极少出现的 MA60 ≤ Close < MA20 边缘态


def _extract_rules(row: dict) -> dict:
    """从周数据行提取决策规则详情，供前端决策面板展示。"""
    close = row["close"]
    macd_today = row["macd"]
    macd_yesterday = row["prev_macd"]
    ma20 = row["ma20"]
    return {
        "macd_today": macd_today,
        "macd_yesterday": macd_yesterday,
        "macd_narrow": abs(macd_today) < abs(macd_yesterday),
        "p5_pp": row["pivot_5_pp"],
        "above_p5": close > row["pivot_5_pp"],
        "p30_pp": row["pivot_30_pp"],
        "above_p30": close > row["pivot_30_pp"],
        "ma20": ma20,
        "ma60": row["ma60"],
        "dif": row["dif"],
        "hist_vol": row["hist_vol"],
        "ma20_dist": round((close - ma20) / ma20 * 100, 2),
        "above_ma60": close >= row["ma60"],
    }


def find_expiry_date(entry_date: datetime.date, weeks: int = 3) -> datetime.date:
    """
    从 entry_date 所在周的周一起算，向后推 weeks 整周，
    返回该目标周内最后一个美股交易日（NYSE session）。
    普通周返回周五；遇假日（如 Good Friday）则回退到该周最后交易日。
    """
    monday = entry_date - datetime.timedelta(days=entry_date.weekday())
    target_monday = monday + datetime.timedelta(weeks=weeks)
    target_friday = target_monday + datetime.timedelta(days=4)

    cal = _get_nyse_calendar()
    # 取目标周一到周五范围内的所有交易日，返回最后一个
    sessions = cal.sessions_in_range(
        pd.Timestamp(target_monday), pd.Timestamp(target_friday)
    )
    if len(sessions) == 0:
        # 极端情况：整周无交易日（理论上不会发生），回退到周五
        return target_friday
    return sessions[-1].date()


def backtest_weeks(weekly_rows: list[dict], daily_df: pd.DataFrame,
                    otm: dict[str, float] | None = None) -> list[dict]:
    """
    逐周回测：分层 → 定行权价 → 找到期日价格 → 判断是否平稳到期。

    weekly_rows: group_by_week 输出（正序）
    daily_df: 日线数据，含 date / close 列
    otm: 各层 OTM 映射，默认 DEFAULT_OTM

    返回倒序（最新一周在前）的 list[dict]。
    """
    if otm is None:
        otm = DEFAULT_OTM
    daily = daily_df.copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    last_data_date = daily["date"].max()

    results = []

    for idx, row in enumerate(weekly_rows):
        tier = classify_tier(row)
        entry_date = row["date"]
        close = row["close"]
        otm_frac = otm.get(tier, 0.10)
        otm_pct = int(otm_frac * 100)
        strike = round(close * (1 - otm_frac), 2)

        # 决策规则详情，供前端悬浮面板展示
        rules = _extract_rules(row)

        # 到期日（3 周后周五）
        expiry_date = find_expiry_date(entry_date, weeks=EXPIRY_WEEKS)

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
                # 到期日非交易日，取之前最近一个交易日
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
            # 平稳到期：结算差比 > 0（到期价高于行权价，未被行权）
            safe_expiry = settle_diff > 0

        # 未平稳到期时，计算恢复天数：到期日后最早收盘价 > 行权价的自然日数
        recovery_days = None
        recovery_gap = None  # 未恢复时，最新收盘价距 strike 的百分比差距
        if safe_expiry is False:
            after = daily[daily["date"] > expiry_date].sort_values("date")
            recovered = after[after["close"] > strike]
            if not recovered.empty:
                recovery_date = recovered.iloc[0]["date"]
                recovery_days = (recovery_date - expiry_date).days
            else:
                # 未恢复：用最新收盘价算距 strike 的差距
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

    # 倒序返回
    results.sort(key=lambda r: r["date"], reverse=True)
    return results


def compute_summary(weeks: list[dict]) -> dict:
    """
    汇总统计。
    weeks: backtest_weeks 输出。
    """
    settled = [w for w in weeks if not w["pending"]]
    pending = [w for w in weeks if w["pending"]]
    safe_count = sum(1 for w in settled if w.get("safe_expiry") is True)
    safe_rate = round(safe_count / len(settled) * 100, 1) if settled else 0.0

    return {
        "total_weeks": len(weeks),
        "settled": len(settled),
        "pending": len(pending),
        "safe_count": safe_count,
        "safe_rate": safe_rate,
    }


def compute_tiers(weeks: list[dict],
                   otm: dict[str, float] | None = None) -> dict:
    """
    按层级统计，包含平稳到期比例。
    返回 {tier: {name, otm, count, settled, safe_count, safe_rate}}
    """
    if otm is None:
        otm = DEFAULT_OTM

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
            "otm": int(otm.get(tier_key, 0.10) * 100),
            "count": len(items),
            "settled": len(settled),
            "safe_count": safe_count,
            "safe_rate": safe_rate,
        }
    return result


def compute_latest(weekly_rows: list[dict], daily_df: pd.DataFrame,
                    otm: dict[str, float] | None = None) -> dict:
    """
    最近一周的完整决策详情，用于前端展示。
    weekly_rows: group_by_week 输出（正序）
    daily_df: 日线数据
    """
    if not weekly_rows:
        return {}
    if otm is None:
        otm = DEFAULT_OTM

    row = weekly_rows[-1]
    tier = classify_tier(row)
    close = row["close"]
    rules = _extract_rules(row)

    # 各层行权价
    strikes = {t: round(close * (1 - o), 2) for t, o in otm.items()}

    expiry_date = find_expiry_date(row["date"], weeks=EXPIRY_WEEKS)

    otm_frac = otm.get(tier, 0.10)
    otm_pct = int(otm_frac * 100)

    return {
        "date": str(row["date"]),
        "close": close,
        "tier": tier,
        "rules": rules,
        "otm": otm_pct,
        "strikes": strikes,
        "expiry_date": str(expiry_date),
    }
