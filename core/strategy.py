"""策略核心：决策树分层、周分组、到期日计算、历史波动率。"""
import datetime
import math

import numpy as np
import pandas as pd

from config import EXPIRY_WEEKS, TRADING_DAYS_YEAR

# NYSE 交易日历（懒加载）
_nyse_calendar = None


def _get_nyse_calendar():
    global _nyse_calendar
    if _nyse_calendar is None:
        import exchange_calendars as xcals
        _nyse_calendar = xcals.get_calendar("XNYS")
    return _nyse_calendar


def compute_hist_vol(closes: pd.Series, window: int = 20) -> float:
    """计算年化历史波动率（百分比）。
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
    """按 ISO 年+周分组，取每周第一个交易日的行。
    输入 df 至少有 date、close 列。返回 list[dict]，date 转为 datetime.date。
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["_iso_yw"] = df["date"].apply(lambda d: (d.isocalendar()[0], d.isocalendar()[1]))
    df = df.sort_values("date")

    result = []
    for _, group in df.groupby("_iso_yw", sort=False):
        group = group.sort_values("date")
        first = group.iloc[0]
        row = first.drop(labels=["_iso_yw"]).to_dict()
        row["date"] = first["date"].date()
        result.append(row)

    result.sort(key=lambda r: r["date"])
    return result


def classify_tier(row: dict) -> str:
    """分层决策树，按优先级依次判定，首个命中即返回。

    层级（按判定优先级）：
      A  企稳双撑    |MACD_today| < |MACD_yesterday| AND Close > P5_PP AND Close > P30_PP
      B1 回调均线    Close < MA20 AND Close > MA60
      B4 低波整理    hist_vol < 50 AND |MA20距离| <= 4.5%
      B2 超跌支撑    DIF < 0 AND Close > P30_PP
      B3 趋势动能弱  MA20 > MA60 AND DIF < 0
      C2 趋势延续    Close >= MA20 AND |MA20偏离| <= 10%
      C3 过热追涨    Close >= MA20 AND |MA20偏离| > 10%
      C1 跌势减速    Close < MA60 AND |MACD| < |prev_MACD|
      C4 加速下杀    Close < MA60 AND |MACD| >= |prev_MACD|
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
    # C 类细分
    if close >= ma20:
        if ma20_dist > 10:
            return "C3"  # 过热追涨
        return "C2"  # 趋势延续
    if close < ma60:
        if abs(macd) < abs(prev_macd):
            return "C1"  # 跌势减速
        return "C4"  # 加速下杀
    return "C2"  # MA60 ≤ Close < MA20 边缘态


def extract_rules(row: dict) -> dict:
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


def find_expiry_date(entry_date: datetime.date) -> datetime.date:
    """从 entry_date 所在周的周一起算，向后推 EXPIRY_WEEKS 整周，
    返回该目标周内最后一个美股交易日。
    """
    monday = entry_date - datetime.timedelta(days=entry_date.weekday())
    target_monday = monday + datetime.timedelta(weeks=EXPIRY_WEEKS)
    target_friday = target_monday + datetime.timedelta(days=4)

    cal = _get_nyse_calendar()
    sessions = cal.sessions_in_range(
        pd.Timestamp(target_monday), pd.Timestamp(target_friday)
    )
    if len(sessions) == 0:
        return target_friday
    return sessions[-1].date()
