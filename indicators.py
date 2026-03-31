"""技术指标计算：MA、MACD、动态 Pivot"""
import pandas as pd

MA_PERIODS = [5, 10, 20, 30, 60, 120]
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
PIVOT_PERIODS = [1, 5, 10, 20, 30, 50, 120]


def add_ma(df: pd.DataFrame) -> pd.DataFrame:
    """添加 MA 均线列：ma5, ma10, ma20, ma30, ma60, ma120"""
    for period in MA_PERIODS:
        df[f"ma{period}"] = df["close"].rolling(window=period).mean()
    return df


def add_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    添加 MACD 指标列。
    - DIF = EMA(close, 12) - EMA(close, 26)
    - DEA = EMA(DIF, 9)
    - MACD = 2 * (DIF - DEA)
    """
    ema_fast = df["close"].ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = df["close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["dif"] = ema_fast - ema_slow
    df["dea"] = df["dif"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    df["macd"] = 2 * (df["dif"] - df["dea"])
    return df


def add_dynamic_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    添加动态 Pivot 指标。
    对每个周期 N，取最近 N 根K线的 high 最大值、low 最小值、当前 close，
    计算 PP/R1/R2/R3/S1/S2/S3。
    """
    for period in PIVOT_PERIODS:
        h = df["high"].rolling(window=period).max()
        l = df["low"].rolling(window=period).min()
        c = df["close"]

        pp = (h + l + c) / 3
        df[f"pivot_{period}_pp"] = pp
        df[f"pivot_{period}_r1"] = 2 * pp - l
        df[f"pivot_{period}_s1"] = 2 * pp - h
        df[f"pivot_{period}_r2"] = pp + (h - l)
        df[f"pivot_{period}_s2"] = pp - (h - l)
        df[f"pivot_{period}_r3"] = h + 2 * (pp - l)
        df[f"pivot_{period}_s3"] = l - 2 * (h - l)
    return df
