# tests/core/test_indicators.py
"""core.indicators 单元测试。"""
import pandas as pd
import numpy as np
from core.indicators import add_ma, add_macd, add_dynamic_pivot


class TestMA:
    """测试移动平均线"""

    def test_ma5(self):
        """5 期 MA 计算正确"""
        closes = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
        df = pd.DataFrame({"close": closes})
        df = add_ma(df)

        # ma5 第 5 行 (index=4): mean(10,11,12,13,14) = 12.0
        assert df["ma5"].iloc[4] == 12.0
        # 前 4 行应为 NaN
        assert pd.isna(df["ma5"].iloc[3])

    def test_all_ma_columns_exist(self):
        """所有 MA 列都应存在"""
        df = pd.DataFrame({"close": range(130)})
        df = add_ma(df)

        for period in [5, 10, 20, 60]:
            col = f"ma{period}"
            assert col in df.columns, f"缺少列 {col}"


class TestMACD:
    """测试 MACD 指标"""

    def test_macd_columns_exist(self):
        """MACD/DIF/DEA 列应存在"""
        df = pd.DataFrame({"close": [float(i) for i in range(50)]})
        df = add_macd(df)

        assert "dif" in df.columns
        assert "dea" in df.columns
        assert "macd" in df.columns

    def test_macd_relationship(self):
        """MACD = 2 * (DIF - DEA)"""
        df = pd.DataFrame({"close": [float(i) + 50.0 for i in range(50)]})
        df = add_macd(df)

        # 找到非 NaN 的行验证关系
        valid = df.dropna(subset=["macd", "dif", "dea"])
        assert len(valid) > 0

        for _, row in valid.iterrows():
            expected = 2 * (row["dif"] - row["dea"])
            assert abs(row["macd"] - expected) < 1e-10


class TestDynamicPivot:
    """测试动态 Pivot 指标"""

    def test_pivot_5_uses_rolling_window(self):
        """周期 5 应使用最近 5 根K线的 high/low"""
        df = pd.DataFrame({
            "high":  [10.0, 20.0, 15.0, 18.0, 25.0, 22.0],
            "low":   [5.0,  8.0,  7.0,  9.0,  12.0, 11.0],
            "close": [8.0,  15.0, 12.0, 16.0, 20.0, 18.0],
        })
        df = add_dynamic_pivot(df)

        # index=4: 最近 5 根 high max=25, low min=5, close=20
        row = df.iloc[4]
        pp = (25.0 + 5.0 + 20.0) / 3  # 16.667
        assert abs(row["pivot_5_pp"] - pp) < 0.001

    def test_all_pivot_columns_exist(self):
        """所有周期的 7 个 Pivot 列都应存在"""
        df = pd.DataFrame({
            "high":  [float(i + 1) for i in range(130)],
            "low":   [float(i) for i in range(130)],
            "close": [float(i + 0.5) for i in range(130)],
        })
        df = add_dynamic_pivot(df)

        for period in [5, 30]:
            for suffix in ["pp", "r1", "r2", "r3", "s1", "s2", "s3"]:
                col = f"pivot_{period}_{suffix}"
                assert col in df.columns, f"缺少列 {col}"
