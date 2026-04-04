# 日K图上方行情快照

## 概述

在 HTML 报告的日K图表上方添加最新行情数据条，展示收盘价、周涨跌幅、OHLC、成交量。

## 数据字段

| 字段 | 来源 | 说明 |
|------|------|------|
| 最新收盘价 | `daily_bars[-1].close` | 大字号 |
| 涨跌幅(%) | `(daily_bars[-1].close - latest.close) / latest.close` | 周入场价为基准 |
| 涨跌额 | `daily_bars[-1].close - latest.close` | 绝对值 |
| 开盘价 | `daily_bars[-1].open` | |
| 最高价 | `daily_bars[-1].high` | |
| 最低价 | `daily_bars[-1].low` | |
| 成交量 | `daily_bars[-1].volume` | |
| 量比 | `daily_bars[-1].volume / daily_bars[-1].vol_ma20` | 倍数显示 |
| 数据日期 | `daily_bars[-1].date` | 标注截止时间 |

## 数据流变更

**run.py**：在输出 JSON 中新增 `market` 字段，从 `daily_bars[-1]` 和 `latest.close` 汇总计算。

```python
market = {
    "date": last_bar["date"],
    "close": last_bar["close"],
    "open": last_bar["open"],
    "high": last_bar["high"],
    "low": last_bar["low"],
    "volume": last_bar["volume"],
    "vol_ratio": last_bar["volume"] / last_bar["vol_ma20"],
    "entry_close": latest["close"],
    "change": last_bar["close"] - latest["close"],
    "change_pct": (last_bar["close"] - latest["close"]) / latest["close"] * 100,
}
```

**template.html**：日K图 `<canvas>` 上方插入行情条 HTML，读取 `data.market`。

## 视觉规范

- 深色背景一致（沿用 `#181c25` / `#232a36` 色系）
- 收盘价：22px bold，白色
- 涨跌幅/涨跌额：绿涨红跌（`#22c55e` / `#ef4444`），美股惯例
- OHLC + 成交量：14px，`#8b95a5` 次要灰色
- 横向紧凑排列，响应式

## 涉及文件

| 文件 | 变更 |
|------|------|
| `run.py` | 新增 `market` 字段到输出 JSON |
| `template.html` | 日K图上方插入行情条 |
