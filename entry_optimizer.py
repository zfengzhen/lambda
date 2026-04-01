"""入场限价优化器：通过历史期权数据找到最优限价倍数 k

用法:
    python entry_optimizer.py                       # 默认 TQQQ，读取 output/TQQQ.json
    python entry_optimizer.py --ticker TQQQ
    python entry_optimizer.py --output report.txt   # 保存报告到文件

前置条件:
    1. 已运行 python run.py 生成 output/TQQQ.json
    2. 设置 MASSIVE_API_KEY 环境变量
"""
import datetime
import logging

from option_fetcher import build_occ_symbol, fetch_option_bars

logger = logging.getLogger(__name__)


def enrich_with_option_data(trades: list[dict], api_key: str,
                             ticker: str = "TQQQ") -> list[dict]:
    """为每笔信号交易拉取信号周（周一至周五）期权日线数据。

    通过 Massive API 获取期权 OHLC，提取:
    - mon_close_option: 周一收盘价（限价单参考价）
    - tue_high/wed_high/thu_high/fri_high: 周二至周五各日最高价
    - week_high: max(周二至周五最高价)
    - option_symbol: 合约 OCC symbol
    - data_complete: True 当且仅当 mon_close_option 非空
                     且至少 3 天有高价数据

    Args:
        trades: get_signal_trades() 输出
        api_key: Massive API Key
        ticker: 标的代码（默认 TQQQ）

    Returns:
        trades 的副本，每条追加上述字段。
    """
    enriched = []
    for trade in trades:
        symbol = build_occ_symbol(ticker, trade["expiry"], trade["strike"], "P")

        # 信号周：周一（week_start）到周五（+4 天）
        mon = datetime.date.fromisoformat(trade["week_start"])
        fri = mon + datetime.timedelta(days=4)
        bars = fetch_option_bars(symbol, str(mon), str(fri), api_key)
        bar_by_date = {b["date"]: b for b in bars}

        def _close(offset: int):
            d = str(mon + datetime.timedelta(days=offset))
            b = bar_by_date.get(d)
            return b["close"] if b else None

        def _high(offset: int):
            d = str(mon + datetime.timedelta(days=offset))
            b = bar_by_date.get(d)
            return b["high"] if b else None

        mon_close_option = _close(0)
        tue_high = _high(1)
        wed_high = _high(2)
        thu_high = _high(3)
        fri_high = _high(4)

        week_high_vals = [v for v in [tue_high, wed_high, thu_high, fri_high]
                          if v is not None]
        week_high = max(week_high_vals) if week_high_vals else None

        data_complete = (mon_close_option is not None
                         and len(week_high_vals) >= 3)

        if not data_complete:
            logger.warning(
                f"[{symbol}] 数据不完整: "
                f"mon_close={mon_close_option}, 有效天数={len(week_high_vals)}"
            )
        else:
            logger.info(f"[{symbol}] mon_close={mon_close_option:.4f}, week_high={week_high:.4f}")

        enriched.append({
            **trade,
            "option_symbol": symbol,
            "mon_close_option": mon_close_option,
            "tue_high": tue_high,
            "wed_high": wed_high,
            "thu_high": thu_high,
            "fri_high": fri_high,
            "week_high": week_high,
            "data_complete": data_complete,
        })

    return enriched
