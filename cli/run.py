"""策略生成入口：同步数据 → 策略计算 → JSON → HTML"""
import logging
import os
from datetime import datetime

import pandas as pd

from config import TICKER, DEFAULT_OTM
from data.sync.orchestrator import ensure_synced
from data.queries import query_equity_bars, query_ticker_iv
from core.indicators import add_ma, add_macd, add_dynamic_pivot
from core.strategy import group_by_week, compute_hist_vol
from core.backtest import (
    backtest_weeks, enrich_with_options,
    compute_summary, compute_tiers, compute_latest,
)
from core.circuit_breaker import apply_circuit_breaker
from output.report import build_report_data, save_json, render_html

logger = logging.getLogger(__name__)


def setup_logging():
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    setup_logging()
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    logger.info(f"===== {TICKER} =====")

    # 1. 数据同步
    ensure_synced(api_key)

    # 2. 加载数据 + 指标
    rows = query_equity_bars("1900-01-01", datetime.now().strftime("%Y-%m-%d"))
    if not rows:
        logger.warning(f"[{TICKER}] DuckDB 无数据")
        return
    df = pd.DataFrame(rows)
    df = add_ma(df)
    df = add_macd(df)
    df = add_dynamic_pivot(df)

    # 3. 策略计算
    df["hist_vol"] = df["close"].rolling(window=21, min_periods=21).apply(
        lambda x: compute_hist_vol(pd.Series(x.values), window=20), raw=False)
    df["prev_macd"] = df["macd"].shift(1)
    # 保留完整日线用于迷你日K图（dropna 会丢弃前 ~60 行）
    full_daily = df[["date", "open", "high", "low", "close"]].copy()
    full_daily["_date"] = pd.to_datetime(full_daily["date"]).dt.date
    full_daily = full_daily.sort_values("_date").reset_index(drop=True)
    df = df.dropna(subset=["ma60"]).reset_index(drop=True)
    logger.info(f"[{TICKER}] 有效数据 {len(df)} 行")

    weekly_rows = group_by_week(df)
    weeks = backtest_weeks(weekly_rows, df)
    enrich_with_options(weeks, df)
    apply_circuit_breaker(weeks)

    summary = compute_summary(weeks)
    tier_stats = compute_tiers(weeks)
    latest = compute_latest(weekly_rows, df)

    # 为 latest 标记熔断状态
    weeks_asc = sorted(weeks, key=lambda w: w["date"])
    if latest and len(weeks_asc) >= 3:
        last = weeks_asc[-1]
        if last.get("skip"):
            latest["skip"] = True
            latest["skip_reason"] = last["skip_reason"]

    # 4. 附加日K bars（pre_bars/post_bars）
    def _ohlc(r):
        return {"o": round(r["open"], 2), "h": round(r["high"], 2),
                "l": round(r["low"], 2), "c": round(r["close"], 2)}
    for w in weeks:
        entry = datetime.strptime(w["date"], "%Y-%m-%d").date() if isinstance(w["date"], str) else w["date"]
        expiry = datetime.strptime(w["expiry_date"], "%Y-%m-%d").date() if isinstance(w["expiry_date"], str) else w["expiry_date"]
        pre = full_daily[full_daily["_date"] <= entry].tail(21)
        post = full_daily[(full_daily["_date"] > entry) & (full_daily["_date"] <= expiry)]
        w["pre_bars"] = [_ohlc(r) for _, r in pre.iterrows()]
        w["post_bars"] = [_ohlc(r) for _, r in post.iterrows()]

    # 5. IV/HV 数据
    dates = pd.to_datetime(df["date"])
    iv_rows = query_ticker_iv(
        dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d"))
    iv_by_date = {r["date"]: round(r["iv"] * 100, 1) for r in iv_rows}
    hv_by_date = {
        row["date"]: round(row["hist_vol"], 1)
        for _, row in df.iterrows() if pd.notna(row.get("hist_vol"))
    }
    for w in weeks:
        w["iv"] = iv_by_date.get(w["date"])
        w["hv"] = hv_by_date.get(w["date"])

    # 6. Daily bars（图表用）
    df["vol_ma20"] = df["volume"].rolling(window=20, min_periods=20).mean()
    recent = df.tail(60)
    daily_bars = [
        {
            "date": row["date"],
            "open": round(row["open"], 2), "high": round(row["high"], 2),
            "low": round(row["low"], 2), "close": round(row["close"], 2),
            "volume": int(row.get("volume", 0)),
            "dif": round(row["dif"], 4) if pd.notna(row.get("dif")) else None,
            "dea": round(row["dea"], 4) if pd.notna(row.get("dea")) else None,
            "macd": round(row["macd"], 4) if pd.notna(row.get("macd")) else None,
            "ma5": round(row["ma5"], 2) if pd.notna(row.get("ma5")) else None,
            "ma10": round(row["ma10"], 2) if pd.notna(row.get("ma10")) else None,
            "ma20": round(row["ma20"], 2) if pd.notna(row.get("ma20")) else None,
            "ma60": round(row["ma60"], 2) if pd.notna(row.get("ma60")) else None,
            "vol_ma20": round(row["vol_ma20"], 0) if pd.notna(row.get("vol_ma20")) else None,
            "iv": iv_by_date.get(row["date"]),
            "hv": round(row["hist_vol"], 1) if pd.notna(row.get("hist_vol")) else None,
        }
        for _, row in recent.iterrows()
    ]

    # 7. 行情快照
    last_bar = daily_bars[-1] if daily_bars else {}
    prev_bar = daily_bars[-2] if len(daily_bars) >= 2 else {}
    if last_bar and prev_bar:
        market = {
            "date": last_bar["date"],
            "close": last_bar["close"],
            "change_pct": round((last_bar["close"] - prev_bar["close"]) / prev_bar["close"] * 100, 2),
            "iv": last_bar.get("iv"),
            "hv": last_bar.get("hv"),
        }
        # 收集所有进行中的合约
        active_contracts = []
        for w in sorted(weeks, key=lambda x: x["date"], reverse=True):
            if w.get("pending") and w.get("option_symbol") and not w.get("skip"):
                sym = w["option_symbol"]
                p_idx = sym.rindex("P")
                strike_val = float(sym[p_idx + 1:])
                active_contracts.append({
                    "date": w["date"], "tier": w["tier"], "otm": w.get("otm"),
                    "symbol": w["option_symbol"], "price": w.get("option_price"),
                    "strike": strike_val, "expiry": w.get("expiry_date"),
                    "dte": w.get("option_dte"),
                    "pre_bars": w.get("pre_bars", []),
                    "post_bars": w.get("post_bars", []),
                })
        # latest 的合约也加入
        if latest and latest.get("option_symbol"):
            latest_sym = latest["option_symbol"]
            if not any(c["symbol"] == latest_sym for c in active_contracts):
                sym = latest_sym
                p_idx = sym.rindex("P")
                strike_val = float(sym[p_idx + 1:])
                matched_w = next((w for w in weeks if w["date"] == latest.get("date")), {})
                active_contracts.insert(0, {
                    "date": latest.get("date"), "tier": latest.get("tier"),
                    "otm": latest.get("otm"), "symbol": latest_sym,
                    "price": latest.get("option_price"), "strike": strike_val,
                    "expiry": latest.get("option_expiry"),
                    "dte": latest.get("option_dte"),
                    "pre_bars": matched_w.get("pre_bars", []),
                    "post_bars": matched_w.get("post_bars", []),
                })
        market["active_contracts"] = active_contracts
    else:
        market = None

    # 8. 输出
    data = build_report_data(
        tiers=weeks, summary=summary, tier_stats=tier_stats,
        latest=latest, weeks=weeks, daily_bars=daily_bars,
        market=market,
        data_range=[dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")],
        generated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        otm_config=DEFAULT_OTM,
    )
    save_json(data)
    render_html(data)
    logger.info("全部完成")


if __name__ == "__main__":
    main()
