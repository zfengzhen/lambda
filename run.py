"""
入口脚本：同步数据 → 策略计算 → 输出 JSON → 内嵌到 HTML

用法:
    python run.py              # 默认 TQQQ
    python run.py TQQQ QQQ    # 多标的批量运行
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

import data_store
from strategy import (
    group_by_week,
    backtest_weeks,
    compute_summary,
    compute_tiers,
    compute_latest,
    compute_hist_vol,
    get_otm_for_ticker,
    DEFAULT_OTM,
)

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")


def setup_logging():
    """配置控制台日志"""
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def fetch_equity_bars(ticker: str, api_key: str) -> pd.DataFrame | None:
    """从 DuckDB 读取股票日K（自动触发增量同步）。返回含指标的 DataFrame，失败返回 None。"""
    from data_sync import ensure_synced
    from indicators import add_ma, add_macd, add_dynamic_pivot

    ensure_synced([ticker], api_key)

    rows = data_store.query_equity_bars(ticker, "1900-01-01",
                                        datetime.now().strftime("%Y-%m-%d"))
    if not rows:
        logger.warning(f"[{ticker}] DuckDB 无数据")
        return None

    df = pd.DataFrame(rows)
    df = add_ma(df)
    df = add_macd(df)
    df = add_dynamic_pivot(df)
    return df


def enrich_weeks_with_options(ticker: str, weeks: list[dict], daily: pd.DataFrame):
    """为每周回测数据补充期权合约信息，并用 OCC 真实 strike 重算结算指标。

    查询行权价向下取整对应的 Put 期权在入场日的价格，
    写入 option_symbol / option_dte / option_price / option_vwap 字段。
    用合约真实 strike 重算 settle_diff / safe_expiry / recovery_days。
    """
    for w in weeks:
        strike = w.get("strike")
        expiry = w.get("expiry_date")
        entry = w.get("date")
        if not strike or not expiry or not entry:
            continue
        opt = data_store.query_option_on_date(ticker, entry, expiry, strike)
        if opt:
            # 从实际匹配的 OCC symbol 提取到期日（可能与策略算的差 1-2 天）
            # OCC 格式: O:{TICKER}{YYMMDD}{P/C}{STRIKE}，日期在 P 之前 6 位
            occ = opt["symbol"]  # e.g. "O:TQQQ260402P00041000"
            p_idx = occ.index("P")
            occ_date6 = occ[p_idx - 6:p_idx]  # "260402"
            occ_expiry = f"20{occ_date6[0:2]}-{occ_date6[2:4]}-{occ_date6[4:6]}"
            dte = (datetime.strptime(occ_expiry, "%Y-%m-%d") - datetime.strptime(entry, "%Y-%m-%d")).days
            # 从合约 OCC symbol 提取真实 strike（末 8 位，单位千分之一美元）
            occ_strike = int(occ[-8:]) / 1000.0
            # 显示 strike：整数显示整数（P50），有小数显示小数（P50.5）
            occ_strike_str = str(int(occ_strike)) if occ_strike == int(occ_strike) else str(occ_strike)
            premium = round(opt["close"], 2)
            w["option_symbol"] = f"{ticker} {occ_expiry} P{occ_strike_str}"
            w["option_dte"] = dte
            w["option_price"] = premium
            w["option_vwap"] = round(opt["vwap"], 4)
            # 用合约真实 strike 重算结算差比和平稳到期
            if w.get("expiry_close") is not None and occ_strike > 0:
                w["settle_diff"] = round((w["expiry_close"] - occ_strike) / occ_strike * 100, 2)
                w["safe_expiry"] = w["settle_diff"] > 0
                # 用 OCC strike 统一重算 recovery_days，避免与策略 strike 不一致
                if w["safe_expiry"]:
                    w["recovery_days"] = None
                    w["recovery_gap"] = None
                else:
                    # 穿仓：用 OCC strike 重算恢复天数
                    expiry_str = w["expiry_date"]
                    after = daily[daily["date"] > expiry_str].sort_values("date")
                    recovered = after[after["close"] > occ_strike]
                    if not recovered.empty:
                        rec_date_str = str(recovered.iloc[0]["date"])
                        delta = datetime.strptime(rec_date_str, "%Y-%m-%d") - datetime.strptime(expiry_str, "%Y-%m-%d")
                        w["recovery_days"] = delta.days
                        w["recovery_gap"] = None
                    else:
                        w["recovery_days"] = None
                        latest_close = float(daily.iloc[-1]["close"])
                        w["recovery_gap"] = round((latest_close - occ_strike) / occ_strike * 100, 1)
        else:
            w["option_symbol"] = None
            w["option_dte"] = None
            w["option_price"] = None
            w["option_vwap"] = None


def compute_strategy(ticker: str, df: pd.DataFrame) -> dict | None:
    """DataFrame → 策略计算 → 返回结果 dict"""
    df["hist_vol"] = df["close"].rolling(window=21, min_periods=21).apply(
        lambda x: compute_hist_vol(pd.Series(x.values), window=20), raw=False
    )
    df["prev_macd"] = df["macd"].shift(1)
    # 保留完整日线用于迷你日K图（dropna 会丢弃前 ~60 行）
    full_daily = df[["date", "open", "high", "low", "close"]].copy()
    full_daily["_date"] = pd.to_datetime(full_daily["date"]).dt.date
    full_daily = full_daily.sort_values("_date").reset_index(drop=True)
    df = df.dropna(subset=["ma60"]).reset_index(drop=True)

    logger.info(f"[{ticker}] 有效数据 {len(df)} 行")

    otm = get_otm_for_ticker(ticker)
    logger.info(f"[{ticker}] OTM: {' '.join(f'{t}={int(v*100)}%' for t,v in otm.items())}")

    weekly_rows = group_by_week(df)
    weeks = backtest_weeks(weekly_rows, df, otm=otm)

    # 丰富期权数据：查询行权价向下取整的 Put 期权价格，用 OCC strike 重算结算指标
    enrich_weeks_with_options(ticker, weeks, df)

    # 熔断标记：前 2 周都是 C 类 + 本周也是 C 类时才可能暂停（A/B 类始终放行）
    #   本周 C 类但非 C1 → 暂停
    #   本周 C1 且前 2 周含 C1 → 继续卖出（跌势已有减速信号）
    #   本周 C1 但前 2 周无 C1 → 暂停（纯下杀后首次减速，不够安全）
    _c_tiers = {"C1", "C2", "C3", "C4"}
    weeks_asc = sorted(weeks, key=lambda w: w["date"])
    for i, w in enumerate(weeks_asc):
        if i >= 2:
            p1 = weeks_asc[i - 1]["tier"]
            p2 = weeks_asc[i - 2]["tier"]
            if p1 in _c_tiers and p2 in _c_tiers and w["tier"] in _c_tiers:
                if w["tier"] == "C1" and (p1 == "C1" or p2 == "C1"):
                    w["skip"] = False  # 前2周有C1减速信号，本周C1继续卖出
                else:
                    w["skip"] = True
                    w["skip_reason"] = f"前2周 {p2}→{p1}，本周 {w['tier']}，连续弱势暂停"
                continue
        w["skip"] = False

    summary = compute_summary(weeks)
    tiers = compute_tiers(weeks, otm=otm)
    latest = compute_latest(weekly_rows, df, otm=otm)
    # 为 latest 标记熔断状态（取最后 3 周判断）
    if latest and len(weeks_asc) >= 3:
        last = weeks_asc[-1]
        if last.get("skip"):
            latest["skip"] = True
            latest["skip_reason"] = last["skip_reason"]

    # 为 latest 查询期权合约
    if latest:
        lt_strike = latest.get("strikes", {}).get(latest["tier"])
        lt_opt = data_store.query_option_on_date(
            ticker, latest["date"], latest["expiry_date"], lt_strike or 0)
        if lt_opt:
            occ = lt_opt["symbol"]
            p_idx = occ.index("P")
            occ_date6 = occ[p_idx - 6:p_idx]
            occ_expiry = f"20{occ_date6[0:2]}-{occ_date6[2:4]}-{occ_date6[4:6]}"
            occ_strike = int(occ[-8:]) / 1000.0
            occ_strike_str = str(int(occ_strike)) if occ_strike == int(occ_strike) else str(occ_strike)
            latest["option_symbol"] = f"{ticker} {occ_expiry} P{occ_strike_str}"
            latest["option_dte"] = (datetime.strptime(occ_expiry, "%Y-%m-%d") - datetime.strptime(latest["date"], "%Y-%m-%d")).days
            latest["option_price"] = round(lt_opt["close"], 2)
            latest["option_strike"] = occ_strike
            latest["option_expiry"] = occ_expiry

    dates = pd.to_datetime(df["date"])

    # 为每周回测数据附加日K：pre_bars（入场日前20交易日+入场日）+ post_bars（入场日后到到期日）
    # 使用 full_daily（dropna 前的完整日线），避免早期数据因 MA60 预热被截断
    def _ohlc(r):
        return {"o": round(r["open"], 2), "h": round(r["high"], 2),
                "l": round(r["low"], 2), "c": round(r["close"], 2)}
    for w in weeks:
        entry = datetime.strptime(w["date"], "%Y-%m-%d").date() if isinstance(w["date"], str) else w["date"]
        expiry = datetime.strptime(w["expiry_date"], "%Y-%m-%d").date() if isinstance(w["expiry_date"], str) else w["expiry_date"]
        # 入场日及之前 20 个交易日
        pre = full_daily[full_daily["_date"] <= entry].tail(21)
        # 入场日之后到到期日
        post = full_daily[(full_daily["_date"] > entry) & (full_daily["_date"] <= expiry)]
        w["pre_bars"] = [_ohlc(r) for _, r in pre.iterrows()]
        w["post_bars"] = [_ohlc(r) for _, r in post.iterrows()]

    # 查询 IV 数据，建立日期→IV 映射
    iv_rows = data_store.query_ticker_iv(
        ticker, dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d"))
    iv_by_date = {r["date"]: round(r["iv"] * 100, 1) for r in iv_rows}

    # 为每周回测数据附加入场日 IV
    for w in weeks:
        w["iv"] = iv_by_date.get(w["date"])

    # 最近 30 个交易日的日K + MACD + MA，供 HTML 图表使用
    # MA5/MA10/MA20/MA60 已由 add_ma() 在完整 df 上计算完成，tail(60) 直接取值即可
    df["vol_ma20"] = df["volume"].rolling(window=20, min_periods=20).mean()
    recent = df.tail(60)
    daily_bars = [
        {
            "date": row["date"],
            "open": round(row["open"], 2),
            "high": round(row["high"], 2),
            "low": round(row["low"], 2),
            "close": round(row["close"], 2),
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
        }
        for _, row in recent.iterrows()
    ]

    # 行情快照：最新交易日数据 + 日涨跌幅
    last_bar = daily_bars[-1] if daily_bars else {}
    prev_bar = daily_bars[-2] if len(daily_bars) >= 2 else {}
    if last_bar and prev_bar:
        prev_close = prev_bar["close"]
        market = {
            "date": last_bar["date"],
            "close": last_bar["close"],
            "change_pct": round((last_bar["close"] - prev_close) / prev_close * 100, 2),
            "iv": last_bar.get("iv"),
        }
        # 收集所有进行中的合约（pending 且有期权数据），按日期从新到旧
        active_contracts = []
        for w in sorted(weeks, key=lambda x: x["date"], reverse=True):
            if w.get("pending") and w.get("option_symbol") and not w.get("skip"):
                # 从 option_symbol 提取 strike（P 后面的数字）
                sym = w["option_symbol"]
                p_idx = sym.rindex("P")
                strike_val = float(sym[p_idx + 1:])
                active_contracts.append({
                    "date": w["date"],
                    "tier": w["tier"],
                    "otm": w.get("otm"),
                    "symbol": w["option_symbol"],
                    "price": w.get("option_price"),
                    "strike": strike_val,
                    "expiry": w.get("expiry_date"),
                    "pre_bars": w.get("pre_bars", []),
                    "post_bars": w.get("post_bars", []),
                })
        # latest 的合约也加入（如果还没在列表中）
        if latest and latest.get("option_symbol"):
            latest_sym = latest["option_symbol"]
            if not any(c["symbol"] == latest_sym for c in active_contracts):
                sym = latest_sym
                p_idx = sym.rindex("P")
                strike_val = float(sym[p_idx + 1:])
                # 从 weeks 中查找同日期的 pre_bars/post_bars
                matched_w = next((w for w in weeks if w["date"] == latest.get("date")), {})
                active_contracts.insert(0, {
                    "date": latest.get("date"),
                    "tier": latest.get("tier"),
                    "otm": latest.get("otm"),
                    "symbol": latest_sym,
                    "price": latest.get("option_price"),
                    "strike": strike_val,
                    "expiry": latest.get("option_expiry"),
                    "pre_bars": matched_w.get("pre_bars", []),
                    "post_bars": matched_w.get("post_bars", []),
                })
        market["active_contracts"] = active_contracts
    else:
        market = None

    return {
        "ticker": ticker,
        "generated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "data_range": [dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")],
        "otm_config": otm,
        "summary": summary,
        "tiers": tiers,
        "latest": latest,
        "weeks": weeks,
        "daily_bars": daily_bars,
        "market": market,
    }


def save_json(ticker: str, result: dict):
    """保存 JSON 到 output/ 目录"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"[{ticker}] JSON 已保存: {json_path}")


def load_template() -> str | None:
    """读取 template.html 模板"""
    template_path = os.path.join(SCRIPT_DIR, "template.html")
    if not os.path.exists(template_path):
        logger.warning("template.html 不存在")
        return None
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def embed_to_html(ticker: str, result: dict, template_html: str):
    """将策略结果内嵌到模板，生成 output/{TICKER}.html"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html_path = os.path.join(OUTPUT_DIR, f"{ticker}.html")

    data_str = json.dumps(result, ensure_ascii=False, indent=2)

    marker = "/* EMBEDDED_DATA_PLACEHOLDER */"
    if marker not in template_html:
        logger.warning("template.html 中未找到内嵌数据占位符")
        return

    html = template_html.replace(marker, f'EMBEDDED_DATA["{ticker}"] = {data_str};\n' + marker)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"[{ticker}] HTML 已生成: {html_path}")


def main():
    parser = argparse.ArgumentParser(description="Lambda Strategy — Sell Put 回测")
    parser.add_argument("tickers", nargs="*", default=["TQQQ"],
                        help="标的代码（默认 TQQQ），可指定多个")
    args = parser.parse_args()

    setup_logging()
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    template_html = load_template()

    for ticker in args.tickers:
        ticker = ticker.upper()
        logger.info(f"===== {ticker} =====")

        df = fetch_equity_bars(ticker, api_key)
        if df is None:
            continue

        result = compute_strategy(ticker, df)
        if result is None:
            continue

        save_json(ticker, result)

        if template_html:
            embed_to_html(ticker, result, template_html)

    logger.info("全部完成")


if __name__ == "__main__":
    main()
