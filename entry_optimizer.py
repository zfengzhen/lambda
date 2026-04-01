"""入场限价优化器：通过历史期权数据找到最优限价倍数 k

用法:
    python entry_optimizer.py                       # 默认 TQQQ，读取 output/TQQQ.json
    python entry_optimizer.py --ticker TQQQ
    python entry_optimizer.py --output report.txt   # 保存报告到文件

数据来源优先级:
    1. 本地 DuckDB（output/market_data.duckdb，最快，无网络）— 需先运行 data_sync.py
    2. Flat Files（S3，完整历史）— 需设置 MASSIVE_S3_ACCESS_KEY / MASSIVE_S3_SECRET_KEY
    3. REST API（近 4 个月）     — 需设置 MASSIVE_API_KEY

前置条件:
    1. 已运行 python run.py 生成 output/TQQQ.json
    2. 至少满足上述其中一个数据来源条件
"""
import datetime
import json
import logging
import os

import numpy as np

import data_store
from option_fetcher import build_occ_symbol, fetch_option_bars, get_signal_trades

logger = logging.getLogger(__name__)


def _enrich(trades: list[dict], ticker: str, fetch_fn) -> list[dict]:
    """核心富化逻辑：用 fetch_fn 拉取每笔交易的信号周期权日线数据。

    Args:
        trades:   get_signal_trades() 输出
        ticker:   标的代码
        fetch_fn: callable(symbol, from_date, to_date) → list[{date,open,high,low,close}]

    Returns:
        trades 的副本，每条追加 option_symbol / mon_close_option /
        tue_high ~ fri_high / week_high / data_complete 字段。
    """
    enriched = []
    for trade in trades:
        symbol = build_occ_symbol(ticker, trade["expiry"], trade["strike"], "P")

        # 信号周：信号日（week_start，通常为周一；节假日周可能为周二）到 +4 日历天。
        # offset=0 = 信号日收盘价（限价单参考价 P），
        # offset=1..4 = 后续交易日最高价（挂单执行窗口）。
        # 节假日周下字段标签（tue_high 等）与日历日期不对应，但算法语义正确。
        mon = datetime.date.fromisoformat(trade["week_start"])
        fri = mon + datetime.timedelta(days=4)
        bars = fetch_fn(symbol, str(mon), str(fri))
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


def enrich_with_option_data(trades: list[dict], api_key: str,
                             ticker: str = "TQQQ") -> list[dict]:
    """通过 REST API 富化信号交易（数据保留约 4 个月）。

    Args:
        trades:  get_signal_trades() 输出
        api_key: Massive REST API Key
        ticker:  标的代码（默认 TQQQ）

    Returns:
        trades 的副本，追加期权日线字段。
    """
    def fetch_fn(symbol, from_date, to_date):
        return fetch_option_bars(symbol, from_date, to_date, api_key)

    return _enrich(trades, ticker, fetch_fn)


def enrich_with_flat_files(trades: list[dict], s3_client,
                            ticker: str = "TQQQ") -> list[dict]:
    """通过 Flat Files（S3）富化信号交易（完整历史，从 2014 年起）。

    Args:
        trades:    get_signal_trades() 输出
        s3_client: flat_file_fetcher.make_s3_client() 返回的客户端
        ticker:    标的代码（默认 TQQQ）

    Returns:
        trades 的副本，追加期权日线字段。
    """
    from flat_file_fetcher import fetch_option_bars_flat

    def fetch_fn(symbol, from_date, to_date):
        return fetch_option_bars_flat(symbol, from_date, to_date, s3_client)

    return _enrich(trades, ticker, fetch_fn)


def enrich_with_db(trades: list[dict], ticker: str = "TQQQ") -> list[dict]:
    """通过本地 DuckDB 富化信号交易（最快，无网络开销）。

    Args:
        trades: get_signal_trades() 输出
        ticker: 标的代码（默认 TQQQ）

    Returns:
        trades 的副本，追加期权日线字段。
    """
    def fetch_fn(symbol, from_date, to_date):
        bars = data_store.query_option_bars(symbol, from_date, to_date)
        return [{"date": b["date"], "open": b["open"], "high": b["high"],
                 "low": b["low"], "close": b["close"]} for b in bars]

    return _enrich(trades, ticker, fetch_fn)


def sweep_k(trades: list[dict],
            k_min: float = 0.5,
            k_max: float = 3.0,
            k_step: float = 0.05) -> list[dict]:
    """扫描不同 k 值，计算每个 k 下的总权利金和成交率。

    限价单 = mon_close_option × k；成交条件：week_high >= 限价单。
    仅使用 data_complete=True 的交易。

    Args:
        trades:          enrich_with_option_data() 输出
        k_min, k_max:    扫描范围（含端点）
        k_step:          步长

    Returns:
        每个 k 对应 {k, total_premium, fill_count, fill_rate} 的列表
    """
    valid = [t for t in trades if t.get("data_complete")]
    if not valid:
        return []

    results = []
    for k in np.arange(k_min, k_max + k_step / 2, k_step):
        k = round(float(k), 10)
        total_premium = 0.0
        fill_count = 0
        for trade in valid:
            limit = round(trade["mon_close_option"] * k, 10)
            if trade["week_high"] >= limit:
                total_premium += limit
                fill_count += 1
        results.append({
            "k": round(k, 4),
            "total_premium": round(total_premium, 6),
            "fill_count": fill_count,
            "fill_rate": round(fill_count / len(valid), 4),
        })
    return results


def find_optimal_k(sweep_results: list[dict]) -> dict:
    """从扫描结果中找出 total_premium 最大的 k 对应的结果行。

    Args:
        sweep_results: sweep_k() 输出

    Returns:
        最优 {k, total_premium, fill_count, fill_rate}

    Raises:
        ValueError: sweep_results 为空时
    """
    if not sweep_results:
        raise ValueError("sweep_results 为空")
    return max(sweep_results, key=lambda r: r["total_premium"])


def print_report(trades: list[dict], sweep_results: list[dict],
                 k_star: dict) -> str:
    """生成人类可读的优化报告。

    Args:
        trades:        enrich_with_option_data() 输出
        sweep_results: sweep_k() 输出（全量）
        k_star:        find_optimal_k() 输出

    Returns:
        报告字符串（可直接 print 或写入文件）
    """
    valid = [t for t in trades if t.get("data_complete")]
    n_valid = len(valid)
    n_skip = len(trades) - n_valid

    baseline = next((r for r in sweep_results if abs(r["k"] - 1.0) < 0.001), None)

    # 分层最优 k
    layer_lines = []
    for layer in ["A", "B1", "B2", "B3", "B4"]:
        layer_trades = [t for t in valid if t["layer"] == layer]
        if not layer_trades:
            continue
        layer_sweep = sweep_k(layer_trades)
        lk = find_optimal_k(layer_sweep) if layer_sweep else None
        if lk:
            layer_lines.append(
                f"  {layer:<4} ({len(layer_trades)} 笔)  "
                f"k={lk['k']:.2f}  "
                f"成交率={lk['fill_rate']*100:.0f}%  "
                f"总权利金={lk['total_premium']:.4f}"
            )

    lines = [
        "=" * 60,
        "Lambda 策略 · 入场限价优化报告",
        "=" * 60,
        f"有效交易: {n_valid} 笔  （跳过 {n_skip} 笔数据不完整）",
        "",
        "── 全局最优 ──",
        f"k_star        : {k_star['k']:.2f}",
        f"成交率        : {k_star['fill_rate']*100:.1f}%"
        f"  ({k_star['fill_count']}/{n_valid} 笔)",
        f"总权利金      : {k_star['total_premium']:.4f}",
    ]

    if baseline:
        gain = k_star["total_premium"] - baseline["total_premium"]
        pct = gain / baseline["total_premium"] * 100 if baseline["total_premium"] else 0
        miss = baseline["fill_count"] - k_star["fill_count"]
        lines += [
            "",
            "── 对比市价单 (k=1.0) ──",
            f"市价总权利金  : {baseline['total_premium']:.4f}",
            f"额外收益      : +{gain:.4f}  (+{pct:.1f}%)",
            f"损失成交笔数  : {miss} 笔",
        ]

    if layer_lines:
        lines += ["", "── 分层最优 k ──"] + layer_lines

    lines += [
        "",
        "── 实盘操作规则 ──",
        f"周一收盘查期权收盘价 P，",
        f"周二开盘挂限价卖单 = P × {k_star['k']:.2f}",
        "GTC 持续至周五收盘；整周未成交则放弃本周。",
        "=" * 60,
    ]

    return "\n".join(lines)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Lambda 策略入场限价优化")
    parser.add_argument("--ticker", default="TQQQ", help="标的代码（默认 TQQQ）")
    parser.add_argument("--output", default=None, help="报告输出路径（默认打印到控制台）")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "output", f"{args.ticker.upper()}.json")
    if not os.path.exists(json_path):
        print(f"错误：{json_path} 不存在，请先运行: python run.py {args.ticker}")
        return 1

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    trades = get_signal_trades(data["weeks"])
    print(f"信号交易: {len(trades)} 笔")

    # 数据来源优先级：本地 DB > S3 > REST API
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "output", "market_data.duckdb")
    if os.path.exists(db_path):
        print("数据来源: 本地 DuckDB")
        enriched = enrich_with_db(trades, ticker=args.ticker.upper())
    elif os.environ.get("MASSIVE_S3_ACCESS_KEY"):
        from flat_file_fetcher import make_s3_client
        s3_client = make_s3_client()
        print("数据来源: Flat Files（S3）")
        enriched = enrich_with_flat_files(trades, s3_client, ticker=args.ticker.upper())
    else:
        api_key = os.environ.get("MASSIVE_API_KEY")
        if not api_key:
            print("错误：未找到本地 DB，且未设置 S3 或 REST API 凭据")
            return 1
        print("数据来源: REST API（近 4 个月）")
        enriched = enrich_with_option_data(trades, api_key, ticker=args.ticker.upper())
    valid_count = sum(1 for t in enriched if t["data_complete"])
    print(f"数据完整: {valid_count} / {len(enriched)} 笔")

    results = sweep_k(enriched)
    if not results:
        print("无有效数据，无法优化")
        return 1

    k_star = find_optimal_k(results)
    report = print_report(enriched, results, k_star)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"报告已保存: {args.output}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
