"""
入口脚本：同步数据 → 策略计算 → 输出 JSON → 内嵌到 HTML → 截图 PNG

用法:
    python run.py              # 默认 TQQQ
    python run.py TQQQ QQQ    # 多标的批量运行
"""
import argparse
import base64
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
)

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
DATA_DIR = OUTPUT_DIR


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


def fetch_daily_bars(ticker: str, api_key: str) -> pd.DataFrame | None:
    """从 DuckDB 读取日K（自动触发增量同步）。返回含指标的 DataFrame，失败返回 None。"""
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


def compute_strategy(ticker: str, df: pd.DataFrame) -> dict | None:
    """DataFrame → 策略计算 → 返回结果 dict"""
    df["hist_vol"] = df["close"].rolling(window=21, min_periods=21).apply(
        lambda x: compute_hist_vol(pd.Series(x.values), window=20), raw=False
    )
    df["prev_macd"] = df["macd"].shift(1)
    df = df.dropna(subset=["ma60"]).reset_index(drop=True)

    logger.info(f"[{ticker}] 有效数据 {len(df)} 行")

    otm_a, otm_b, otm_c = get_otm_for_ticker(ticker)
    logger.info(f"[{ticker}] OTM: A={otm_a*100:.0f}% B={otm_b*100:.0f}% C={otm_c*100:.0f}%")

    weekly_rows = group_by_week(df)
    weeks = backtest_weeks(weekly_rows, df, otm_a=otm_a, otm_b=otm_b, otm_c=otm_c)
    summary = compute_summary(weeks)
    tiers = compute_tiers(weeks, otm_a=otm_a, otm_b=otm_b, otm_c=otm_c)
    latest = compute_latest(weekly_rows, df, otm_a=otm_a, otm_b=otm_b, otm_c=otm_c)

    dates = pd.to_datetime(df["date"])
    return {
        "ticker": ticker,
        "generated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "data_range": [dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")],
        "otm_config": {"otm_a": otm_a, "otm_b": otm_b, "otm_c": otm_c},
        "summary": summary,
        "tiers": tiers,
        "latest": latest,
        "weeks": weeks,
    }


def save_json(ticker: str, result: dict):
    """保存 JSON 到 output/ 目录"""
    os.makedirs(DATA_DIR, exist_ok=True)
    json_path = os.path.join(DATA_DIR, f"{ticker}.json")
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

    # 过滤掉 daily_bars（如有），避免嵌入过大数据
    html_data = {k: v for k, v in result.items() if k != "daily_bars"}
    data_str = json.dumps(html_data, ensure_ascii=False, indent=2)

    marker = "/* EMBEDDED_DATA_PLACEHOLDER */"
    if marker not in template_html:
        logger.warning("template.html 中未找到内嵌数据占位符")
        return

    html = template_html.replace(marker, f'EMBEDDED_DATA["{ticker}"] = {data_str};\n' + marker)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"[{ticker}] HTML 已生成: {html_path}")


def capture_screenshot(ticker: str, result: dict | None):
    """用 Playwright 截图生成 PNG。缺失时跳过。"""
    if result is None or not isinstance(result.get("latest"), dict):
        logger.info("无策略结果，跳过截图")
        return
    date_str = result["latest"].get("date")
    if not date_str:
        logger.warning("[截图] latest.date 缺失，跳过截图")
        return

    html_path = os.path.join(OUTPUT_DIR, f"{ticker}.html")
    if not os.path.exists(html_path):
        logger.warning(f"{ticker}.html 不存在，跳过截图")
        return

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("[截图] playwright 未安装，跳过。请运行: pip install playwright")
        return

    png_name = f"lambda-strategy-{ticker}-{date_str}.png"
    png_path = os.path.join(OUTPUT_DIR, png_name)
    file_url = "file://" + os.path.abspath(html_path)

    export_js = """
    () => new Promise((resolve, reject) => {
        let waited = 0;
        const waitCanvas = setInterval(() => {
            if (typeof html2canvas !== 'undefined') {
                clearInterval(waitCanvas);
                doExport();
            } else if (waited > 5000) {
                clearInterval(waitCanvas);
                reject(new Error('html2canvas 加载超时'));
            }
            waited += 100;
        }, 100);

        function doExport() {
            const ticker = document.getElementById('tickerName').textContent || 'TQQQ';
            const tmp = document.createElement('div');
            tmp.style.cssText = 'position:fixed;left:-9999px;top:0;width:1500px;background:#0a0e17;padding:20px;color:#c8d0dc;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;line-height:1.6';

            const headerHtml = `<div style="display:flex;align-items:center;gap:14px;margin-bottom:12px">
                <span style="color:#e8ecf1;font-size:26px;font-weight:bold">Lambda 策略系统</span>
                <span style="color:#4fc3f7;font-size:20px;font-weight:bold">${ticker}</span>
            </div>`;
            tmp.innerHTML = headerHtml;
            tmp.appendChild(document.getElementById('overviewRow').cloneNode(true));

            const box = document.querySelector('.box');
            tmp.appendChild(box.cloneNode(true));

            const label = document.createElement('div');
            label.style.cssText = 'color:#8b95a5;font-size:14px;margin:16px 0 8px';
            label.textContent = '最近 8 周操作明细';
            tmp.appendChild(label);

            const tableWrap = document.createElement('div');
            const origTable = document.querySelector('table.weekly');
            const tbl = origTable.cloneNode(true);
            const tbody = tbl.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            rows.forEach((r, i) => { if (i >= 8) r.remove(); });
            tableWrap.appendChild(tbl);
            tmp.appendChild(tableWrap);

            tmp.appendChild(document.querySelector('.risk-note').cloneNode(true));
            document.body.appendChild(tmp);

            html2canvas(tmp, {
                backgroundColor: '#0a0e17',
                scale: 2,
                useCORS: true,
                width: 1500,
                windowWidth: 1500,
            }).then(canvas => {
                document.body.removeChild(tmp);
                resolve(canvas.toDataURL('image/png'));
            }).catch(err => {
                document.body.removeChild(tmp);
                reject(err);
            });
        }
    })
    """

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1500, "height": 900})
            page.goto(file_url, wait_until="networkidle")
            page.wait_for_selector("#overviewRow .stat", timeout=10000)
            data_url = page.evaluate(export_js)

        header = "data:image/png;base64,"
        if data_url.startswith(header):
            img_data = base64.b64decode(data_url[len(header):])
            with open(png_path, "wb") as f:
                f.write(img_data)
            logger.info(f"[截图] 已保存: {png_path}")
        else:
            logger.warning("[截图] canvas 返回格式异常")
    except Exception as e:
        err_msg = str(e)
        if "Executable doesn't exist" in err_msg or "browserType.launch" in err_msg:
            logger.warning("[截图] Chromium 未安装，跳过。请运行: playwright install chromium")
        else:
            logger.warning(f"[截图] 截图失败: {e}")


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

        df = fetch_daily_bars(ticker, api_key)
        if df is None:
            continue

        result = compute_strategy(ticker, df)
        if result is None:
            continue

        save_json(ticker, result)

        if template_html:
            embed_to_html(ticker, result, template_html)

        capture_screenshot(ticker, result)

    logger.info("全部完成")


if __name__ == "__main__":
    main()
