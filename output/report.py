"""报告生成：JSON 组装 + HTML 嵌入。"""
import json
import logging
import os

from config import TICKER

logger = logging.getLogger(__name__)

# output/ 既是 Python 包也是数据目录
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def build_report_data(*, tiers, summary, tier_stats, latest, weeks,
                      daily_bars, market, data_range, generated,
                      otm_config) -> dict:
    """将各模块计算结果组装成 JSON 结构。"""
    return {
        "ticker": TICKER,
        "generated": generated,
        "data_range": data_range,
        "otm_config": otm_config,
        "summary": summary,
        "tiers": tier_stats,
        "latest": latest,
        "weeks": weeks,
        "daily_bars": daily_bars,
        "market": market,
    }


def save_json(data: dict) -> str:
    """保存 JSON 到 output/ 目录，返回文件路径。"""
    json_path = os.path.join(_THIS_DIR, f"{TICKER}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON 已保存: {json_path}")
    return json_path


def load_template() -> str | None:
    """读取 template.html 模板。"""
    template_path = os.path.join(_THIS_DIR, "template.html")
    if not os.path.exists(template_path):
        logger.warning("template.html 不存在")
        return None
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def render_html(data: dict) -> str | None:
    """将策略结果内嵌到模板，生成 output/{TICKER}.html，返回文件路径。"""
    template_html = load_template()
    if not template_html:
        return None

    html_path = os.path.join(_THIS_DIR, f"{TICKER}.html")
    data_str = json.dumps(data, ensure_ascii=False, indent=2)
    marker = "/* EMBEDDED_DATA_PLACEHOLDER */"
    if marker not in template_html:
        logger.warning("template.html 中未找到内嵌数据占位符")
        return None

    html = template_html.replace(
        marker, f'EMBEDDED_DATA["{TICKER}"] = {data_str};\n' + marker)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"HTML 已生成: {html_path}")
    return html_path
