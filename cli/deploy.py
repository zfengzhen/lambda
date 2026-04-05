"""部署入口：读取 HTML → 密码包装 → Cloudflare → Telegram"""
import logging
import os
import sys

from config import TICKER
from output.deploy import wrap_with_password, deploy_to_cloudflare, send_telegram

logger = logging.getLogger(__name__)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    html_path = os.path.join(OUTPUT_DIR, f"{TICKER}.html")
    if not os.path.exists(html_path):
        logger.error(f"文件不存在: {html_path}")
        sys.exit(1)

    password = os.environ.get("LAMBDA_DEPLOY_PASSWORD", "")
    if not password:
        logger.error("缺少环境变量 LAMBDA_DEPLOY_PASSWORD")
        sys.exit(1)

    with open(html_path, "r", encoding="utf-8") as f:
        raw_html = f.read()
    logger.info(f"读取 {html_path} ({len(raw_html)} bytes)")

    wrapped = wrap_with_password(raw_html, password)
    logger.info(f"密码包装完成 ({len(wrapped)} bytes)")

    url = deploy_to_cloudflare(wrapped)
    send_telegram(url)
    logger.info(f"部署完成: {url}")


if __name__ == "__main__":
    main()
