"""Telegram 截图推送模块

扫描 output/ 目录中的 PNG 截图，通过 Telegram Bot API 发送后删除。
"""
import argparse
import glob
import logging
import os
import sys

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── 路径常量 ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")


def scan_png_files(directory: str) -> list[str]:
    """扫描目录中的 PNG 文件，返回排序后的绝对路径列表"""
    pattern = os.path.join(directory, "*.png")
    return sorted(glob.glob(pattern))


def send_photo(token: str, chat_id: str, image_path: str) -> bool:
    """通过 Telegram Bot API 发送图片

    Args:
        token: Bot token
        chat_id: 目标聊天 ID
        image_path: 图片文件绝对路径

    Returns:
        发送成功返回 True，失败返回 False
    """
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(image_path, "rb") as f:
            resp = requests.post(url, data={"chat_id": chat_id}, files={"photo": f}, timeout=30)
        result = resp.json()
        if result.get("ok"):
            logger.info("发送成功: %s", os.path.basename(image_path))
            return True
        else:
            logger.error("API 错误: %s", result.get("description", "未知错误"))
            return False
    except Exception as e:
        logger.error("发送失败 %s: %s", os.path.basename(image_path), e)
        return False


def process_and_send(
    token: str, chat_id: str, directory: str, dry_run: bool = False
) -> tuple[int, int]:
    """扫描 PNG 并逐一发送，成功后删除源文件

    Args:
        token: Bot token
        chat_id: 目标聊天 ID
        directory: 扫描目录
        dry_run: 仅日志，不实际发送和删除

    Returns:
        (sent_count, failed_count)
    """
    files = scan_png_files(directory)
    if not files:
        logger.info("未找到 PNG 文件")
        return (0, 0)

    sent = 0
    failed = 0
    for path in files:
        name = os.path.basename(path)
        if dry_run:
            logger.info("[DRY RUN] 将发送: %s", name)
            sent += 1
            continue

        if send_photo(token, chat_id, path):
            try:
                os.remove(path)
            except OSError as e:
                logger.warning("删除失败 %s: %s", name, e)
            sent += 1
        else:
            failed += 1

    logger.info("完成: 发送 %d, 失败 %d", sent, failed)
    return (sent, failed)


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(description="发送 output/ 截图到 Telegram")
    parser.add_argument("--dry-run", action="store_true", help="仅打印，不实际发送")
    args = parser.parse_args()

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("错误: 未设置环境变量 TELEGRAM_BOT_TOKEN", file=sys.stderr)
        sys.exit(1)

    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not chat_id:
        print("错误: 未设置环境变量 TELEGRAM_CHAT_ID", file=sys.stderr)
        sys.exit(1)

    sent, failed = process_and_send(token, chat_id, OUTPUT_DIR, dry_run=args.dry_run)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
