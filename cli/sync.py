"""数据同步入口"""
import logging
import os

from data.sync.orchestrator import ensure_synced


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    if not api_key:
        print("警告：未设置 MASSIVE_API_KEY，跳过股票数据同步")
    ensure_synced(api_key)


if __name__ == "__main__":
    main()
