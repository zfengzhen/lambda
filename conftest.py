"""pytest 全局配置：注册自定义 marker，默认跳过 online 测试。"""
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "online: 需要网络和 API Key 的在线验证测试"
    )


def pytest_collection_modifyitems(config, items):
    # 未指定 -m online 时自动跳过 online 标记的测试
    if config.getoption("-m", default="") != "online":
        skip_online = pytest.mark.skip(reason="需要 -m online 显式指定运行")
        for item in items:
            if "online" in item.keywords:
                item.add_marker(skip_online)
