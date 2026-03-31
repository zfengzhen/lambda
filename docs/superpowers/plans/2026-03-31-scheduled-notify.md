# Scheduled Notify 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `notify.py` 模块，扫描 `output/*.png` 通过 Telegram Bot 发送截图并删除已发文件；配合 launchd plist 实现定时 `run.py && notify.py` 调度。

**Architecture:** 独立的 `notify.py` 脚本，不修改 `run.py`。通过环境变量读取 Telegram 配置，用 `requests` 调用 Bot API `sendPhoto`。launchd plist 串联两个脚本执行。

**Tech Stack:** Python 3, requests (已有), glob (标准库), macOS launchd

---

## 文件结构

| 文件 | 操作 | 职责 |
|------|------|------|
| `notify.py` | 新建 | 扫描 PNG → Telegram 发送 → 删除文件 |
| `tests/test_notify.py` | 新建 | notify.py 单元测试 |
| `com.lambda.scheduled-notify.plist` | 新建 | launchd 配置模板（项目内保存，用户复制到 ~/Library/LaunchAgents/） |
| `docs/scheduled-notify.md` | 新建 | 使用说明文档 |

---

### Task 1: notify.py — 核心推送逻辑（TDD）

**Files:**
- Create: `notify.py`
- Create: `tests/test_notify.py`

- [ ] **Step 1: 编写 test_notify.py — 测试 PNG 扫描**

```python
"""notify.py 单元测试"""
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notify import scan_png_files


class TestScanPngFiles:
    """扫描 output/ 下的 PNG 文件"""

    def test_finds_png_files(self, tmp_path):
        """能找到目录下的 PNG 文件"""
        (tmp_path / "a.png").write_bytes(b"fake")
        (tmp_path / "b.png").write_bytes(b"fake")
        (tmp_path / "c.json").write_text("{}")
        result = scan_png_files(str(tmp_path))
        assert len(result) == 2
        assert all(f.endswith(".png") for f in result)

    def test_empty_dir(self, tmp_path):
        """空目录返回空列表"""
        result = scan_png_files(str(tmp_path))
        assert result == []

    def test_no_png(self, tmp_path):
        """没有 PNG 文件返回空列表"""
        (tmp_path / "data.json").write_text("{}")
        result = scan_png_files(str(tmp_path))
        assert result == []
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd /Users/fengzhen.zhang/alpha/lambda-worktrees/feature-scheduled-notify && source .venv/bin/activate && python -m pytest tests/test_notify.py::TestScanPngFiles -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notify'`

- [ ] **Step 3: 实现 scan_png_files**

```python
"""
Telegram 截图推送：扫描 output/*.png → 发送 → 删除

用法:
    python notify.py              # 发送所有待推送截图
    python notify.py --dry-run    # 仅打印待发送文件
"""
import argparse
import glob
import logging
import os
import sys

import requests

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")


def scan_png_files(directory: str) -> list[str]:
    """扫描目录下所有 PNG 文件，返回绝对路径列表"""
    pattern = os.path.join(directory, "*.png")
    return sorted(glob.glob(pattern))
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m pytest tests/test_notify.py::TestScanPngFiles -v`
Expected: 3 passed

- [ ] **Step 5: 编写 Telegram 发送测试**

在 `tests/test_notify.py` 追加：

```python
from notify import send_photo


class TestSendPhoto:
    """Telegram sendPhoto API 调用"""

    @patch("notify.requests.post")
    def test_send_success(self, mock_post, tmp_path):
        """发送成功返回 True"""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"ok": True, "result": {}},
        )
        png = tmp_path / "test.png"
        png.write_bytes(b"\x89PNG fake image data")
        result = send_photo("token123", "chat456", str(png))
        assert result is True
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "sendPhoto" in call_args[0][0]
        assert call_args[1]["data"]["chat_id"] == "chat456"

    @patch("notify.requests.post")
    def test_send_api_error(self, mock_post, tmp_path):
        """API 返回 ok=false 时返回 False"""
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"ok": False, "description": "chat not found"},
        )
        png = tmp_path / "test.png"
        png.write_bytes(b"\x89PNG fake")
        result = send_photo("token123", "chat456", str(png))
        assert result is False

    @patch("notify.requests.post")
    def test_send_network_error(self, mock_post, tmp_path):
        """网络异常返回 False"""
        mock_post.side_effect = requests.ConnectionError("timeout")
        png = tmp_path / "test.png"
        png.write_bytes(b"\x89PNG fake")
        result = send_photo("token123", "chat456", str(png))
        assert result is False
```

- [ ] **Step 6: 运行测试确认失败**

Run: `python -m pytest tests/test_notify.py::TestSendPhoto -v`
Expected: FAIL — `ImportError: cannot import name 'send_photo'`

- [ ] **Step 7: 实现 send_photo**

在 `notify.py` 追加：

```python
def send_photo(token: str, chat_id: str, image_path: str) -> bool:
    """通过 Telegram Bot API 发送图片，成功返回 True"""
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(image_path, "rb") as photo:
            resp = requests.post(url, data={"chat_id": chat_id}, files={"photo": photo})
        result = resp.json()
        if result.get("ok"):
            return True
        logger.error(f"Telegram API 错误: {result.get('description', '未知')}")
        return False
    except Exception as e:
        logger.error(f"发送失败: {e}")
        return False
```

- [ ] **Step 8: 运行测试确认通过**

Run: `python -m pytest tests/test_notify.py::TestSendPhoto -v`
Expected: 3 passed

- [ ] **Step 9: 编写 process_and_send 主流程测试**

在 `tests/test_notify.py` 追加：

```python
from notify import process_and_send


class TestProcessAndSend:
    """主流程：扫描 → 发送 → 删除"""

    @patch("notify.send_photo", return_value=True)
    def test_send_and_delete(self, mock_send, tmp_path):
        """发送成功后删除文件"""
        png = tmp_path / "shot.png"
        png.write_bytes(b"\x89PNG")
        sent, failed = process_and_send("tok", "cid", str(tmp_path), dry_run=False)
        assert sent == 1
        assert failed == 0
        assert not png.exists()

    @patch("notify.send_photo", return_value=False)
    def test_send_fail_keeps_file(self, mock_send, tmp_path):
        """发送失败时保留文件"""
        png = tmp_path / "shot.png"
        png.write_bytes(b"\x89PNG")
        sent, failed = process_and_send("tok", "cid", str(tmp_path), dry_run=False)
        assert sent == 0
        assert failed == 1
        assert png.exists()

    def test_dry_run(self, tmp_path):
        """dry-run 不发送也不删除"""
        png = tmp_path / "shot.png"
        png.write_bytes(b"\x89PNG")
        sent, failed = process_and_send("tok", "cid", str(tmp_path), dry_run=True)
        assert sent == 0
        assert failed == 0
        assert png.exists()

    def test_no_files(self, tmp_path):
        """无 PNG 时返回 (0, 0)"""
        sent, failed = process_and_send("tok", "cid", str(tmp_path), dry_run=False)
        assert sent == 0
        assert failed == 0
```

- [ ] **Step 10: 运行测试确认失败**

Run: `python -m pytest tests/test_notify.py::TestProcessAndSend -v`
Expected: FAIL — `ImportError: cannot import name 'process_and_send'`

- [ ] **Step 11: 实现 process_and_send**

在 `notify.py` 追加：

```python
def process_and_send(token: str, chat_id: str, directory: str, dry_run: bool = False) -> tuple[int, int]:
    """扫描 PNG → 发送 → 删除。返回 (成功数, 失败数)"""
    files = scan_png_files(directory)
    if not files:
        logger.info("无待发送的 PNG 文件")
        return 0, 0

    logger.info(f"发现 {len(files)} 个 PNG 文件")
    sent, failed = 0, 0

    for path in files:
        name = os.path.basename(path)
        if dry_run:
            logger.info(f"[dry-run] 待发送: {name}")
            continue

        if send_photo(token, chat_id, path):
            os.remove(path)
            logger.info(f"已发送并删除: {name}")
            sent += 1
        else:
            logger.warning(f"发送失败，保留文件: {name}")
            failed += 1

    return sent, failed
```

- [ ] **Step 12: 运行测试确认通过**

Run: `python -m pytest tests/test_notify.py::TestProcessAndSend -v`
Expected: 4 passed

- [ ] **Step 13: 编写 main 入口测试（环境变量）**

在 `tests/test_notify.py` 追加：

```python
from notify import main


class TestMain:
    """CLI 入口与环境变量"""

    def test_missing_token(self, capsys):
        """缺少 TELEGRAM_BOT_TOKEN 时 exit 1"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code == 1
        captured = capsys.readouterr()
        assert "TELEGRAM_BOT_TOKEN" in captured.err

    def test_missing_chat_id(self, capsys):
        """缺少 TELEGRAM_CHAT_ID 时 exit 1"""
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "tok"}, clear=True):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code == 1
        captured = capsys.readouterr()
        assert "TELEGRAM_CHAT_ID" in captured.err
```

在 `tests/test_notify.py` 顶部 import 补充 `import os`。

- [ ] **Step 14: 运行测试确认失败**

Run: `python -m pytest tests/test_notify.py::TestMain -v`
Expected: FAIL — `ImportError: cannot import name 'main'`

- [ ] **Step 15: 实现 main 入口**

在 `notify.py` 追加：

```python
def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(description="Lambda 策略截图 → Telegram 推送")
    parser.add_argument("--dry-run", action="store_true", help="仅打印待发送文件，不实际发送")
    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("错误: 未设置环境变量 TELEGRAM_BOT_TOKEN", file=sys.stderr)
        sys.exit(1)

    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not chat_id:
        print("错误: 未设置环境变量 TELEGRAM_CHAT_ID", file=sys.stderr)
        sys.exit(1)

    sent, failed = process_and_send(token, chat_id, OUTPUT_DIR, dry_run=args.dry_run)

    if args.dry_run:
        logger.info("dry-run 模式，未实际发送")
    else:
        logger.info(f"完成: 发送 {sent} 个，失败 {failed} 个")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 16: 运行全部 notify 测试**

Run: `python -m pytest tests/test_notify.py -v`
Expected: ALL passed

- [ ] **Step 17: Commit**

```bash
git add notify.py tests/test_notify.py
git commit -m "[feature/scheduled-notify][功能] 新增 notify.py Telegram 截图推送模块"
```

---

### Task 2: launchd plist 模板

**Files:**
- Create: `com.lambda.scheduled-notify.plist`

- [ ] **Step 1: 创建 plist 文件**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.lambda.scheduled-notify</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-c</string>
        <string>cd /Users/fengzhen.zhang/alpha/lambda &amp;&amp; source .venv/bin/activate &amp;&amp; python run.py &amp;&amp; python notify.py</string>
    </array>

    <!-- 每天 04:30 执行（北京时间，约对应美东收盘后）-->
    <!-- 用户可自定义：修改 Hour/Minute，或改为 Weekday 等 -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>4</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>/Users/fengzhen.zhang/alpha/lambda/output/scheduled-notify.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/fengzhen.zhang/alpha/lambda/output/scheduled-notify.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>MASSIVE_API_KEY</key>
        <string>YOUR_MASSIVE_API_KEY</string>
        <key>TELEGRAM_BOT_TOKEN</key>
        <string>YOUR_BOT_TOKEN</string>
        <key>TELEGRAM_CHAT_ID</key>
        <string>YOUR_CHAT_ID</string>
    </dict>
</dict>
</plist>
```

- [ ] **Step 2: Commit**

```bash
git add com.lambda.scheduled-notify.plist
git commit -m "[feature/scheduled-notify][功能] 新增 launchd 定时调度配置模板"
```

---

### Task 3: 使用说明文档

**Files:**
- Create: `docs/scheduled-notify.md`

- [ ] **Step 1: 编写文档**

内容包含：
1. Telegram Bot 创建步骤（BotFather /newbot → 获取 token）
2. 获取 chat_id（发消息给 bot → 调用 getUpdates）
3. 环境变量配置（TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID）
4. launchd 安装：复制 plist → 修改路径和环境变量 → `launchctl load`
5. 手动运行：`python notify.py` / `python notify.py --dry-run`
6. 日志查看：`tail -f output/scheduled-notify.log`
7. 卸载：`launchctl unload`

- [ ] **Step 2: Commit**

```bash
git add docs/scheduled-notify.md
git commit -m "[feature/scheduled-notify][文档] 新增定时推送使用说明"
```

---

### Task 4: 全量验证

- [ ] **Step 1: 运行全部测试**

Run: `python -m pytest tests/ -v`
Expected: ALL passed（包括已有测试和新增测试）

- [ ] **Step 2: 手动验证 notify.py**

```bash
# 在 output/ 放一个测试 PNG
cp /path/to/any.png output/test-notify.png

# dry-run 验证
TELEGRAM_BOT_TOKEN=test TELEGRAM_CHAT_ID=test python notify.py --dry-run
# 预期输出: [dry-run] 待发送: test-notify.png

# 实际发送验证（需配置真实 token 和 chat_id）
TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=xxx python notify.py
# 预期: 图片发送到 Telegram，output/test-notify.png 被删除
```

- [ ] **Step 3: 确认文件完整性**

检查新增文件列表：
- `notify.py`
- `tests/test_notify.py`
- `com.lambda.scheduled-notify.plist`
- `docs/scheduled-notify.md`
