"""notify.py 单元测试"""
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notify import scan_png_files, send_photo, process_and_send, main


class TestScanPngFiles:
    """PNG 文件扫描"""

    def test_finds_png_files(self, tmp_path):
        """找到目录中的 PNG 文件，忽略其他格式"""
        (tmp_path / "a.png").write_text("")
        (tmp_path / "b.png").write_text("")
        (tmp_path / "data.json").write_text("{}")
        result = scan_png_files(str(tmp_path))
        assert len(result) == 2
        assert all(p.endswith(".png") for p in result)
        # 返回排序后的绝对路径
        assert result == sorted(result)
        assert all(os.path.isabs(p) for p in result)

    def test_empty_dir(self, tmp_path):
        """空目录返回空列表"""
        result = scan_png_files(str(tmp_path))
        assert result == []

    def test_no_png(self, tmp_path):
        """目录中无 PNG 时返回空列表"""
        (tmp_path / "data.json").write_text("{}")
        (tmp_path / "report.html").write_text("")
        result = scan_png_files(str(tmp_path))
        assert result == []


class TestSendPhoto:
    """Telegram sendPhoto 封装"""

    @patch("builtins.open", MagicMock())
    @patch("notify.requests.post")
    def test_send_success(self, mock_post):
        """API 返回 ok=True 时返回 True"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ok": True}
        mock_post.return_value = mock_resp
        assert send_photo("tok", "123", "/fake/img.png") is True

    @patch("builtins.open", MagicMock())
    @patch("notify.requests.post")
    def test_send_api_error(self, mock_post):
        """API 返回 ok=False 时返回 False"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ok": False, "description": "bad request"}
        mock_post.return_value = mock_resp
        assert send_photo("tok", "123", "/fake/img.png") is False

    @patch("builtins.open", MagicMock())
    @patch("notify.requests.post")
    def test_send_network_error(self, mock_post):
        """网络异常时捕获并返回 False"""
        mock_post.side_effect = ConnectionError("timeout")
        assert send_photo("tok", "123", "/fake/img.png") is False


class TestProcessAndSend:
    """扫描-发送-删除 流程"""

    @patch("notify.send_photo", return_value=True)
    def test_send_and_delete(self, mock_send, tmp_path):
        """发送成功后删除文件"""
        png = tmp_path / "test.png"
        png.write_text("")
        sent, failed = process_and_send("tok", "123", str(tmp_path))
        assert sent == 1
        assert failed == 0
        assert not png.exists()

    @patch("notify.send_photo", return_value=False)
    def test_send_fail_keeps_file(self, mock_send, tmp_path):
        """发送失败时保留文件"""
        png = tmp_path / "test.png"
        png.write_text("")
        sent, failed = process_and_send("tok", "123", str(tmp_path))
        assert sent == 0
        assert failed == 1
        assert png.exists()

    @patch("notify.send_photo")
    def test_dry_run(self, mock_send, tmp_path):
        """dry_run 模式不发送、不删除"""
        png = tmp_path / "test.png"
        png.write_text("")
        sent, failed = process_and_send("tok", "123", str(tmp_path), dry_run=True)
        mock_send.assert_not_called()
        assert sent == 1
        assert failed == 0
        assert png.exists()

    def test_no_files(self, tmp_path):
        """无文件时返回 (0, 0)"""
        sent, failed = process_and_send("tok", "123", str(tmp_path))
        assert sent == 0
        assert failed == 0


class TestMain:
    """CLI 入口"""

    def test_missing_token(self, capsys):
        """缺少 TELEGRAM_BOT_TOKEN 时退出并提示"""
        env = {k: v for k, v in os.environ.items()
               if k not in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")}
        with patch.dict(os.environ, env, clear=True), \
             patch("sys.argv", ["notify.py"]), \
             pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
        assert "TELEGRAM_BOT_TOKEN" in capsys.readouterr().err

    def test_missing_chat_id(self, capsys):
        """缺少 TELEGRAM_CHAT_ID 时退出并提示"""
        env = {k: v for k, v in os.environ.items()
               if k not in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")}
        env["TELEGRAM_BOT_TOKEN"] = "fake-token"
        with patch.dict(os.environ, env, clear=True), \
             patch("sys.argv", ["notify.py"]), \
             pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
        assert "TELEGRAM_CHAT_ID" in capsys.readouterr().err
