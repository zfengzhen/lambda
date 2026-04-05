"""deploy.py 单元测试"""
import hashlib


def test_wrap_with_password_contains_hash():
    """包装后的 HTML 应包含密码的 SHA-256 hash，不包含密码明文"""
    from deploy import wrap_with_password

    html = "<html><body>Hello</body></html>"
    password = "test123"
    result = wrap_with_password(html, password)

    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    assert pw_hash in result, "应包含密码 SHA-256 hash"
    assert password not in result, "不应包含密码明文"


def test_wrap_with_password_contains_encoded_content():
    """包装后的 HTML 应包含 base64 编码的原始内容"""
    import base64
    from deploy import wrap_with_password

    html = "<html><body>策略报告</body></html>"
    result = wrap_with_password(html, "pw")

    encoded = base64.b64encode(html.encode("utf-8")).decode("ascii")
    assert encoded in result, "应包含 base64 编码的原始 HTML"



def test_wrap_with_password_has_auth_screen():
    """包装后的 HTML 应包含密码输入界面"""
    from deploy import wrap_with_password

    result = wrap_with_password("<html></html>", "pw")
    assert 'id="auth-screen"' in result
    assert 'type="password"' in result
    assert "localStorage" in result


def test_send_telegram_missing_env(monkeypatch):
    """缺少 Telegram 环境变量时应静默跳过（不阻塞部署）"""
    from deploy import send_telegram

    monkeypatch.delenv("LAMBDA_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("LAMBDA_TELEGRAM_CHAT_ID", raising=False)

    # 不应抛异常，静默跳过
    send_telegram("https://example.netlify.app")


def test_send_telegram_calls_api(monkeypatch):
    """应调用 Telegram Bot API sendMessage"""
    from unittest.mock import MagicMock
    from deploy import send_telegram

    monkeypatch.setenv("LAMBDA_TELEGRAM_BOT_TOKEN", "fake-token")
    monkeypatch.setenv("LAMBDA_TELEGRAM_CHAT_ID", "12345")

    mock_post = MagicMock()
    mock_post.return_value.status_code = 200
    monkeypatch.setattr("deploy.requests.post", mock_post)

    send_telegram("https://example.netlify.app")

    mock_post.assert_called_once()
    call_url = mock_post.call_args[0][0]
    assert "fake-token" in call_url
    assert "sendMessage" in call_url
