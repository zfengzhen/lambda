# Netlify 自动部署 + Telegram 通知 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 每天 14:00 自动生成 TQQQ.html，密码包装后部署到 Netlify，成功后 Telegram 通知链接。

**Architecture:** 新建 `deploy.py`，读取 `output/TQQQ.html` → 注入前端密码锁 → ZIP 打包 → POST 到 Netlify Deploy API → Telegram Bot API 通知。修改 launchd plist 为每天执行并串联 `deploy.py`。

**Tech Stack:** Python 3, requests（已有）, hashlib/base64/zipfile/io（标准库）, Netlify REST API (ZIP deploy), Telegram Bot API

---

### Task 1: deploy.py — 密码包装函数

**Files:**
- Create: `deploy.py`
- Test: `tests/test_deploy.py`

- [ ] **Step 1: 创建 tests/test_deploy.py，编写密码包装测试**

```python
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m pytest tests/test_deploy.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'deploy'`

- [ ] **Step 3: 实现 wrap_with_password**

```python
"""
部署脚本：密码包装 → Netlify 部署 → Telegram 通知

用法:
    python deploy.py                    # 部署 output/TQQQ.html
    python deploy.py --ticker QQQ       # 部署 output/QQQ.html
"""
import base64
import hashlib
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")


def wrap_with_password(html: str, password: str) -> str:
    """将原始 HTML 用前端密码锁包装。

    密码 SHA-256 hash 嵌入页面，用户输入后前端比对。
    验证通过后 base64 解码原始 HTML 并渲染。
    localStorage 缓存验证状态 7 天。
    """
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    encoded = base64.b64encode(html.encode("utf-8")).decode("ascii")

    return f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lambda Report</title>
<style>
  body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }}
  #auth-screen {{
    display: flex; flex-direction: column; align-items: center;
    justify-content: center; height: 100vh; background: #f5f5f5;
  }}
  #auth-screen input[type="password"] {{
    padding: 12px 16px; font-size: 16px; border: 1px solid #ccc;
    border-radius: 8px; width: 240px; margin-bottom: 12px;
    outline: none; text-align: center;
  }}
  #auth-screen input[type="password"]:focus {{ border-color: #4a90d9; }}
  #auth-screen button {{
    padding: 10px 32px; font-size: 16px; background: #4a90d9;
    color: white; border: none; border-radius: 8px; cursor: pointer;
  }}
  #auth-screen button:hover {{ background: #357abd; }}
  #auth-screen .error {{ color: #e74c3c; margin-top: 8px; font-size: 14px; }}
</style>
</head>
<body>
<div id="auth-screen">
  <h2>Lambda Report</h2>
  <input type="password" id="pw-input" placeholder="输入密码" autofocus>
  <button onclick="verify()">确认</button>
  <div class="error" id="error-msg"></div>
</div>
<script>
const HASH = "{pw_hash}";
const DATA = "{encoded}";
const CACHE_KEY = "lambda_auth";
const CACHE_DAYS = 7;

async function sha256(text) {{
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}}

function unlock() {{
  // base64 解码并渲染原始 HTML
  var decoded = atob(DATA);
  var bytes = new Uint8Array(decoded.length);
  for (var i = 0; i < decoded.length; i++) bytes[i] = decoded.charCodeAt(i);
  var html = new TextDecoder("utf-8").decode(bytes);
  document.open();
  document.write(html);
  document.close();
}}

async function verify() {{
  var pw = document.getElementById("pw-input").value;
  var h = await sha256(pw);
  if (h === HASH) {{
    localStorage.setItem(CACHE_KEY, JSON.stringify({{hash: h, expires: Date.now() + CACHE_DAYS * 86400000}}));
    unlock();
  }} else {{
    document.getElementById("error-msg").textContent = "密码错误";
  }}
}}

// 回车提交
document.getElementById("pw-input").addEventListener("keydown", function(e) {{
  if (e.key === "Enter") verify();
}});

// 检查 localStorage 缓存
(function() {{
  try {{
    var cache = JSON.parse(localStorage.getItem(CACHE_KEY));
    if (cache && cache.hash === HASH && cache.expires > Date.now()) unlock();
  }} catch(e) {{}}
}})();
</script>
</body>
</html>"""
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m pytest tests/test_deploy.py -v`
Expected: 3 passed

- [ ] **Step 5: 提交**

```bash
git add deploy.py tests/test_deploy.py
git commit -m "[feature/netlify-deploy][功能] 密码包装函数 wrap_with_password"
```

---

### Task 2: deploy.py — Netlify 部署函数

**Files:**
- Modify: `deploy.py`
- Modify: `tests/test_deploy.py`

- [ ] **Step 1: 在 tests/test_deploy.py 添加部署函数测试**

```python
def test_build_deploy_zip():
    """构建的 ZIP 应包含 index.html"""
    from deploy import build_deploy_zip

    html = "<html>test</html>"
    zip_bytes = build_deploy_zip(html)

    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    assert "index.html" in zf.namelist()
    assert zf.read("index.html").decode("utf-8") == html


def test_deploy_to_netlify_missing_env(monkeypatch):
    """缺少环境变量时应抛出 ValueError"""
    import deploy

    monkeypatch.delenv("NETLIFY_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("NETLIFY_SITE_ID", raising=False)

    try:
        deploy.deploy_to_netlify("<html></html>")
        assert False, "应抛出 ValueError"
    except ValueError as e:
        assert "NETLIFY_AUTH_TOKEN" in str(e)
```

需在文件顶部添加 `import zipfile, io`。

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m pytest tests/test_deploy.py::test_build_deploy_zip tests/test_deploy.py::test_deploy_to_netlify_missing_env -v`
Expected: FAIL — `cannot import name 'build_deploy_zip'`

- [ ] **Step 3: 在 deploy.py 实现 build_deploy_zip 和 deploy_to_netlify**

```python
def build_deploy_zip(html: str) -> bytes:
    """将 HTML 打包为 ZIP（index.html），返回 ZIP 字节。"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.html", html)
    return buf.getvalue()


def deploy_to_netlify(html: str) -> str:
    """将 HTML 部署到 Netlify，返回站点 URL。

    环境变量：NETLIFY_AUTH_TOKEN, NETLIFY_SITE_ID
    使用 ZIP deploy 方式：POST /api/v1/sites/{site_id}/deploys
    """
    token = os.environ.get("NETLIFY_AUTH_TOKEN", "")
    site_id = os.environ.get("NETLIFY_SITE_ID", "")
    if not token:
        raise ValueError("缺少环境变量 NETLIFY_AUTH_TOKEN")
    if not site_id:
        raise ValueError("缺少环境变量 NETLIFY_SITE_ID")

    zip_bytes = build_deploy_zip(html)

    resp = requests.post(
        f"https://api.netlify.com/api/v1/sites/{site_id}/deploys",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/zip",
        },
        data=zip_bytes,
        timeout=60,
    )
    resp.raise_for_status()

    data = resp.json()
    url = data.get("ssl_url") or data.get("url") or f"https://{site_id}"
    logger.info(f"Netlify 部署成功: {url} (deploy_id={data.get('id')})")
    return url
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m pytest tests/test_deploy.py -v`
Expected: 5 passed

- [ ] **Step 5: 提交**

```bash
git add deploy.py tests/test_deploy.py
git commit -m "[feature/netlify-deploy][功能] Netlify ZIP 部署函数"
```

---

### Task 3: deploy.py — Telegram 通知 + main 入口

**Files:**
- Modify: `deploy.py`
- Modify: `tests/test_deploy.py`

- [ ] **Step 1: 在 tests/test_deploy.py 添加 Telegram 和 main 相关测试**

```python
def test_send_telegram_missing_env(monkeypatch):
    """缺少 Telegram 环境变量时应静默跳过（不阻塞部署）"""
    from deploy import send_telegram

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    # 不应抛异常，静默跳过
    send_telegram("https://example.netlify.app")


def test_send_telegram_calls_api(monkeypatch):
    """应调用 Telegram Bot API sendMessage"""
    from unittest.mock import MagicMock
    from deploy import send_telegram

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")

    mock_post = MagicMock()
    mock_post.return_value.status_code = 200
    monkeypatch.setattr("deploy.requests.post", mock_post)

    send_telegram("https://example.netlify.app")

    mock_post.assert_called_once()
    call_url = mock_post.call_args[0][0]
    assert "fake-token" in call_url
    assert "sendMessage" in call_url
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m pytest tests/test_deploy.py::test_send_telegram_missing_env tests/test_deploy.py::test_send_telegram_calls_api -v`
Expected: FAIL — `cannot import name 'send_telegram'`

- [ ] **Step 3: 在 deploy.py 实现 send_telegram 和 main**

```python
def send_telegram(url: str, ticker: str = "TQQQ"):
    """部署成功后发送 Telegram 通知。缺少环境变量时静默跳过。"""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("缺少 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID，跳过通知")
        return

    text = f"Lambda {ticker} 报告已更新\n{url}"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=15,
        )
        if resp.status_code == 200:
            logger.info("Telegram 通知已发送")
        else:
            logger.warning(f"Telegram 通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.warning(f"Telegram 通知异常: {e}")


def main():
    """入口：读取 HTML → 密码包装 → Netlify 部署 → Telegram 通知"""
    import argparse

    parser = argparse.ArgumentParser(description="部署策略报告到 Netlify")
    parser.add_argument("--ticker", default="TQQQ", help="标的代码（默认 TQQQ）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ticker = args.ticker.upper()
    html_path = os.path.join(OUTPUT_DIR, f"{ticker}.html")

    if not os.path.exists(html_path):
        logger.error(f"文件不存在: {html_path}")
        sys.exit(1)

    password = os.environ.get("DEPLOY_PASSWORD", "")
    if not password:
        logger.error("缺少环境变量 DEPLOY_PASSWORD")
        sys.exit(1)

    # 读取原始 HTML
    with open(html_path, "r", encoding="utf-8") as f:
        raw_html = f.read()
    logger.info(f"[{ticker}] 读取 {html_path} ({len(raw_html)} bytes)")

    # 密码包装
    wrapped = wrap_with_password(raw_html, password)
    logger.info(f"[{ticker}] 密码包装完成 ({len(wrapped)} bytes)")

    # 部署到 Netlify
    url = deploy_to_netlify(wrapped)

    # Telegram 通知
    send_telegram(url, ticker)

    logger.info(f"[{ticker}] 部署完成: {url}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m pytest tests/test_deploy.py -v`
Expected: 7 passed

- [ ] **Step 5: 提交**

```bash
git add deploy.py tests/test_deploy.py
git commit -m "[feature/netlify-deploy][功能] Telegram 通知 + main 入口"
```

---

### Task 4: 更新 launchd plist 为每天 14:00 + 串联 deploy.py

**Files:**
- Modify: `com.lambda.scheduled-notify.plist`

- [ ] **Step 1: 修改 plist — 调度改为每天 14:00**

将 `StartCalendarInterval` 从包含 `Weekday` 的两项数组改为不含 `Weekday` 的单项（每天触发）：

```xml
    <!-- 每天 14:00 执行 -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>14</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
```

- [ ] **Step 2: 修改 plist — 命令串联 deploy.py**

将 `ProgramArguments` 中的命令从：
```
cd ... && source .venv/bin/activate && python run.py && python notify.py
```
改为：
```
cd ... && source .venv/bin/activate && python run.py && python deploy.py
```

- [ ] **Step 3: 修改 plist — 新增环境变量**

在 `EnvironmentVariables` dict 中添加：
```xml
        <key>NETLIFY_AUTH_TOKEN</key>
        <string>__FILL_IN__</string>
        <key>NETLIFY_SITE_ID</key>
        <string>__FILL_IN__</string>
        <key>DEPLOY_PASSWORD</key>
        <string>__FILL_IN__</string>
        <key>MASSIVE_S3_ACCESS_KEY</key>
        <string>__FILL_IN__</string>
        <key>MASSIVE_S3_SECRET_KEY</key>
        <string>__FILL_IN__</string>
```

> **注意：** `__FILL_IN__` 占位符需用户手动替换为真实值。S3 key 是 `data_sync` 期权同步必需的，之前 plist 漏配。

- [ ] **Step 4: 提交**

```bash
git add com.lambda.scheduled-notify.plist
git commit -m "[feature/netlify-deploy][配置] launchd 改为每天14:00，串联 deploy.py"
```

---

### Task 5: 加载 plist + 端到端验证

- [ ] **Step 1: 用户手动填写 plist 中的环境变量占位符**

用户在 `~/.zshrc` 中确认以下变量已设置：
- `NETLIFY_AUTH_TOKEN` — 从 Netlify Dashboard > User Settings > Applications > Personal access tokens 获取
- `NETLIFY_SITE_ID` — 从 Netlify Dashboard > zfz-lambda > Site configuration > General > Site ID 获取
- `DEPLOY_PASSWORD` — 自定义密码

然后手动更新 plist 中的占位符。

- [ ] **Step 2: 手动运行 deploy.py 验证**

```bash
source .venv/bin/activate
source ~/.zshrc
python deploy.py
```

Expected: 控制台输出部署成功 URL，Telegram 收到通知消息。

- [ ] **Step 3: 浏览器验证密码锁**

打开 Netlify 站点 URL：
1. 应显示密码输入界面
2. 输入错误密码 → 显示"密码错误"
3. 输入正确密码 → 显示 TQQQ 策略报告
4. 关闭标签页重新打开 → 自动跳过密码（localStorage 缓存）

- [ ] **Step 4: 重新加载 launchd plist**

```bash
launchctl unload ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist 2>/dev/null
cp com.lambda.scheduled-notify.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist
launchctl list | grep lambda
```

- [ ] **Step 5: 提交最终状态**

```bash
git add -A
git commit -m "[feature/netlify-deploy][完成] 端到端验证通过"
```
