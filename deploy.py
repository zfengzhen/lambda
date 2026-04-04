"""
部署脚本：密码包装 → Netlify 部署 → Telegram 通知

用法:
    python deploy.py                    # 部署 output/TQQQ.html
    python deploy.py --ticker QQQ       # 部署 output/QQQ.html
"""
import base64
import hashlib
import io
import logging
import os
import sys
import zipfile

import requests

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")


def build_deploy_zip(html: str) -> bytes:
    """将 HTML 打包为 ZIP（index.html + _headers），返回 ZIP 字节。"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.html", html)
        # 强制 Netlify 以 text/html 提供文件
        zf.writestr("_headers", "/*\n  Content-Type: text/html; charset=utf-8\n")
    return buf.getvalue()


def deploy_to_netlify(html: str) -> str:
    """将 HTML 部署到 Netlify，返回站点 URL。

    环境变量：LAMBDA_NETLIFY_AUTH_TOKEN, LAMBDA_NETLIFY_SITE_ID
    使用 ZIP deploy 方式：POST /api/v1/sites/{site_id}/deploys
    """
    token = os.environ.get("LAMBDA_NETLIFY_AUTH_TOKEN", "")
    site_id = os.environ.get("LAMBDA_NETLIFY_SITE_ID", "")
    if not token:
        raise ValueError("缺少环境变量 LAMBDA_NETLIFY_AUTH_TOKEN")
    if not site_id:
        raise ValueError("缺少环境变量 LAMBDA_NETLIFY_SITE_ID")

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
  var decoded = atob(DATA);
  var bytes = new Uint8Array(decoded.length);
  for (var i = 0; i < decoded.length; i++) bytes[i] = decoded.charCodeAt(i);
  var html = new TextDecoder("utf-8").decode(bytes);
  // 用 iframe + srcdoc 加载，确保脚本正常执行
  document.getElementById("auth-screen").style.display = "none";
  var iframe = document.createElement("iframe");
  iframe.style.cssText = "position:fixed;top:0;left:0;width:100%;height:100%;border:none;margin:0;padding:0;";
  iframe.srcdoc = html;
  document.body.style.margin = "0";
  document.body.style.overflow = "hidden";
  document.body.appendChild(iframe);
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

document.getElementById("pw-input").addEventListener("keydown", function(e) {{
  if (e.key === "Enter") verify();
}});

(function() {{
  try {{
    var cache = JSON.parse(localStorage.getItem(CACHE_KEY));
    if (cache && cache.hash === HASH && cache.expires > Date.now()) unlock();
  }} catch(e) {{}}
}})();
</script>
</body>
</html>"""


def send_telegram(url: str, ticker: str = "TQQQ"):
    """部署成功后发送 Telegram 通知。缺少环境变量时静默跳过。"""
    token = os.environ.get("LAMBDA_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("LAMBDA_TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("缺少 LAMBDA_TELEGRAM_BOT_TOKEN 或 LAMBDA_TELEGRAM_CHAT_ID，跳过通知")
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

    password = os.environ.get("LAMBDA_DEPLOY_PASSWORD", "")
    if not password:
        logger.error("缺少环境变量 LAMBDA_DEPLOY_PASSWORD")
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
