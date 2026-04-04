# Netlify 自动部署 + Telegram 通知

## 概述

每天下午 2 点自动执行策略计算，将生成的 TQQQ.html 部署到 Netlify，并通过 Telegram 通知链接。

## 架构

```
launchd (每天 14:00)
  → run.py (生成 output/TQQQ.html)
  → deploy.py (密码包装 → Netlify API 部署 → Telegram 通知)
```

## 组件设计

### 1. deploy.py

新建脚本，职责：

#### 1.1 密码包装

- 读取 `output/TQQQ.html` 原始内容
- 将原始 HTML base64 编码，嵌入一个密码锁页面
- 密码从环境变量 `DEPLOY_PASSWORD` 读取
- 页面中存储密码的 SHA-256 hash（不暴露明文）
- 用户输入密码后，前端计算 SHA-256 与页面内 hash 比对
- 验证通过：base64 解码原始 HTML 并替换当前页面内容
- localStorage 存储：`{ hash: sha256(password), expires: now + 7天 }`
- 7 天内同浏览器再次访问自动跳过密码输入

#### 1.2 Netlify 部署

- 使用 Netlify File Deploy API（`POST /api/v1/sites/{site_id}/deploys`）
- 上传包装后的 HTML 作为 `index.html`
- 需要环境变量：
  - `NETLIFY_AUTH_TOKEN` — Netlify Personal Access Token
  - `NETLIFY_SITE_ID` — 站点 ID（从 zfz-lambda 项目获取）

#### 1.3 Telegram 通知

- 部署成功后调用 Telegram Bot API `sendMessage`
- 消息内容包含站点链接
- 需要环境变量：
  - `TELEGRAM_BOT_TOKEN`
  - `TELEGRAM_CHAT_ID`

### 2. launchd plist 修改

- 文件：`com.lambda.scheduled-notify.plist`
- 改动：`StartCalendarInterval` 从每周二/六改为每天 14:00
- 命令改为：`python run.py && python deploy.py`
- 新增环境变量：`NETLIFY_AUTH_TOKEN`、`NETLIFY_SITE_ID`、`DEPLOY_PASSWORD`

### 3. 密码锁页面结构

```html
<!-- 密码输入界面 -->
<div id="auth-screen">
  <input type="password" placeholder="输入密码">
  <button>确认</button>
</div>

<!-- 原始报告（初始隐藏） -->
<div id="content" style="display:none"></div>

<script>
  // SHA-256 hash 比对
  // localStorage 7 天缓存
  // 验证通过后 base64 解码并渲染原始 HTML
</script>
```

## 环境变量清单

| 变量 | 用途 | 存放位置 |
|------|------|----------|
| `MASSIVE_API_KEY` | 股票数据 API | ~/.zshrc（已有） |
| `MASSIVE_S3_ACCESS_KEY` | S3 期权数据 | ~/.zshrc（已有） |
| `MASSIVE_S3_SECRET_KEY` | S3 期权数据 | ~/.zshrc（已有） |
| `NETLIFY_AUTH_TOKEN` | Netlify 部署认证 | ~/.zshrc（新增） |
| `NETLIFY_SITE_ID` | Netlify 站点 ID | ~/.zshrc（新增） |
| `DEPLOY_PASSWORD` | 前端密码锁 | ~/.zshrc（新增） |
| `TELEGRAM_BOT_TOKEN` | Telegram 通知 | ~/.zshrc（已有） |
| `TELEGRAM_CHAT_ID` | Telegram 通知 | ~/.zshrc（已有） |

## 数据流

```
run.py
  → output/TQQQ.html (原始报告，~586KB)

deploy.py
  → 读取 output/TQQQ.html
  → base64 编码 + 密码锁包装 → 临时 index.html (~800KB)
  → Netlify API 上传 index.html
  → 部署成功 → Telegram 发送链接
```

## 错误处理

- `run.py` 失败：`&&` 短路，不执行 deploy.py
- TQQQ.html 不存在：deploy.py 报错退出
- Netlify API 失败：打印错误，不发 Telegram
- Telegram API 失败：打印错误（不阻塞，部署已完成）

## 不做的事

- 不改 `template.html` — 密码包装在 deploy.py 动态完成
- 不装 Netlify CLI — 用纯 Python requests 调 API
- 不加新依赖 — 只用 requests（已有）+ hashlib/base64（标准库）
