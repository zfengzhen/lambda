# Scheduled Notify 设计文档

## 概述

在 Lambda Sell Put 策略系统中，新增定时运行 + Telegram 截图推送功能。通过 macOS launchd 定时执行 `run.py` 生成策略截图，再由独立的 `notify.py` 将新截图发送到 Telegram 并删除已发送的文件。

## 需求

- 定时运行 `run.py`，生成策略 PNG 截图
- 将新生成的截图通过 Telegram Bot 发送（只发图片，无 caption）
- 发送成功后删除 PNG 文件
- 调度方式：macOS launchd，用户自定义 cron 时间
- 推送目标：仅 Telegram（不做 iMessage）

## 架构

### 数据流

```
launchd 定时触发
  → run.py（拉数据 → 策略计算 → HTML → PNG 写入 output/）
  → notify.py（扫描 output/*.png → Telegram sendPhoto → 删除 PNG）
```

### 文件变更

| 文件 | 操作 | 说明 |
|------|------|------|
| `notify.py` | 新建 | Telegram 推送模块 |
| `tests/test_notify.py` | 新建 | notify.py 单元测试 |
| `docs/scheduled-notify.md` | 新建 | launchd 配置使用说明 |
| `run.py` | 不改动 | — |
| `requirements.txt` | 不改动 | `requests` 已有 |

## 模块设计

### notify.py

**职责**：扫描 `output/` 目录下的 PNG 文件，通过 Telegram Bot API 发送，发送成功后删除。

**环境变量**：
- `TELEGRAM_BOT_TOKEN` — Bot API Token（通过 BotFather 创建）
- `TELEGRAM_CHAT_ID` — 目标 chat ID（个人/群组/频道）

**核心逻辑**：
1. 读取环境变量，缺失则报错退出
2. 用 `glob` 扫描 `output/*.png`
3. 对每个 PNG 文件：
   - 调用 Telegram Bot API `sendPhoto`（POST multipart/form-data）
   - 检查响应 `ok` 字段
   - 成功则删除文件，失败则记录日志保留文件
4. 无 PNG 时静默退出（exit 0）

**API 调用**：
```
POST https://api.telegram.org/bot{TOKEN}/sendPhoto
  - chat_id: {CHAT_ID}
  - photo: (file upload)
```

**错误处理**：
- 环境变量缺失：打印错误，exit 1
- 网络错误 / API 返回失败：记录日志，保留文件不删除，继续处理下一个
- 无 PNG 文件：静默 exit 0

**CLI 接口**：
```bash
python notify.py              # 扫描 output/ 发送所有 PNG
python notify.py --dry-run    # 只打印待发送文件，不实际发送
```

### launchd 配置

**plist 文件**：`com.lambda.scheduled-notify.plist`，放置于 `~/Library/LaunchAgents/`

**执行命令**：
```bash
bash -c "cd /project/path && source .venv/bin/activate && python run.py && python notify.py"
```

**调度时间**：用户通过 `StartCalendarInterval` 自定义，示例为每天 04:30（北京时间，对应美股收盘后）。

**日志输出**：stdout/stderr 重定向到 `output/scheduled-notify.log`。

**管理命令**：
```bash
launchctl load ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist
launchctl unload ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist
launchctl list | grep lambda
```

## 测试

### test_notify.py

- 测试 PNG 扫描逻辑（有文件 / 无文件）
- 测试 Telegram API 调用（mock requests.post）
- 测试发送成功后文件被删除
- 测试发送失败时文件保留
- 测试环境变量缺失时的错误处理
- 测试 `--dry-run` 模式

## 配置说明文档

`docs/scheduled-notify.md` 包含：
1. Telegram Bot 创建步骤（BotFather）
2. 获取 chat_id 的方法
3. 环境变量配置
4. launchd plist 安装与管理
5. 手动运行 notify.py 的方法
