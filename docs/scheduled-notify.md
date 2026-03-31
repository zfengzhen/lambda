# 定时推送使用说明

本文档说明如何设置 Telegram Bot 定时推送策略截图。

## 1. Telegram Bot 创建步骤

### 1.1 新建 Bot

1. 在 Telegram 搜索 `@BotFather`
2. 发送命令 `/newbot`
3. BotFather 提示输入 bot 名称（如 `My Lambda Bot`）
4. 输入用户名（必须以 `bot` 结尾，如 `my_lambda_bot`）
5. 获得 **API Token**，格式类似 `123456789:ABCdefGHIjklMNOpqrsTUVwxyz1234567890`
6. 保存 Token，稍后需要配置到环境变量

## 2. 获取 chat_id

chat_id 是目标接收者的 Telegram ID（个人为正数，群组为负数）。

### 2.1 个人 chat_id

1. 给刚创建的 Bot 发送任意一条消息（如 `/start` 或 `hello`）
2. 在浏览器中访问以下链接，替换 `<TOKEN>` 为上一步获得的 API Token：
   ```
   https://api.telegram.org/bot<TOKEN>/getUpdates
   ```
3. 得到 JSON 响应，找到 `result` 数组中第一条消息的 `chat.id` 字段
   ```json
   {
     "result": [
       {
         "message": {
           "chat": {
             "id": 123456789
           }
         }
       }
     ]
   }
   ```
4. `123456789` 即为你的 chat_id

### 2.2 群组 chat_id

1. 将 Bot 添加到目标群组
2. 在群组中发送任意消息
3. 同样访问 getUpdates 链接查看
4. 群组的 chat_id 为负数，如 `-987654321`

## 3. 环境变量配置

需要配置两个环境变量：

| 变量名 | 说明 | 例子 |
|------|------|------|
| `TELEGRAM_BOT_TOKEN` | Bot API Token | `123456789:ABCdefGHIjklMNOpqrsTUVwxyz1234567890` |
| `TELEGRAM_CHAT_ID` | 目标 chat ID（个人为正数，群组为负数） | `123456789` 或 `-987654321` |

### 3.1 Shell 配置（临时生效）

在当前 Shell 会话中设置：

```bash
export TELEGRAM_BOT_TOKEN="your_bot_token_here"
export TELEGRAM_CHAT_ID="your_chat_id_here"
```

### 3.2 Shell 配置（持久生效）

编辑 `~/.zshrc`（zsh）或 `~/.bash_profile`（bash），添加：

```bash
export TELEGRAM_BOT_TOKEN="your_bot_token_here"
export TELEGRAM_CHAT_ID="your_chat_id_here"
```

保存后执行：

```bash
source ~/.zshrc  # 或 source ~/.bash_profile
```

### 3.3 launchd 配置（推荐用于定时任务）

launchd 的定时任务通过 plist 文件的 `EnvironmentVariables` 字段设置环境变量（参考 4.2 节）。

## 4. launchd 定时任务安装与管理

launchd 是 macOS 原生的任务调度工具。本项目提供 `com.lambda.scheduled-notify.plist` 文件。

### 4.1 安装 launchd 任务

1. **复制 plist 文件到 LaunchAgents 目录**
   ```bash
   cp com.lambda.scheduled-notify.plist ~/Library/LaunchAgents/
   ```

2. **编辑 plist 文件，替换占位符**
   ```bash
   open -e ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist
   ```

   需要替换以下内容：
   - `YOUR_HOME_DIR` — 替换为你的主目录路径（如 `/Users/fengzhen.zhang`）
   - `YOUR_VENV_PYTHON` — 替换为虚拟环境中 Python 的完整路径（如 `/Users/fengzhen.zhang/alpha/lambda-worktrees/feature-scheduled-notify/.venv/bin/python`）
   - `YOUR_PROJECT_DIR` — 替换为项目目录路径（如 `/Users/fengzhen.zhang/alpha/lambda-worktrees/feature-scheduled-notify`）
   - `YOUR_BOT_TOKEN` — 替换为 Telegram Bot Token
   - `YOUR_CHAT_ID` — 替换为 chat ID

3. **（可选）修改执行时间**

   在 plist 中查找 `StartCalendarInterval` 字段，默认为每天上午 9 点。修改示例：

   ```xml
   <key>StartCalendarInterval</key>
   <array>
     <dict>
       <key>Hour</key>
       <integer>9</integer>        <!-- 小时（0-23） -->
       <key>Minute</key>
       <integer>30</integer>       <!-- 分钟（0-59） -->
       <key>Weekday</key>
       <integer>1</integer>        <!-- 星期（0=周日，1=周一，...，5=周五，6=周六） -->
     </dict>
   </array>
   ```

   移除 `Weekday` 字段表示每天执行；添加多个 `<dict>` 块表示多个时间点。

### 4.2 加载与卸载任务

**加载任务**（生效）：
```bash
launchctl load ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist
```

**卸载任务**（停止执行）：
```bash
launchctl unload ~/Library/LaunchAgents/com.lambda.scheduled-notify.plist
```

### 4.3 查看任务状态

```bash
# 查看所有 lambda 相关任务
launchctl list | grep lambda

# 查看特定任务详情
launchctl list com.lambda.scheduled-notify
```

任务已加载时，`launchctl list` 输出会显示对应的 job 名称和进程信息。

## 5. 手动运行

### 5.1 发送所有待推送截图

激活虚拟环境后运行：

```bash
source .venv/bin/activate
python notify.py
```

脚本将查找 `output/` 目录下所有新生成的 PNG 截图，通过 Telegram Bot 发送到指定 chat ID。

### 5.2 干运行模式（仅预览）

```bash
python notify.py --dry-run
```

脚本将打印待发送文件列表，但不实际发送消息。用于调试和确认配置无误。

## 6. 日志查看

推送日志记录在 `output/scheduled-notify.log` 文件中。

**实时查看日志**：

```bash
tail -f output/scheduled-notify.log
```

**查看最后 50 行日志**：

```bash
tail -50 output/scheduled-notify.log
```

日志包含：
- 发送的截图文件名
- 发送成功/失败状态
- Telegram API 响应信息
- 异常和调试信息

## 常见问题排查

| 问题 | 排查步骤 |
|------|--------|
| Bot 不回复 | 检查 Token 是否正确；确认 Bot 有权限 |
| `chat.id` 找不到 | 确保已向 Bot 发送消息；检查 API 响应中 `result` 数组是否为空 |
| 环境变量未识别 | 确认已设置 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID`；Shell 会话中运行 `echo $TELEGRAM_BOT_TOKEN` 验证 |
| launchd 任务不执行 | 检查 plist 中的路径是否正确；查看系统日志 `log show --predicate 'process == "launchd"'` |
| 发送失败 | 查看 `output/scheduled-notify.log`；检查网络连接；验证 Telegram API Token 和 chat ID |

## 参考链接

- Telegram Bot API: https://core.telegram.org/bots/api
- macOS launchd 文档: https://www.man7.org/linux/man-pages/man5/launchd.plist.5.html
- Telegram @BotFather: https://t.me/BotFather
