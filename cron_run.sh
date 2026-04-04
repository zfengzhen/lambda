#!/bin/bash
# 定时任务入口：激活虚拟环境 → 运行策略 → 部署

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/output/cron.log"

# 加载环境变量（API keys 等）
source ~/.zshrc 2>/dev/null || true

# 激活虚拟环境
source "$SCRIPT_DIR/.venv/bin/activate"

exec >> "$LOG_FILE" 2>&1
echo "========== $(date '+%Y-%m-%d %H:%M:%S') =========="

cd "$SCRIPT_DIR"

# 策略计算 + 生成报告
python run.py TQQQ

# 部署到 Netlify
python deploy.py --ticker TQQQ

echo "Done at $(date '+%Y-%m-%d %H:%M:%S')"
