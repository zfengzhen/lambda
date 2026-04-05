"""全局常量：TQQQ 专用配置。"""
from pathlib import Path

TICKER = "TQQQ"
LEVERAGE = 3
EXPIRY_WEEKS = 4
TRADING_DAYS_YEAR = 252

# 策略 OTM 表（3x 杠杆基准值）
DEFAULT_OTM = {
    "A": 0.08,
    "B1": 0.08, "B2": 0.08, "B3": 0.12, "B4": 0.15,
    "C1": 0.12, "C2": 0.15, "C3": 0.15, "C4": 0.20,
}

# 层级中文名
TIER_NAMES = {
    "A": "企稳双撑",
    "B1": "回调均线", "B2": "超跌支撑", "B3": "趋势动能弱", "B4": "低波整理",
    "C1": "跌势减速", "C2": "趋势延续", "C3": "过热追涨", "C4": "加速下杀",
}

ALL_TIERS = ["A", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4"]

# 数据库
DB_PATH = Path(__file__).parent / "output" / "market_data.duckdb"

# S3 默认值
S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"

# REST API
REST_BASE_URL = "https://api.massive.com"
REST_MAX_RETRIES = 5
REST_RETRY_DELAY = 15

# IV 计算参数
RISK_FREE_RATE = 0.05
IV_MIN_DTE = 7
IV_TARGET_DAYS = 30

# 数据同步
FULL_SYNC_YEARS = 2
