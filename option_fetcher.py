"""期权数据获取：OCC 合约 symbol 构建、日线 OHLC 拉取、信号交易提取"""
import datetime

BASE_URL = "https://api.massive.com"
STRIKE_INCREMENT = 0.5  # TQQQ 期权行权价间距（美元）
MAX_RETRIES = 5
PAGE_DELAY = 15


def round_to_strike_increment(price: float, increment: float = STRIKE_INCREMENT) -> float:
    """将价格圆整到最近的行权价间距。

    Args:
        price: 目标行权价（如 38.56）
        increment: 行权价间距（默认 0.5）

    Returns:
        圆整后的行权价（如 38.5）
    """
    return round(round(price / increment) * increment, 10)


def build_occ_symbol(ticker: str, expiry_date: str, strike: float,
                     contract_type: str = "P") -> str:
    """构建 OCC 格式期权合约 symbol（含 Massive API 'O:' 前缀）。

    格式：O:{TICKER}{YYMMDD}{C|P}{8位行权价×1000，零填充}

    Args:
        ticker: 标的代码，如 "TQQQ"
        expiry_date: 到期日 "YYYY-MM-DD"
        strike: 行权价（美元），如 38.5
        contract_type: "C"（看涨）或 "P"（看跌），默认 "P"

    Returns:
        如 "O:TQQQ250131P00038500"
    """
    d = datetime.date.fromisoformat(expiry_date)
    date_str = d.strftime("%y%m%d")
    strike_int = int(round(strike * 1000))
    strike_str = f"{strike_int:08d}"
    return f"O:{ticker}{date_str}{contract_type.upper()}{strike_str}"
