"""
config/settings.py — конфигурация бота.

Ключевые параметры:
- LEVERAGE = 5  (максимальное плечо на всех биржах)
- Размер позиции = баланс_на_бирже × LEVERAGE
  при балансе $5 → позиция $25 на каждую биржу
- Финальный размер = min(позиция_биржа1, позиция_биржа2, max_size_сигнал)
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# ТОРГОВЫЕ ПАРАМЕТРЫ
# ============================================================
LEVERAGE: int = int(os.getenv("LEVERAGE", "5"))

# Минимальный спред для входа (в %)
MIN_SPREAD_PCT: float = float(os.getenv("MIN_SPREAD_PCT", "0.5"))

# Максимальное количество одновременных позиций
MAX_OPEN_POSITIONS: int = int(os.getenv("MAX_OPEN_POSITIONS", "3"))

# Порог алерта баланса: уведомлять если баланс упал ниже X% от стартового
BALANCE_ALERT_PCT: float = float(os.getenv("BALANCE_ALERT_PCT", "50"))

# ============================================================
# TELEGRAM
# ============================================================
TG_API_ID:      int = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH:    str = os.getenv("TG_API_HASH", "")
TG_SESSION:     str = os.getenv("TG_SESSION", "arb_bot_session")
SIGNAL_CHANNEL: str = os.getenv("SIGNAL_CHANNEL", "")

NOTIFY_BOT_TOKEN: str = os.getenv("NOTIFY_BOT_TOKEN", "")
NOTIFY_CHAT_ID:   str = os.getenv("NOTIFY_CHAT_ID", "")

# ============================================================
# API КЛЮЧИ БИРЖ (только LIVE)
# ============================================================
EXCHANGE_KEYS: dict = {
    "binance": {
        "api_key":    os.getenv("BINANCE_API_KEY", ""),
        "api_secret": os.getenv("BINANCE_API_SECRET", ""),
    },
    "bybit": {
        "api_key":    os.getenv("BYBIT_API_KEY", ""),
        "api_secret": os.getenv("BYBIT_API_SECRET", ""),
    },
    "bitget": {
        "api_key":      os.getenv("BITGET_API_KEY", ""),
        "api_secret":   os.getenv("BITGET_API_SECRET", ""),
        "api_password": os.getenv("BITGET_PASSPHRASE", ""),
    },
    "mexc": {
        "api_key":    os.getenv("MEXC_API_KEY", ""),
        "api_secret": os.getenv("MEXC_API_SECRET", ""),
    },
    "kucoin": {
        "api_key":      os.getenv("KUCOIN_API_KEY", ""),
        "api_secret":   os.getenv("KUCOIN_API_SECRET", ""),
        "api_password": os.getenv("KUCOIN_PASSPHRASE", ""),
    },
    "gate": {
        "api_key":    os.getenv("GATE_API_KEY", ""),
        "api_secret": os.getenv("GATE_API_SECRET", ""),
    },
    "ourbit": {
        "api_key":    os.getenv("OURBIT_API_KEY", ""),
        "api_secret": os.getenv("OURBIT_API_SECRET", ""),
    },
}


def get_exchange_keys(exchange: str) -> dict:
    keys = EXCHANGE_KEYS.get(exchange.lower(), {})
    if not keys.get("api_key"):
        raise ValueError(f"No API keys configured for {exchange}")
    return keys
