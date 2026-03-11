import os
from dotenv import load_dotenv

load_dotenv()

LEVERAGE:            int   = int(os.getenv("LEVERAGE", "5"))
PROXY:               str   = os.getenv("PROXY", "")   # напр: http://user:pass@1.2.3.4:8080
MIN_SPREAD_PCT:      float = float(os.getenv("MIN_SPREAD_PCT", "0.5"))
MIN_VOLUME_USD:      float = float(os.getenv("MIN_VOLUME_USD", "500000"))  # минимальный объём 24ч в USD
MAX_OPEN_POSITIONS:  int   = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
BALANCE_ALERT_PCT:   float = float(os.getenv("BALANCE_ALERT_PCT", "50"))

DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/arb_bot")

TG_API_ID:      int = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH:    str = os.getenv("TG_API_HASH", "")
TG_SESSION:     str = os.getenv("TG_SESSION", "arb_bot_session")
SIGNAL_CHANNEL: str = os.getenv("SIGNAL_CHANNEL", "")

NOTIFY_BOT_TOKEN: str = os.getenv("NOTIFY_BOT_TOKEN", "")
NOTIFY_CHAT_ID:   str = os.getenv("NOTIFY_CHAT_ID", "")

EXCHANGE_KEYS: dict = {
    "binance": {"api_key": os.getenv("BINANCE_API_KEY", ""), "api_secret": os.getenv("BINANCE_API_SECRET", "")},
    "bybit":   {"api_key": os.getenv("BYBIT_API_KEY",   ""), "api_secret": os.getenv("BYBIT_API_SECRET",   "")},
    "bitget":  {"api_key": os.getenv("BITGET_API_KEY",  ""), "api_secret": os.getenv("BITGET_API_SECRET",  ""), "api_password": os.getenv("BITGET_PASSPHRASE", "")},
    "mexc":    {"api_key": os.getenv("MEXC_API_KEY",    ""), "api_secret": os.getenv("MEXC_API_SECRET",    "")},
    "kucoin":  {"api_key": os.getenv("KUCOIN_API_KEY",  ""), "api_secret": os.getenv("KUCOIN_API_SECRET",  ""), "api_password": os.getenv("KUCOIN_PASSPHRASE", "")},
    "gate":    {"api_key": os.getenv("GATE_API_KEY",    ""), "api_secret": os.getenv("GATE_API_SECRET",    "")},
}

def get_exchange_keys(exchange: str) -> dict:
    keys = EXCHANGE_KEYS.get(exchange.lower(), {})
    if not keys.get("api_key"):
        raise ValueError(f"No API keys configured for {exchange}")
    return keys
