import os
from dotenv import load_dotenv

load_dotenv()

LEVERAGE:            int   = int(os.getenv("LEVERAGE", "5"))
PROXY:               str   = os.getenv("PROXY", "")
# Минимальный спред: стратегия #4 работает от 3%
# (медиана закрытия 2 минуты при обоих отрицательных фандингах)
MIN_SPREAD_PCT:      float = float(os.getenv("MIN_SPREAD_PCT", "4.0"))
AUTO_CLOSE_SPREAD_PCT: float = 3.3  # Авто-закрытие если спред упал ниже (толерантность 0.2%)
# Минимальный объём 24ч. Для малых монет (ZBCN, NTRN) объём может быть $20k-100k
# Ставим 0 чтобы не блокировать — объём проверяется визуально
MIN_VOLUME_USD:      float = float(os.getenv("MIN_VOLUME_USD", "0"))
# ⚠️  Если в .env стоит MIN_VOLUME_USD=500000 — убери или поставь MIN_VOLUME_USD=0
# Малые монеты (ZBCN, NTRN, LYN) имеют объём $10k-100k и будут заблокированы при 500k
MAX_OPEN_POSITIONS:  int   = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
BALANCE_ALERT_PCT:   float = float(os.getenv("BALANCE_ALERT_PCT", "50"))

CACHE_MIN_SPREAD:    float = float(os.getenv("CACHE_MIN_SPREAD", "4.0"))

# ── Фандинг-фильтр ────────────────────────────────────────────
# Данные: diff<0.1% → медиана закрытия 69м (отлично)
#         diff 0.1-0.2% → 491м (приемлемо)
#         diff 0.2-0.3% → 785м (плохо)
#         diff>0.3% → 1351м (очень плохо)
MAX_FUNDING_DIFF_PCT: float = float(os.getenv("MAX_FUNDING_DIFF_PCT", "0.1"))  # было 0.2
MAX_FUNDING_ABS_PCT:  float = float(os.getenv("MAX_FUNDING_ABS_PCT", "1.5"))

# ── Интервальные исключения ────────────────────────────────────
# 4H/4H: медиана закрытия 102м, 62% за 4ч — лучший вариант, берём при diff<0.2%
# 1H/1H: медиана 4493м (75ч!) — крайне плохо, брать только при diff=0% и abs<0.3%
# 1H/4H: медиана 3353м — плохо, не брать
# Разрешённые интервалы для торговли
ALLOWED_INTERVALS: set = {4, 8}  # 1H только при строгих условиях

DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/arb_bot")

TG_API_ID:      int = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH:    str = os.getenv("TG_API_HASH", "")
TG_SESSION:     str = os.getenv("TG_SESSION", "arb_bot_session")
SIGNAL_CHANNEL: str = os.getenv("SIGNAL_CHANNEL", "")

NOTIFY_BOT_TOKEN: str = os.getenv("NOTIFY_BOT_TOKEN", "")
NOTIFY_CHAT_ID:   str = os.getenv("NOTIFY_CHAT_ID", "")

# Бот и канал для MEXC сигналов (если MEXC_BOT_TOKEN не задан — используется основной)
MEXC_BOT_TOKEN: str = os.getenv("MEXC_BOT_TOKEN", os.getenv("NOTIFY_BOT_TOKEN", ""))
MEXC_CHAT_ID:   str = os.getenv("MEXC_CHAT_ID",   os.getenv("NOTIFY_CHAT_ID", ""))

# Бот и канал для лога ордеров (если ORDERS_BOT_TOKEN не задан — используется основной)
ORDERS_BOT_TOKEN: str = os.getenv("ORDERS_BOT_TOKEN", os.getenv("NOTIFY_BOT_TOKEN", ""))
ORDERS_CHAT_ID:   str = os.getenv("ORDERS_CHAT_ID",   os.getenv("NOTIFY_CHAT_ID", ""))

# Поріг закриття при маржинальному ризику:
# якщо unrealized loss однієї ноги >= MARGIN_RISK_PCT % від виділеної маржі — закриваємо обидві.
# Приклад: маржа $5, позиція -$4 = 80% → закриваємо. Default = 80.
MARGIN_RISK_PCT: float = float(os.getenv("MARGIN_RISK_PCT", "80"))

# ── Фільтри захисту (Anti-Toxic Flow) ─────────────────────────
ATR_VOLATILE_MULTIPLIER: float = float(os.getenv("ATR_VOLATILE_MULTIPLIER", "2.5"))
DEPTH_MIN_RATIO:         float = float(os.getenv("DEPTH_MIN_RATIO", "3.0"))
INDEX_DEVIATION_PCT:     float = float(os.getenv("INDEX_DEVIATION_PCT", "1.0"))

EXCHANGE_KEYS: dict = {
    "binance": {"api_key": os.getenv("BINANCE_API_KEY", ""), "api_secret": os.getenv("BINANCE_API_SECRET", "")},
    "bybit":   {"api_key": os.getenv("BYBIT_API_KEY",   ""), "api_secret": os.getenv("BYBIT_API_SECRET",   "")},
    "bitget":  {"api_key": os.getenv("BITGET_API_KEY",  ""), "api_secret": os.getenv("BITGET_API_SECRET",  ""), "api_password": os.getenv("BITGET_PASSPHRASE", "")},
    "mexc":    {"api_key": os.getenv("MEXC_API_KEY", ""), "api_secret": os.getenv("MEXC_API_SECRET", ""), "user_id": os.getenv("MEXC_USER_ID", "")},
    "kucoin":  {"api_key": os.getenv("KUCOIN_API_KEY",  ""), "api_secret": os.getenv("KUCOIN_API_SECRET",  ""), "api_password": os.getenv("KUCOIN_PASSPHRASE", "")},
    "gate":    {"api_key": os.getenv("GATE_API_KEY",    ""), "api_secret": os.getenv("GATE_API_SECRET",    "")},
}

def get_exchange_keys(exchange: str) -> dict:
    keys = EXCHANGE_KEYS.get(exchange.lower(), {})
    if not keys.get("api_key"):
        raise ValueError(f"No API keys configured for {exchange}")
    return keys