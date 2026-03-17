import aiohttp
from settings import NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID


# Маппинг названий бирж в префиксы TradingView
_TV_PREFIX = {
    'binance': 'BINANCE',
    'bybit':   'BYBIT',
    'mexc':    'MEXC',
    'gate':    'GATE',
    'bitget':  'BITGET',
    'kucoin':  'KUCOIN',
}


def tradingview_url(short_exchange: str, long_exchange: str, ticker: str) -> str:
    """Формирует ссылку на TradingView с парой short/long.
    Формат: BINANCE:BASUSDT.P / MEXC:BASUSDT.P
    """
    symbol = ticker.upper()
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
    s_prefix = _TV_PREFIX.get(short_exchange.lower(), short_exchange.upper())
    l_prefix = _TV_PREFIX.get(long_exchange.lower(), long_exchange.upper())
    pair = f"{s_prefix}:{symbol}.P/{l_prefix}:{symbol}.P"
    return f"https://www.tradingview.com/chart/?symbol={pair}"


async def notify(text: str, tv_url: str = None):
    """Отправляет сообщение в Telegram.
    tv_url — если передан, добавляет inline кнопку TradingView.
    """
    if not NOTIFY_BOT_TOKEN or not NOTIFY_CHAT_ID:
        print(f"[Notify] {text}")
        return
    url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":                  NOTIFY_CHAT_ID,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    if tv_url:
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "📈 TradingView", "url": tv_url}
            ]]
        }
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload,
                         timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        print(f"[Notify] error: {e}")