import requests
from settings import NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID
from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(max_workers=2)

_TV_PREFIX = {
    'binance': 'BINANCE',
    'bybit':   'BYBIT',
    'mexc':    'MEXC',
    'gate':    'GATE',
    'bitget':  'BITGET',
    'kucoin':  'KUCOIN',
}

# Ссылки на страницы фьючерсной торговли
_EX_FUTURES_URL = {
    'mexc':    'https://futures.mexc.com/exchange/{symbol}_USDT',
    'binance': 'https://www.binance.com/en/futures/{symbol}USDT',
    'bybit':   'https://www.bybit.com/trade/usdt/{symbol}USDT',
    'gate':    'https://www.gate.io/futures/usdt/{symbol}_USDT',
    'bitget':  'https://www.bitget.com/futures/usdt/{symbol}USDT',
    'kucoin':  'https://www.kucoin.com/futures/trade/{symbol}USDTM',
}


def exchange_url(exchange: str, ticker: str) -> str:
    """Возвращает ссылку на страницу фьючерса на бирже."""
    # Убираем USDT из тикера — шаблон сам добавит нужный суффикс
    symbol = ticker.upper()
    if symbol.endswith('USDT'):
        symbol = symbol[:-4]
    tmpl = _EX_FUTURES_URL.get(exchange.lower(), '')
    if not tmpl:
        return ''
    return tmpl.format(symbol=symbol)


def tradingview_url(short_exchange: str, long_exchange: str, ticker: str) -> str:
    symbol = ticker.upper()
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
    s_prefix = _TV_PREFIX.get(short_exchange.lower(), short_exchange.upper())
    l_prefix = _TV_PREFIX.get(long_exchange.lower(), long_exchange.upper())
    pair = f"{s_prefix}:{symbol}.P/{l_prefix}:{symbol}.P"
    return f"https://www.tradingview.com/chart/?symbol={pair}"


def _safe_html(text: str) -> str:
    """Экранирует символы которые ломают HTML parse в Telegram.
    Оставляем только разрешённые теги: <b>, </b>, <i>, </i>, <code>, </code>.
    Все остальные < и > заменяем на &lt; и &gt;
    """
    import re
    # Сначала находим разрешённые теги и заменяем их на placeholder
    allowed = re.findall(r'</?(?:b|i|code|pre|s|u)>', text)
    result = text
    # Заменяем разрешённые теги на временные маркеры
    for i, tag in enumerate(allowed):
        result = result.replace(tag, f'\x00TAG{i}\x00', 1)
    # Экранируем оставшиеся < и >
    result = result.replace('<', '&lt;').replace('>', '&gt;')
    # Возвращаем разрешённые теги
    for i, tag in enumerate(allowed):
        result = result.replace(f'\x00TAG{i}\x00', tag, 1)
    return result


def _send_sync(text: str, tv_url: str = None, buttons: list = None) -> int | None:
    """Синхронная отправка. Возвращает message_id для последующего редактирования."""
    if not NOTIFY_BOT_TOKEN or not NOTIFY_CHAT_ID:
        print(f"[Notify] {text[:120]}")
        return None
    url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":                  NOTIFY_CHAT_ID,
        "text":                     _safe_html(text),
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    all_buttons = []
    if tv_url:
        all_buttons.append({"text": "📈 TradingView", "url": tv_url})
    if buttons:
        all_buttons.extend(buttons)
    if all_buttons:
        keyboard = []
        tv_btns = [b for b in all_buttons if 'TradingView' in b['text']]
        ex_btns = [b for b in all_buttons if 'TradingView' not in b['text']]
        if tv_btns:
            keyboard.append(tv_btns)
        if ex_btns:
            keyboard.append(ex_btns)
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if not r.ok:
            print(f"[Notify] error {r.status_code}: {r.text[:100]}")
            return None
        return r.json().get('result', {}).get('message_id')
    except Exception as e:
        print(f"[Notify] error: {e}")
        return None


def _edit_sync(message_id: int, text: str, tv_url: str = None, buttons: list = None):
    """Редактирует существующее сообщение."""
    if not NOTIFY_BOT_TOKEN or not NOTIFY_CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id":                  NOTIFY_CHAT_ID,
        "message_id":               message_id,
        "text":                     _safe_html(text),
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    all_buttons = []
    if tv_url:
        all_buttons.append({"text": "📈 TradingView", "url": tv_url})
    if buttons:
        all_buttons.extend(buttons)
    if all_buttons:
        keyboard = []
        tv_btns = [b for b in all_buttons if 'TradingView' in b['text']]
        ex_btns = [b for b in all_buttons if 'TradingView' not in b['text']]
        if tv_btns:
            keyboard.append(tv_btns)
        if ex_btns:
            keyboard.append(ex_btns)
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if not r.ok and 'message is not modified' not in r.text:
            print(f"[Notify] edit error {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"[Notify] edit error: {e}")


async def notify(text: str, tv_url: str = None, buttons: list = None) -> int | None:
    """Отправляет сообщение. Возвращает message_id."""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _send_sync, text, tv_url, buttons)


async def edit_notify(message_id: int, text: str, tv_url: str = None, buttons: list = None):
    """Редактирует существующее сообщение в Telegram."""
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _edit_sync, message_id, text, tv_url, buttons)