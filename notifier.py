"""
notifier.py — уведомления через Telegram.

Три независимых бота (токен + chat_id из .env):
  NOTIFY_BOT_TOKEN / NOTIFY_CHAT_ID   — открытие/закрытие сделок (основной)
  MEXC_BOT_TOKEN   / MEXC_CHAT_ID     — сигналы MEXC (бот не торгует)
  ORDERS_BOT_TOKEN / ORDERS_CHAT_ID   — лог исполнения ордеров

Если MEXC_BOT_TOKEN / ORDERS_BOT_TOKEN не заданы в .env —
автоматически используется основной бот (NOTIFY_BOT_TOKEN).
"""

import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from settings import (
    NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID,
    MEXC_BOT_TOKEN,   MEXC_CHAT_ID,
    ORDERS_BOT_TOKEN, ORDERS_CHAT_ID,
)

_executor = ThreadPoolExecutor(max_workers=4)

_TV_PREFIX = {
    'binance': 'BINANCE',
    'bybit':   'BYBIT',
    'mexc':    'MEXC',
    'gate':    'GATE',
    'bitget':  'BITGET',
    'kucoin':  'KUCOIN',
}

_EX_FUTURES_URL = {
    'mexc':    'https://futures.mexc.com/exchange/{symbol}_USDT',
    'binance': 'https://www.binance.com/en/futures/{symbol}USDT',
    'bybit':   'https://www.bybit.com/trade/usdt/{symbol}USDT',
    'gate':    'https://www.gate.io/futures/usdt/{symbol}_USDT',
    'bitget':  'https://www.bitget.com/futures/usdt/{symbol}USDT',
    'kucoin':  'https://www.kucoin.com/futures/trade/{symbol}USDTM',
}


def exchange_url(exchange: str, ticker: str) -> str:
    symbol = ticker.upper()
    if symbol.endswith('USDT'):
        symbol = symbol[:-4]
    tmpl = _EX_FUTURES_URL.get(exchange.lower(), '')
    return tmpl.format(symbol=symbol) if tmpl else ''


def tradingview_url(short_exchange: str, long_exchange: str, ticker: str) -> str:
    symbol = ticker.upper()
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
    s_prefix = _TV_PREFIX.get(short_exchange.lower(), short_exchange.upper())
    l_prefix = _TV_PREFIX.get(long_exchange.lower(), long_exchange.upper())
    pair = f"{s_prefix}:{symbol}.P/{l_prefix}:{symbol}.P"
    return f"https://www.tradingview.com/chart/?symbol={pair}"


def _safe_html(text: str) -> str:
    import re
    allowed = re.findall(r'</?(?:b|i|code|pre|s|u)>', text)
    result = text
    for i, tag in enumerate(allowed):
        result = result.replace(tag, f'\x00TAG{i}\x00', 1)
    result = result.replace('<', '&lt;').replace('>', '&gt;')
    for i, tag in enumerate(allowed):
        result = result.replace(f'\x00TAG{i}\x00', tag, 1)
    return result


def _send_sync(text: str, bot_token: str, chat_id: str,
               tv_url: str = None, buttons: list = None) -> int | None:
    """Синхронная отправка через указанного бота в указанный чат."""
    if not bot_token or not chat_id:
        print(f"[Notify] пропущено — bot_token или chat_id не заданы в .env")
        print(f"[Notify] {text[:120]}")
        return None

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id":                  chat_id,
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
        tv_btns = [b for b in all_buttons if 'TradingView' in b['text']]
        ex_btns = [b for b in all_buttons if 'TradingView' not in b['text']]
        keyboard = []
        if tv_btns:
            keyboard.append(tv_btns)
        if ex_btns:
            keyboard.append(ex_btns)
        payload["reply_markup"] = {"inline_keyboard": keyboard}

    try:
        r = requests.post(url, json=payload, timeout=15)
        if not r.ok:
            print(f"[Notify] ошибка {r.status_code}: {r.text[:200]}")
            return None
        return r.json().get('result', {}).get('message_id')
    except Exception as e:
        print(f"[Notify] исключение: {e}")
        return None


def _edit_sync(message_id: int, text: str, bot_token: str, chat_id: str,
               tv_url: str = None, buttons: list = None):
    """Синхронное редактирование сообщения."""
    if not bot_token or not chat_id or not message_id:
        return

    url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    payload = {
        "chat_id":                  chat_id,
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
        tv_btns = [b for b in all_buttons if 'TradingView' in b['text']]
        ex_btns = [b for b in all_buttons if 'TradingView' not in b['text']]
        keyboard = []
        if tv_btns:
            keyboard.append(tv_btns)
        if ex_btns:
            keyboard.append(ex_btns)
        payload["reply_markup"] = {"inline_keyboard": keyboard}

    try:
        r = requests.post(url, json=payload, timeout=15)
        if not r.ok and 'message is not modified' not in r.text:
            print(f"[Notify] edit ошибка {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[Notify] edit исключение: {e}")


# ── Публичные async функции ───────────────────────────────────

async def notify(text: str, tv_url: str = None, buttons: list = None) -> int | None:
    """Основной чат — открытие/закрытие сделок."""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor, _send_sync, text, NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID, tv_url, buttons
    )


async def notify_mexc(text: str, tv_url: str = None, buttons: list = None) -> int | None:
    """MEXC-бот — сигналы где есть MEXC, торговля не открывается."""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor, _send_sync, text, MEXC_BOT_TOKEN, MEXC_CHAT_ID, tv_url, buttons
    )


async def notify_order(exchange: str, side: str, symbol: str,
                       price: float, qty, size_usd: float,
                       order_id: str = '', extra: str = '') -> None:
    """Бот ордеров — каждый исполненный ордер с деталями."""
    import asyncio
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    side_emoji = '📉' if side.lower() in ('sell', 'short', 'close_short') else '📈'

    msg = (
        f"⚡ <b>{exchange.upper()}</b> | {side.upper()}\n"
        f"{side_emoji} <b>{symbol.upper()}</b>\n"
        f"💲 Цена: <code>${price:,.6f}</code>\n"
        f"📦 Qty:  <code>{qty}</code>\n"
        f"💵 ~<code>${size_usd:,.2f}</code>\n"
        f"🕐 {now} UTC"
    )
    if order_id:
        msg += f"\n🔑 ID: <code>{order_id}</code>"
    if extra:
        msg += f"\n{extra}"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        _executor, _send_sync, msg, ORDERS_BOT_TOKEN, ORDERS_CHAT_ID, None, None
    )


async def edit_notify(message_id: int, text: str,
                      tv_url: str = None, buttons: list = None):
    """Редактирует сообщение в основном чате."""
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        _executor, _edit_sync, message_id, text, NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID, tv_url, buttons
    )