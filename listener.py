"""
listener.py — слушает Telegram-канал и запускает торговлю.

Логика обработки сообщений:
- NewMessage:     всегда обрабатываем (новый сигнал)
- MessageEdited:  обрабатываем ТОЛЬКО если тикер есть в закреплённом сообщении
- Закреплённое:   читается при старте и обновляется при каждом его редактировании
- Дедупликация:   один тикер не открывается дважды за DEDUP_WINDOW_S секунд
"""

import asyncio
import time
from telethon import TelegramClient, events
from telethon.tl.functions.channels import GetFullChannelRequest

from settings import TG_API_ID, TG_API_HASH, TG_SESSION, SIGNAL_CHANNEL
from signal_parser import parse_message, parse_pinned, OpenSignal, CloseSignal
from risk_manager import check_signal
from order_executor import open_position, close_position
from exchanges import create_exchange
from notifier import notify, notify_mexc

DEDUP_WINDOW_S = 120
_seen: dict[str, float] = {}

# Тикеры из закреплённого сообщения
_pinned_tickers: set[str] = set()
_pinned_msg_id: int | None = None

# Дедупликация MEXC уведомлений — не спамим одним тикером
_mexc_notified: dict[str, float] = {}
MEXC_NOTIFY_WINDOW_S = 300  # не повторять уведомление 5 минут


async def _notify_mexc(signal, reason: str = None):
    """Отправляет уведомление о MEXC сигнале без открытия сделки."""
    now = time.time()
    key = f"{signal.ticker}_{signal.short_exchange}_{signal.long_exchange}"
    if now - _mexc_notified.get(key, 0) < MEXC_NOTIFY_WINDOW_S:
        return
    _mexc_notified[key] = now

    other_ex = signal.long_exchange if signal.short_exchange == 'mexc' else signal.short_exchange
    fs = f"{signal.funding_short*100:+.3f}%" if signal.funding_short is not None else "—"
    fl = f"{signal.funding_long*100:+.3f}%" if signal.funding_long is not None else "—"
    i_s = f"{signal.interval_short}ч" if signal.interval_short else "—"
    i_l = f"{signal.interval_long}ч" if signal.interval_long else "—"

    msg = (f"👁 <b>MEXC сигнал: {signal.ticker}</b>\n"
           f"📊 Спред: {signal.spread_pct:.2f}%\n"
           f"📉 Short: {signal.short_exchange.upper()} @ ${signal.short_price:.6f}\n"
           f"📈 Long:  {signal.long_exchange.upper()} @ ${signal.long_price:.6f}\n"
           f"💸 Фандинг: {fs} / {fl} | Интервалы: {i_s}/{i_l}")
    if reason:
        msg += f"\n⚠️ Причина пропуска: {reason}"
    msg += f"\n\n⚠️ <i>MEXC API не поддерживает торговлю — открой вручную</i>"

    from notifier import tradingview_url, exchange_url
    tv_url   = tradingview_url(signal.short_exchange, signal.long_exchange, signal.ticker)
    short_url = exchange_url(signal.short_exchange, signal.ticker)
    long_url  = exchange_url(signal.long_exchange, signal.ticker)

    buttons = []
    if short_url:
        buttons.append({"text": f"📉 {signal.short_exchange.upper()}", "url": short_url})
    if long_url:
        buttons.append({"text": f"📈 {signal.long_exchange.upper()}", "url": long_url})

    await notify_mexc(msg, tv_url=tv_url, buttons=buttons)
    print(f"[MEXC] Уведомление отправлено: {signal.ticker} {signal.short_exchange}↕{signal.long_exchange}")


def _is_duplicate(ticker: str) -> bool:
    now = time.time()
    if now - _seen.get(ticker, 0) < DEDUP_WINDOW_S:
        return True
    _seen[ticker] = now
    return False


def _funding_ok(signal) -> tuple[bool, str]:
    """Фандинг-фильтр — Стратегия #4 (лучшая по данным 61,471 закрытий).

    Данные с правильным форматом времени MM:SS показывают:
      Медиана закрытия: 24 минуты (не 304 как считали раньше!)
      4H/4H медиана: 5 минут, <1ч = 76%
      8H/8H медиана: 9 минут, <1ч = 84%

    Стратегия #4 даёт $86/мес при $20/сделке — лучший результат:

    ✅ Условие A: diff < 0.2%
       (быстрое закрытие, медиана 25м)

    ✅ Условие B: оба фандинга отрицательные И diff < 1.0%
       (оба платят нам фандинг — дополнительная прибыль сверх спреда)

    ❌ Всё остальное: один фандинг положительный И diff >= 0.2%
    """
    fs = signal.funding_short
    fl = signal.funding_long
    if fs is None or fl is None:
        return True, "фандинг неизвестен — не блокируем"

    diff_pct       = abs(fs - fl) * 100
    both_negative  = fs < 0 and fl < 0
    i_s = signal.interval_short
    i_l = signal.interval_long
    info = f"short={fs*100:+.3f}% long={fl*100:+.3f}% diff={diff_pct:.3f}% int={i_s}ч/{i_l}ч"

    # Условие A: разница фандинга маленькая — берём всегда
    if diff_pct < 0.2:
        return True, f"diff<0.2% ✅ {info}"

    # Условие B: оба фандинга отрицательные — получаем фандинг с обеих сторон
    if both_negative and diff_pct < 1.0:
        return True, f"оба отриц. + diff<1% ✅ {info}"

    # Всё остальное — не берём
    return False, f"diff≥0.2% и не оба отриц. ❌ {info}"


class SignalListener:

    def __init__(self):
        self.client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
        self._running = False

    async def start(self):
        global _pinned_msg_id
        self._running = True
        await self.client.start()
        me = await self.client.get_me()
        print(f"[Listener] Logged in as {me.username}")
        print(f"[Listener] Watching: {SIGNAL_CHANNEL}")
        await notify(f"🟢 <b>Бот запущен</b>\nКанал: {SIGNAL_CHANNEL}")

        # Читаем закреплённое при старте
        await self._load_pinned()

        # Порог "недавнего" сообщения — редактирования в пределах этого времени обрабатываем всегда
        RECENT_MSG_SECONDS = 5 * 60  # 5 минут

        @self.client.on(events.NewMessage(chats=SIGNAL_CHANNEL))
        async def on_new(event):
            if not self._running:
                return
            received_ms = int(time.time() * 1000)
            await self._handle(event.message.text or "", received_ms, is_edit=False)

        @self.client.on(events.MessageEdited(chats=SIGNAL_CHANNEL))
        async def on_edit(event):
            if not self._running:
                return
            received_ms = int(time.time() * 1000)
            msg_id = event.message.id

            # Если редактируется закреплённое — обновляем список тикеров
            if msg_id == _pinned_msg_id:
                print(f"[Listener] Закреплённое сообщение обновлено — перечитываем")
                await self._load_pinned()
                return

            # Проверяем возраст сообщения
            msg_date = event.message.date
            if msg_date:
                import datetime
                age_seconds = (datetime.datetime.now(datetime.timezone.utc) - msg_date).total_seconds()
                is_recent = age_seconds <= RECENT_MSG_SECONDS
            else:
                is_recent = False

            await self._handle(event.message.text or "", received_ms, is_edit=True, is_recent=is_recent)

        await self.client.run_until_disconnected()

    async def stop(self):
        self._running = False
        await notify("🔴 <b>Бот остановлен</b>")
        await self.client.disconnect()

    async def _load_pinned(self):
        """Загружает закреплённое сообщение, обновляет _pinned_tickers и _pinned_msg_id."""
        global _pinned_tickers, _pinned_msg_id
        try:
            full = await self.client(GetFullChannelRequest(SIGNAL_CHANNEL))
            pinned_id = full.full_chat.pinned_msg_id
            if not pinned_id:
                print(f"[Listener] Закреплённое сообщение не найдено")
                return
            _pinned_msg_id = pinned_id
            msgs = await self.client.get_messages(SIGNAL_CHANNEL, ids=pinned_id)
            if not msgs:
                return
            text = msgs.text or ""
            # DEBUG: выводим первые 300 символов сырого текста
            print(f"[Listener] Закреплённое RAW (первые 300 симв): {repr(text[:300])}")
            signals = parse_pinned(text)
            _pinned_tickers = {s.ticker for s in signals}
            print(f"[Listener] Закреплённое: {len(signals)} спредов → тикеры: {_pinned_tickers}")
        except Exception as e:
            print(f"[Listener] Ошибка чтения закреплённого: {e}")

    async def _handle(self, text: str, received_ms: int, is_edit: bool, is_recent: bool = False):
        signal = parse_message(text)
        if signal is None:
            return

        if isinstance(signal, OpenSignal):
            in_pinned = signal.ticker in _pinned_tickers
            is_mexc   = 'mexc' in (signal.short_exchange, signal.long_exchange)
            other_ex  = signal.long_exchange if signal.short_exchange == 'mexc' else signal.short_exchange

            if is_edit:
                if is_recent:
                    source = "edit+recent"
                elif in_pinned:
                    source = "edit+pinned"
                else:
                    # Если сигнал с MEXC — отправляем инфо-уведомление
                    if is_mexc:
                        await _notify_mexc(signal)
                    print(f"[Listener] edit {signal.ticker} — не в закреплённом и не свежее, пропускаем")
                    return
            else:
                source = "new"

            # Фандинг-фильтр применяется ко ВСЕМ типам сообщений
            ok, reason = _funding_ok(signal)
            if not ok:
                if is_mexc:
                    await _notify_mexc(signal, reason=reason)
                print(f"[Listener] [{source}] {signal.ticker} — {reason}, пропускаем")
                return

            # Если одна из бирж MEXC — только уведомление, не торгуем
            if is_mexc:
                await _notify_mexc(signal)
                print(f"[Listener] [{source}] MEXC сигнал {signal.ticker} — уведомление отправлено, торговля пропущена")
                return

            print(f"[Listener] [{source}] OPEN {signal.ticker} {signal.short_exchange}↕{signal.long_exchange} {signal.spread_pct:.2f}% | {reason}")
            await self._open(signal, received_ms)

        elif isinstance(signal, CloseSignal):
            print(f"[Listener] CLOSE {signal.ticker}")
            await self._close(signal)

    async def _open(self, signal: OpenSignal, received_ms: int):
        if _is_duplicate(signal.ticker):
            print(f"[Listener] Дубль {signal.ticker} — пропускаем (окно {DEDUP_WINDOW_S}с)")
            return

        try:
            short_ex = create_exchange(signal.short_exchange)
            long_ex  = create_exchange(signal.long_exchange)
        except ValueError as e:
            # Тихо пропускаем неизвестные биржи (ourbit, aster и т.д.)
            if 'Unknown exchange' in str(e):
                return
            print(f"[Listener] Ошибка биржи {signal.ticker}: {e}")
            _seen.pop(signal.ticker, None)
            return
        except Exception as e:
            print(f"[Listener] Ошибка биржи {signal.ticker}: {e}")
            _seen.pop(signal.ticker, None)
            return

        try:
            risk = await asyncio.wait_for(
                check_signal(signal, short_ex, long_ex),
                timeout=30
            )
        except asyncio.TimeoutError:
            print(f"[Listener] TIMEOUT check_signal {signal.ticker}")
            await notify(f"⏱ Таймаут проверки {signal.ticker} (30с)")
            await short_ex.close()
            await long_ex.close()
            return
        except Exception as e:
            print(f"[Listener] Ошибка check_signal {signal.ticker}: {e}")
            await short_ex.close()
            await long_ex.close()
            return

        await short_ex.close()
        await long_ex.close()

        if not risk:
            print(f"[Listener] Пропущен {signal.ticker}: {risk.reason}")
            await notify(f"⏭ Пропущен {signal.ticker}\n{risk.reason}")
            return

        print(f"[Listener] Risk OK: {risk.reason}")
        try:
            trade, msg = await asyncio.wait_for(
                open_position(signal, risk.final_size_usd, signal_received_ms=received_ms),
                timeout=60
            )
        except asyncio.TimeoutError:
            print(f"[Listener] TIMEOUT open_position {signal.ticker}")
            await notify(f"⏱ Таймаут открытия {signal.ticker} (60с)")
            _seen.pop(signal.ticker, None)  # сбрасываем дедуп — можно попробовать снова
            return
        except Exception as e:
            print(f"[Listener] Ошибка open_position {signal.ticker}: {e}")
            await notify(f"❌ Ошибка при открытии {signal.ticker}: {e}")
            _seen.pop(signal.ticker, None)  # сбрасываем дедуп
            return

        # Если сделка не открылась (ошибка биржи) — сбрасываем дедуп
        if trade is None:
            _seen.pop(signal.ticker, None)
        # notify уже вызван внутри open_position с кнопкой TradingView

    async def _close(self, signal: CloseSignal):
        try:
            _, msg = await close_position(signal)
        except Exception as e:
            await notify(f"❌ Ошибка при закрытии {signal.ticker}: {e}")
            return
        await notify(msg)