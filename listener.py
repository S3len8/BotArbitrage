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
from notifier import notify

DEDUP_WINDOW_S = 120
_seen: dict[str, float] = {}

# Тикеры из закреплённого сообщения
_pinned_tickers: set[str] = set()
_pinned_msg_id: int | None = None


def _is_duplicate(ticker: str) -> bool:
    now = time.time()
    if now - _seen.get(ticker, 0) < DEDUP_WINDOW_S:
        return True
    _seen[ticker] = now
    return False


def _funding_ok(signal) -> tuple[bool, str]:
    """Проверяет фандинг-фильтр. Возвращает (ok, причина).
    Логика:
      ✅ Условие 1: оба фандинга < 1.5% И разница < 0.2%
      ✅ Условие 2: интервалы 4ч/8ч И:
           - оба фандинга < 0.5% → разница < 0.3%
           - хотя бы один ≥ 0.5% → разница < 0.2%
      ✅ Условие 3: оба интервала 1ч И разница = 0%
      ❌ всё остальное
    Если фандинг неизвестен — не блокируем.
    """
    from settings import MAX_FUNDING_DIFF_PCT, MAX_FUNDING_ABS_PCT
    fs = signal.funding_short
    fl = signal.funding_long
    if fs is None or fl is None:
        return True, "фандинг неизвестен — не блокируем"

    diff_pct  = abs(fs - fl) * 100
    short_abs = abs(fs) * 100
    long_abs  = abs(fl) * 100
    i_s = signal.interval_short
    i_l = signal.interval_long

    # Условие 1: оба < 1.5% И разница < 0.2%
    if diff_pct <= MAX_FUNDING_DIFF_PCT and short_abs < MAX_FUNDING_ABS_PCT and long_abs < MAX_FUNDING_ABS_PCT:
        return True, f"фандинг OK: short={fs*100:+.3f}% long={fl*100:+.3f}% diff={diff_pct:.3f}%"

    # Условие 2: интервалы 4ч/8ч с динамичным порогом разницы
    if i_s in (4, 8) and i_l in (4, 8):
        dynamic_max = 0.3 if (short_abs < 0.5 and long_abs < 0.5) else 0.2
        if diff_pct <= dynamic_max:
            return True, (f"фандинг исключение: short={fs*100:+.3f}% long={fl*100:+.3f}% "
                          f"diff={diff_pct:.3f}% ≤ {dynamic_max}%, интервалы {i_s}ч/{i_l}ч")
        return False, (f"фандинг: интервалы {i_s}ч/{i_l}ч, diff={diff_pct:.3f}% "
                       f"> {dynamic_max}% ({'оба<0.5%→0.3%' if short_abs < 0.5 and long_abs < 0.5 else 'один≥0.5%→0.2%'})")

    # Условие 3: оба интервала 1ч, оба фандинга < 0.5%, разница < 0.15%
    if i_s == 1 and i_l == 1:
        if short_abs < 0.5 and long_abs < 0.5 and diff_pct < 0.15:
            return True, (f"фандинг исключение 1ч/1ч: "
                          f"short={fs*100:+.3f}% long={fl*100:+.3f}% diff={diff_pct:.3f}%")
        return False, (f"фандинг: 1ч/1ч но не прошёл — "
                       f"short={fs*100:+.3f}% long={fl*100:+.3f}% diff={diff_pct:.3f}% "
                       f"(нужно: оба<0.5% и diff<0.15%)")

    # Интервал неизвестен — применяем базовый порог 0.2%
    if i_s is None or i_l is None:
        if diff_pct <= MAX_FUNDING_DIFF_PCT:
            return True, f"фандинг: diff={diff_pct:.3f}% OK, интервалы неизвестны"
        return False, f"фандинг: diff={diff_pct:.3f}% > {MAX_FUNDING_DIFF_PCT}%, интервалы неизвестны"

    return False, (f"фандинг: интервалы {i_s}ч/{i_l}ч — не 4ч/8ч/1ч, diff={diff_pct:.3f}%")


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

            if is_edit:
                if is_recent:
                    source = "edit+recent"
                elif in_pinned:
                    source = "edit+pinned"
                else:
                    print(f"[Listener] edit {signal.ticker} — не в закреплённом и не свежее, пропускаем")
                    return
            else:
                source = "new"

            # Фандинг-фильтр применяется ко ВСЕМ типам сообщений
            ok, reason = _funding_ok(signal)
            if not ok:
                print(f"[Listener] [{source}] {signal.ticker} — {reason}, пропускаем")
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
            _, msg = await asyncio.wait_for(
                open_position(signal, risk.final_size_usd, signal_received_ms=received_ms),
                timeout=60
            )
        except asyncio.TimeoutError:
            print(f"[Listener] TIMEOUT open_position {signal.ticker}")
            await notify(f"⏱ Таймаут открытия {signal.ticker} (60с)")
            return
        except Exception as e:
            print(f"[Listener] Ошибка open_position {signal.ticker}: {e}")
            await notify(f"❌ Ошибка при открытии {signal.ticker}: {e}")
            return
        # notify уже вызван внутри open_position с кнопкой TradingView

    async def _close(self, signal: CloseSignal):
        try:
            _, msg = await close_position(signal)
        except Exception as e:
            await notify(f"❌ Ошибка при закрытии {signal.ticker}: {e}")
            return
        await notify(msg)