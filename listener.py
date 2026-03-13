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

# Тикеры из закреплённого сообщения — только они разрешены для edit-сигналов
_pinned_tickers: set[str] = set()
# ID закреплённого сообщения — чтобы отличить его редактирование от обычных
_pinned_msg_id: int | None = None


def _is_duplicate(ticker: str) -> bool:
    now = time.time()
    if now - _seen.get(ticker, 0) < DEDUP_WINDOW_S:
        return True
    _seen[ticker] = now
    return False


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

            # Обычное редактирование — только если тикер есть в закреплённом
            await self._handle(event.message.text or "", received_ms, is_edit=True)

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
            signals = parse_pinned(text)
            _pinned_tickers = {s.ticker for s in signals}
            print(f"[Listener] Закреплённое: {len(signals)} спредов → тикеры: {_pinned_tickers}")
        except Exception as e:
            print(f"[Listener] Ошибка чтения закреплённого: {e}")

    async def _handle(self, text: str, received_ms: int, is_edit: bool):
        signal = parse_message(text)
        if signal is None:
            return

        if isinstance(signal, OpenSignal):
            # Редактирование — только если тикер есть в закреплённом
            if is_edit and signal.ticker not in _pinned_tickers:
                print(f"[Listener] edit {signal.ticker} — не в закреплённом, пропускаем")
                return
            source = "edit" if is_edit else "new"
            print(f"[Listener] [{source}] OPEN {signal.ticker} {signal.short_exchange}↕{signal.long_exchange} {signal.spread_pct:.2f}%")
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
        await notify(msg)

    async def _close(self, signal: CloseSignal):
        try:
            _, msg = await close_position(signal)
        except Exception as e:
            await notify(f"❌ Ошибка при закрытии {signal.ticker}: {e}")
            return
        await notify(msg)