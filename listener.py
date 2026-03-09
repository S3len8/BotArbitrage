"""
telegram/listener.py — слушает сигнальный канал через Telethon.

Telethon используем потому что нужно читать чужой канал
(не только сообщения боту), а это требует user-сессию, не bot-токен.

Поток обработки:
  Новое сообщение → parse_message() → RiskCheck → OrderExecutor → Notify
"""

import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel

from config.settings import TG_API_ID, TG_API_HASH, TG_SESSION, SIGNAL_CHANNEL, DEMO_MODE
from telegram.signal_parser import parse_message, OpenSignal, CloseSignal
from core.risk_manager import check_signal
from core.order_executor import open_position, close_position
from notifications.telegram_notify import send_notification
from exchanges.base import create_exchange
from storage.db import db_init


class SignalListener:
    def __init__(self):
        self.client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
        self._running = False

    async def start(self):
        """Запускает слушатель."""
        db_init()

        await self.client.start()
        print(f"[Listener] Logged in as {(await self.client.get_me()).username}")
        print(f"[Listener] Listening to: {SIGNAL_CHANNEL}")
        print(f"[Listener] Mode: {'🟡 DEMO' if DEMO_MODE else '🔴 LIVE'}")

        await send_notification(
            f"🤖 Арбитражный бот запущен\n"
            f"Режим: {'🟡 DEMO (paper trading)' if DEMO_MODE else '🔴 LIVE'}\n"
            f"Канал: {SIGNAL_CHANNEL}"
        )

        @self.client.on(events.NewMessage(chats=SIGNAL_CHANNEL))
        async def handler(event):
            await self._handle_message(event.message.text or "")

        self._running = True
        await self.client.run_until_disconnected()

    async def _handle_message(self, text: str):
        """Обрабатывает одно входящее сообщение."""
        if not text.strip():
            return

        print(f"[Listener] New message: {text[:80]}...")

        signal = parse_message(text)

        if signal is None:
            # Не сигнал — игнорируем
            return

        # ---- OPEN сигнал ----
        if isinstance(signal, OpenSignal):
            await self._handle_open(signal)

        # ---- CLOSE сигнал ----
        elif isinstance(signal, CloseSignal):
            await self._handle_close(signal)

    async def _handle_open(self, signal: OpenSignal):
        print(f"[Listener] OPEN signal: {signal.ticker} "
              f"short={signal.short_exchange} long={signal.long_exchange} "
              f"spread={signal.spread_pct}%")

        # Создаём клиентов бирж для проверки спреда
        try:
            short_ex = create_exchange(signal.short_exchange)
            long_ex  = create_exchange(signal.long_exchange)
        except Exception as e:
            msg = f"❌ Ошибка создания клиента биржи для {signal.ticker}: {e}"
            print(f"[Listener] {msg}")
            await send_notification(msg)
            return

        # Risk check
        risk = await check_signal(signal, short_ex, long_ex)
        await short_ex.close()
        await long_ex.close()

        if not risk:
            msg = f"⏭ Сигнал {signal.ticker} пропущен\n{risk.reason}"
            print(f"[Listener] {msg}")
            await send_notification(msg)
            return

        print(f"[Listener] Risk OK: {risk.reason}")

        # Открываем позицию
        try:
            trade, msg = await open_position(signal)
        except Exception as e:
            err_msg = f"❌ Критическая ошибка при открытии {signal.ticker}: {e}"
            print(f"[Listener] {err_msg}")
            await send_notification(err_msg)
            return

        await send_notification(msg)

    async def _handle_close(self, signal: CloseSignal):
        print(f"[Listener] CLOSE signal: {signal.ticker}")

        try:
            trade, msg = await close_position(signal)
        except Exception as e:
            err_msg = f"❌ Критическая ошибка при закрытии {signal.ticker}: {e}"
            print(f"[Listener] {err_msg}")
            await send_notification(err_msg)
            return

        await send_notification(msg)

    async def stop(self):
        self._running = False
        await self.client.disconnect()
