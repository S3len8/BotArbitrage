"""
listener.py — слушает Telegram-канал и запускает торговлю.
Запускается и останавливается через GUI (кнопка START/STOP).
"""

import time
from telethon import TelegramClient, events
from settings import TG_API_ID, TG_API_HASH, TG_SESSION, SIGNAL_CHANNEL
from signal_parser import parse_message, OpenSignal, CloseSignal
from risk_manager import check_signal
from order_executor import open_position, close_position
from exchanges import create_exchange
from notifier import notify


class SignalListener:

    def __init__(self):
        self.client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
        self._running = False

    async def start(self):
        self._running = True
        await self.client.start()
        me = await self.client.get_me()
        print(f"[Listener] Logged in as {me.username}")
        print(f"[Listener] Watching: {SIGNAL_CHANNEL}")
        await notify(f"🟢 <b>Бот запущен</b>\nКанал: {SIGNAL_CHANNEL}")

        @self.client.on(events.NewMessage(chats=SIGNAL_CHANNEL))
        async def handler(event):
            if self._running:
                # Фиксируем время получения сигнала сразу
                received_ms = int(time.time() * 1000)
                await self._handle(event.message.text or "", received_ms)

        await self.client.run_until_disconnected()

    async def stop(self):
        self._running = False
        await notify("🔴 <b>Бот остановлен</b>")
        await self.client.disconnect()

    async def _handle(self, text: str, received_ms: int):
        signal = parse_message(text)
        if signal is None:
            return
        if isinstance(signal, OpenSignal):
            await self._open(signal, received_ms)
        elif isinstance(signal, CloseSignal):
            await self._close(signal)

    async def _open(self, signal: OpenSignal, received_ms: int):
        print(f"[Listener] OPEN {signal.ticker} {signal.short_exchange}↕{signal.long_exchange} {signal.spread_pct:.2f}%")
        try:
            short_ex = create_exchange(signal.short_exchange)
            long_ex  = create_exchange(signal.long_exchange)
        except Exception as e:
            await notify(f"❌ Ошибка биржи для {signal.ticker}: {e}")
            return

        risk = await check_signal(signal, short_ex, long_ex)
        await short_ex.close()
        await long_ex.close()

        if not risk:
            await notify(f"⏭ Пропущен {signal.ticker}\n{risk.reason}")
            return

        print(f"[Listener] Risk OK: {risk.reason}")
        try:
            _, msg = await open_position(signal, risk.final_size_usd, signal_received_ms=received_ms)
        except Exception as e:
            await notify(f"❌ Ошибка при открытии {signal.ticker}: {e}")
            return
        await notify(msg)

    async def _close(self, signal: CloseSignal):
        print(f"[Listener] CLOSE {signal.ticker}")
        try:
            _, msg = await close_position(signal)
        except Exception as e:
            await notify(f"❌ Ошибка при закрытии {signal.ticker}: {e}")
            return
        await notify(msg)
