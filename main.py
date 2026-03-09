"""
main.py — запускает бота и GUI-дашборд одновременно.

Запуск:
  python main.py

  Бот: слушает Telegram-канал, торгует по сигналам
  GUI: http://localhost:8080
"""

import asyncio
import uvicorn

from db import db_init


async def run_bot():
    from listener import SignalListener
    listener = SignalListener()
    try:
        await listener.start()
    except Exception as e:
        print(f"[Bot] Fatal: {e}")


async def run_gui():
    config = uvicorn.Config(
        "gui.server:app",
        host="0.0.0.0",
        port=8080,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    db_init()
    print("=" * 50)
    print("  ARB BOT — LIVE MODE")
    print("  Dashboard → http://localhost:8080")
    print("=" * 50)
    await asyncio.gather(run_bot(), run_gui())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Stopped.")
