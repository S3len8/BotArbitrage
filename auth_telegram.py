"""
auth_telegram.py — одноразовая авторизация Telegram.
Запусти этот скрипт ОДИН РАЗ перед main.py:
    python auth_telegram.py

После успешной авторизации создаётся файл сессии arb_bot_session.session
Больше этот скрипт запускать не нужно.
"""

import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()

TG_API_ID   = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_SESSION  = os.getenv("TG_SESSION", "arb_bot_session")


async def main():
    print("=" * 50)
    print("  ARB BOT — Авторизация Telegram")
    print("=" * 50)
    print(f"  Session file: {TG_SESSION}.session")
    print()

    client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
    await client.start()

    me = await client.get_me()
    print()
    print(f"✅ Успешно! Вошли как: {me.first_name} (@{me.username})")
    print(f"✅ Сессия сохранена в: {TG_SESSION}.session")
    print()
    print("Теперь можно запускать main.py")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
