"""
notifications/telegram_notify.py — уведомления о сделках.

Отправляет сообщения в твой личный чат/группу через бота.
"""

import aiohttp
from config.settings import NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID


async def send_notification(text: str, parse_mode: str = "HTML"):
    """
    Отправляет сообщение в Telegram.
    Не бросает исключение — ошибки только логируем,
    чтобы не прерывать торговый цикл.
    """
    if not NOTIFY_BOT_TOKEN or not NOTIFY_CHAT_ID:
        print(f"[Notify] (no bot configured) {text}")
        return

    url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": NOTIFY_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    body = await r.text()
                    print(f"[Notify] Telegram error {r.status}: {body}")
    except Exception as e:
        print(f"[Notify] Failed to send notification: {e}")
