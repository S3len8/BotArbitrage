import aiohttp
from settings import NOTIFY_BOT_TOKEN, NOTIFY_CHAT_ID


async def notify(text: str):
    if not NOTIFY_BOT_TOKEN or not NOTIFY_CHAT_ID:
        print(f"[Notify] {text}")
        return
    url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={
                "chat_id": NOTIFY_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        print(f"[Notify] error: {e}")
