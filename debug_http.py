"""
debug_http.py — сравниваем requests (как python-binance) vs aiohttp (как ccxt)
Запуск: python debug_http.py
"""
import asyncio
import aiohttp
import requests
import hmac, hashlib, time
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY    = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")


def sign(params: dict) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()


def make_params():
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 10000}
    params["signature"] = sign(params)
    return params


print("=" * 55)
print("  HTTP ТЕСТ — requests vs aiohttp")
print("=" * 55)

# ── 1. requests (синхронный, как python-binance) ──────────────
print("\n── 1. requests (синхронный) ──────────────────────")
try:
    url = "https://fapi.binance.com/fapi/v2/balance"
    r = requests.get(url, params=make_params(),
                     headers={"X-MBX-APIKEY": API_KEY}, timeout=10)
    print(f"  Status: {r.status_code}")
    print(f"  Body: {r.text[:300]}")
except Exception as e:
    print(f"  ❌ {type(e).__name__}: {e}")

# ── 2. requests — Spot ────────────────────────────────────────
print("\n── 2. requests Spot account ──────────────────────")
try:
    url = "https://api.binance.com/api/v3/account"
    r = requests.get(url, params=make_params(),
                     headers={"X-MBX-APIKEY": API_KEY}, timeout=10)
    print(f"  Status: {r.status_code}")
    text = r.text[:300]
    print(f"  Body: {text}")
except Exception as e:
    print(f"  ❌ {type(e).__name__}: {e}")

# ── 3. aiohttp (как ccxt) ─────────────────────────────────────
async def test_aiohttp():
    print("\n── 3. aiohttp (как ccxt) ─────────────────────")
    try:
        url = "https://fapi.binance.com/fapi/v2/balance"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=make_params(),
                headers={"X-MBX-APIKEY": API_KEY},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                text = await r.text()
                print(f"  Status: {r.status}")
                print(f"  Body: {text[:300]}")
    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {e}")

asyncio.run(test_aiohttp())

print("\n" + "=" * 55)
print("  Если requests ✅ а aiohttp ❌ — проблема в SSL/прокси aiohttp")
print("  Если оба ✅ — код нормально работает")  
print("  Если оба ❌ — блокировка по IP")
print("=" * 55)
