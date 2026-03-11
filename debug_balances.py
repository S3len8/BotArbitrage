"""
debug_balances.py — показывает точно что возвращает каждая биржа.
Запуск: python debug_balances.py
"""
import asyncio
import ccxt.async_support as ccxt
from dotenv import load_dotenv
import os

load_dotenv()

async def check(name, client, balance_calls):
    print(f"\n{'─'*50}")
    print(f"  {name.upper()}")
    key = os.getenv(f"{name.upper()}_API_KEY", "")
    if not key:
        print(f"  ⚠  Нет API ключа в .env")
        await client.close()
        return

    print(f"  Key: {key[:6]}...{key[-4:]}")
    for label, params in balance_calls:
        try:
            b = await client.fetch_balance(params)
            usdt = b.get("USDT") or b.get("usdt") or {}
            total = float(usdt.get("total") or 0)
            free  = float(usdt.get("free")  or 0)
            print(f"  ✅ [{label}] USDT total=${total:.4f}  free=${free:.4f}")
            # Показать ВСЕ ненулевые активы
            nonzero = {k: v for k,v in b.items()
                       if isinstance(v, dict) and float(v.get("total") or 0) > 0}
            if nonzero:
                for coin, val in list(nonzero.items())[:5]:
                    print(f"       {coin}: total={val.get('total')}  free={val.get('free')}")
        except Exception as e:
            print(f"  ❌ [{label}] {type(e).__name__}: {str(e)[:120]}")
    await client.close()


async def main():
    print("="*50)
    print("  DEBUG BALANCES")
    print("="*50)

    tasks = [
        ("bybit", ccxt.bybit({
            "apiKey": os.getenv("BYBIT_API_KEY",""),
            "secret": os.getenv("BYBIT_API_SECRET",""),
            "enableRateLimit": True,
        }), [
            ("CONTRACT",  {"accountType": "CONTRACT"}),
            ("UNIFIED",   {"accountType": "UNIFIED"}),
            ("default",   {}),
        ]),
        ("mexc", ccxt.mexc({
            "apiKey": os.getenv("MEXC_API_KEY",""),
            "secret": os.getenv("MEXC_API_SECRET",""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        }), [
            ("future",  {"type": "future"}),
            ("swap",    {"type": "swap"}),
            ("default", {}),
        ]),
        ("binance", ccxt.binance({
            "apiKey": os.getenv("BINANCE_API_KEY",""),
            "secret": os.getenv("BINANCE_API_SECRET",""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        }), [
            ("future",  {"type": "future"}),
            ("default", {}),
        ]),
        ("bitget", ccxt.bitget({
            "apiKey":    os.getenv("BITGET_API_KEY",""),
            "secret":    os.getenv("BITGET_API_SECRET",""),
            "password":  os.getenv("BITGET_PASSPHRASE",""),
            "options":   {"defaultType": "swap"},
            "enableRateLimit": True,
        }), [
            ("swap",    {"type": "swap"}),
            ("default", {}),
        ]),
        ("kucoin", ccxt.kucoinfutures({
            "apiKey": os.getenv("KUCOIN_API_KEY",""),
            "secret": os.getenv("KUCOIN_API_SECRET",""),
            "enableRateLimit": True,
        }), [
            ("default", {}),
            ("futures", {"type": "futures"}),
        ]),
        ("gate", ccxt.gate({
            "apiKey": os.getenv("GATE_API_KEY",""),
            "secret": os.getenv("GATE_API_SECRET",""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        }), [
            ("usdt settle", {"settle": "usdt"}),
            ("future",      {"type": "future"}),
            ("default",     {}),
        ]),
    ]

    for name, client, calls in tasks:
        await check(name, client, calls)

    print(f"\n{'='*50}")
    print("  Готово. Скинь вывод — скажу что исправить.")
    print("="*50)

asyncio.run(main())
