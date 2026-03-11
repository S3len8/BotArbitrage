"""
debug_direct.py — тест балансов через новый exchanges.py (requests-based)
Запуск: python debug_direct.py
"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

from exchanges import BinanceExchange, BybitExchange, MexcExchange, BitgetExchange, KucoinExchange, GateExchange

PAIRS = [
    ("BINANCE", BinanceExchange),
    ("BYBIT",   BybitExchange),
    ("MEXC",    MexcExchange),
    ("BITGET",  BitgetExchange),
    ("KUCOIN",  KucoinExchange),
    ("GATE",    GateExchange),
]

async def main():
    print("="*50)
    print("  ТЕСТ БАЛАНСОВ (requests-based)")
    print("="*50)
    for name, cls in PAIRS:
        key = os.getenv(f"{name}_API_KEY", "")
        if not key:
            print(f"\n  {name}: нет ключа в .env")
            continue
        print(f"\n── {name} ────────────────────────────────")
        try:
            ex = cls()
            bal = await ex.get_futures_balance()
            print(f"  Futures balance: ${bal:.4f} USDT")
            await ex.close()
        except Exception as e:
            print(f"  {type(e).__name__}: {e}")
    print("\n" + "="*50)

asyncio.run(main())