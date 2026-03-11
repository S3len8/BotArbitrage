"""
check_balances.py — диагностика балансов по всем биржам.
Запуск: python check_balances.py

Показывает:
- Подключился ли к бирже
- Какой баланс вернула биржа (spot + futures отдельно)
- Точный текст ошибки если что-то пошло не так
"""

import asyncio
import ccxt.async_support as ccxt
from dotenv import load_dotenv
import os

load_dotenv()

EXCHANGES_CONFIG = {
    "bybit": {
        "class": ccxt.bybit,
        "params": {
            "apiKey": os.getenv("BYBIT_API_KEY", ""),
            "secret": os.getenv("BYBIT_API_SECRET", ""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        },
        "balance_params": [
            {"accountType": "CONTRACT"},   # futures
            {"accountType": "UNIFIED"},    # unified margin
            {},                            # default
        ],
    },
    "mexc": {
        "class": ccxt.mexc,
        "params": {
            "apiKey": os.getenv("MEXC_API_KEY", ""),
            "secret": os.getenv("MEXC_API_SECRET", ""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        },
        "balance_params": [
            {"type": "future"},
            {},
        ],
    },
    "binance": {
        "class": ccxt.binance,
        "params": {
            "apiKey": os.getenv("BINANCE_API_KEY", ""),
            "secret": os.getenv("BINANCE_API_SECRET", ""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        },
        "balance_params": [
            {"type": "future"},
            {},
        ],
    },
    "bitget": {
        "class": ccxt.bitget,
        "params": {
            "apiKey": os.getenv("BITGET_API_KEY", ""),
            "secret": os.getenv("BITGET_API_SECRET", ""),
            "password": os.getenv("BITGET_PASSPHRASE", ""),
            "options": {"defaultType": "swap"},
            "enableRateLimit": True,
        },
        "balance_params": [
            {"type": "swap"},
            {"marginCoin": "USDT"},
            {},
        ],
    },
    "kucoin": {
        "class": ccxt.kucoinfutures,  # KuCoin Futures — отдельный класс!
        "params": {
            "apiKey": os.getenv("KUCOIN_API_KEY", ""),
            "secret": os.getenv("KUCOIN_API_SECRET", ""),
            "password": os.getenv("KUCOIN_PASSPHRASE", ""),
            "enableRateLimit": True,
        },
        "balance_params": [
            {},
            {"type": "futures"},
        ],
    },
    "gate": {
        "class": ccxt.gate,
        "params": {
            "apiKey": os.getenv("GATE_API_KEY", ""),
            "secret": os.getenv("GATE_API_SECRET", ""),
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        },
        "balance_params": [
            {"type": "future"},
            {"settle": "usdt"},
            {},
        ],
    },
    "ourbit": {
        "class": getattr(ccxt, "ourbit", None),
        "params": {
            "apiKey": os.getenv("OURBIT_API_KEY", ""),
            "secret": os.getenv("OURBIT_API_SECRET", ""),
            "enableRateLimit": True,
        },
        "balance_params": [{}],
    },
}

SEP = "─" * 55


async def check_exchange(name: str, cfg: dict):
    print(f"\n{'═'*55}")
    print(f"  {name.upper()}")
    print(SEP)

    if cfg["class"] is None:
        print(f"  ❌ Не найден в ccxt (OURBIT может не поддерживаться)")
        return

    api_key = cfg["params"].get("apiKey", "")
    if not api_key:
        print(f"  ⚠️  API ключ не задан в .env — пропускаю")
        return

    print(f"  API key: {api_key[:6]}...{api_key[-4:] if len(api_key) > 10 else '???'}")

    client = cfg["class"](cfg["params"])
    try:
        # Пробуем все варианты параметров balance_params
        success = False
        for bp in cfg["balance_params"]:
            try:
                balance = await client.fetch_balance(bp)
                usdt = balance.get("USDT", {})
                total  = usdt.get("total", 0) or 0
                free   = usdt.get("free",  0) or 0
                used   = usdt.get("used",  0) or 0

                print(f"  ✅ Подключено (params={bp})")
                print(f"     USDT total : ${float(total):.4f}")
                print(f"     USDT free  : ${float(free):.4f}")
                print(f"     USDT used  : ${float(used):.4f}")

                # Показываем все ненулевые активы
                others = {k: v for k, v in balance.items()
                          if isinstance(v, dict)
                          and k != 'USDT'
                          and float(v.get('total', 0) or 0) > 0}
                if others:
                    print(f"     Другие активы: {list(others.keys())}")

                success = True
                break
            except Exception as e:
                print(f"  ⚠️  params={bp} → {type(e).__name__}: {str(e)[:120]}")

        if not success:
            print(f"  ❌ Все варианты параметров провалились")

    except Exception as e:
        print(f"  ❌ Критическая ошибка: {type(e).__name__}: {e}")
    finally:
        await client.close()


async def main():
    print("\n" + "═"*55)
    print("  ARB BOT — ДИАГНОСТИКА БАЛАНСОВ")
    print("═"*55)
    print("  Проверяю подключение к биржам...")

    for name, cfg in EXCHANGES_CONFIG.items():
        await check_exchange(name, cfg)

    print(f"\n{'═'*55}")
    print("  Готово. Скопируй весь вывод если нужна помощь.")
    print("═"*55 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
