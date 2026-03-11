"""
debug_minimal.py — обходим load_markets, идём напрямую к нужным эндпоинтам.
Запуск: python debug_minimal.py
"""
import asyncio
import ccxt.async_support as ccxt
from dotenv import load_dotenv
import os

load_dotenv()


async def test_binance_futures_direct():
    """Binance Futures — прямой запрос минуя load_markets"""
    print("\n── BINANCE FUTURES (прямой запрос) ──────────")
    c = ccxt.binance({
        "apiKey": os.getenv("BINANCE_API_KEY",""),
        "secret": os.getenv("BINANCE_API_SECRET",""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })
    # Отключаем авто-загрузку рынков
    c.options['fetchCurrencies'] = False
    try:
        # Прямой вызов futures balance endpoint
        result = await c.fapiPrivateGetAccount()
        assets = result.get('assets', [])
        for a in assets:
            if float(a.get('walletBalance', 0)) > 0:
                print(f"  ✅ {a['asset']}: wallet={a['walletBalance']}  available={a.get('availableBalance','?')}")
        if not any(float(a.get('walletBalance', 0)) > 0 for a in assets):
            print("  Futures баланс = 0 (деньги не переведены на Futures кошелёк)")
    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {str(e)[:150]}")
    await c.close()


async def test_binance_spot_direct():
    """Binance Spot — прямой запрос"""
    print("\n── BINANCE SPOT (прямой запрос) ─────────────")
    c = ccxt.binance({
        "apiKey": os.getenv("BINANCE_API_KEY",""),
        "secret": os.getenv("BINANCE_API_SECRET",""),
        "enableRateLimit": True,
    })
    c.options['fetchCurrencies'] = False
    try:
        result = await c.privateGetAccount()
        balances = result.get('balances', [])
        nonzero = [b for b in balances if float(b.get('free',0)) > 0 or float(b.get('locked',0)) > 0]
        for b in nonzero[:10]:
            print(f"  ✅ {b['asset']}: free={b['free']}  locked={b['locked']}")
        if not nonzero:
            print("  Spot баланс пустой")
    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {str(e)[:150]}")
    await c.close()


async def test_bybit_direct():
    """Bybit — прямой запрос баланса минуя query-info"""
    print("\n── BYBIT (прямой запрос) ────────────────────")
    c = ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY",""),
        "secret": os.getenv("BYBIT_API_SECRET",""),
        "enableRateLimit": True,
    })
    c.options['fetchCurrencies'] = False
    for acc in ['CONTRACT', 'UNIFIED', 'SPOT']:
        try:
            result = await c.privateGetV5AccountWalletBalance({"accountType": acc})
            items = result.get('result', {}).get('list', [])
            for wallet in items:
                for coin in wallet.get('coin', []):
                    if coin.get('coin') == 'USDT' and float(coin.get('walletBalance', 0)) > 0:
                        print(f"  ✅ [{acc}] USDT wallet={coin['walletBalance']}  available={coin.get('availableToWithdraw','?')}")
        except Exception as e:
            print(f"  ❌ [{acc}] {type(e).__name__}: {str(e)[:100]}")
    await c.close()


async def test_mexc_direct():
    """MEXC — прямой запрос"""
    print("\n── MEXC (прямой запрос) ─────────────────────")
    # Spot
    c = ccxt.mexc({
        "apiKey": os.getenv("MEXC_API_KEY",""),
        "secret": os.getenv("MEXC_API_SECRET",""),
        "enableRateLimit": True,
    })
    c.options['fetchCurrencies'] = False
    try:
        result = await c.privateGetAccount()
        balances = result.get('balances', [])
        nonzero = [b for b in balances if float(b.get('free',0)) > 0]
        for b in nonzero[:5]:
            print(f"  ✅ Spot {b['asset']}: free={b['free']}")
    except Exception as e:
        print(f"  ❌ Spot: {type(e).__name__}: {str(e)[:120]}")
    await c.close()

    # Futures contract
    c2 = ccxt.mexc({
        "apiKey": os.getenv("MEXC_API_KEY",""),
        "secret": os.getenv("MEXC_API_SECRET",""),
        "options": {"defaultType": "swap"},
        "enableRateLimit": True,
    })
    c2.options['fetchCurrencies'] = False
    try:
        result = await c2.contractPrivateGetAccountAssets()
        for asset in (result.get('data') or []):
            if float(asset.get('availableBalance', 0)) > 0:
                print(f"  ✅ Futures {asset.get('currency')}: available={asset['availableBalance']}")
    except Exception as e:
        print(f"  ❌ Futures: {type(e).__name__}: {str(e)[:120]}")
    await c2.close()


async def test_gate_direct():
    """Gate.io — прямой запрос futures"""
    print("\n── GATE (прямой запрос) ─────────────────────")
    c = ccxt.gate({
        "apiKey": os.getenv("GATE_API_KEY",""),
        "secret": os.getenv("GATE_API_SECRET",""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })
    c.options['fetchCurrencies'] = False
    try:
        result = await c.privateFuturesGetSettleAccounts({"settle": "usdt"})
        print(f"  ✅ Futures USDT: total={result.get('total')}  available={result.get('available')}")
    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {str(e)[:150]}")
    await c.close()


async def test_kucoin_direct():
    """KuCoin Futures — прямой запрос"""
    print("\n── KUCOIN FUTURES (прямой запрос) ───────────")
    c = ccxt.kucoinfutures({
        "apiKey": os.getenv("KUCOIN_API_KEY",""),
        "secret": os.getenv("KUCOIN_API_SECRET",""),
        "enableRateLimit": True,
    })
    c.options['fetchCurrencies'] = False
    try:
        result = await c.futuresPrivateGetAccountOverview({"currency": "USDT"})
        data = result.get('data', {})
        print(f"  ✅ USDT: available={data.get('availableBalance')}  wallet={data.get('accountEquity')}")
    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {str(e)[:150]}")
    await c.close()


async def test_bitget_direct():
    """Bitget — прямой запрос futures"""
    print("\n── BITGET (прямой запрос) ───────────────────")
    c = ccxt.bitget({
        "apiKey":   os.getenv("BITGET_API_KEY",""),
        "secret":   os.getenv("BITGET_API_SECRET",""),
        "password": os.getenv("BITGET_PASSPHRASE",""),
        "options":  {"defaultType": "swap"},
        "enableRateLimit": True,
    })
    c.options['fetchCurrencies'] = False
    try:
        result = await c.privateMixGetV2MixAccountAccounts({"productType": "USDT-FUTURES"})
        for item in (result.get('data') or []):
            if float(item.get('available', 0)) > 0:
                print(f"  ✅ {item.get('marginCoin')}: available={item['available']}  usdtEquity={item.get('usdtEquity')}")
    except Exception as e:
        try:
            # старый эндпоинт
            result = await c.privateMixGetAccount({"symbol": "BTCUSDT_UMCBL", "marginCoin": "USDT"})
            data = result.get('data', {})
            print(f"  ✅ USDT available={data.get('available')}")
        except Exception as e2:
            print(f"  ❌ {type(e).__name__}: {str(e)[:120]}")
    await c.close()


async def main():
    print("="*50)
    print("  ПРЯМЫЕ ЗАПРОСЫ — минуем load_markets")
    print("="*50)
    await test_binance_spot_direct()
    await test_binance_futures_direct()
    await test_bybit_direct()
    await test_mexc_direct()
    await test_gate_direct()
    await test_kucoin_direct()
    await test_bitget_direct()
    print("\n" + "="*50)
    print("  Готово — скинь вывод")
    print("="*50)

asyncio.run(main())