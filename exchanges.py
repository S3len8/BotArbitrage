"""
exchanges.py — адаптеры всех бирж.

get_futures_balance() использует requests (синхронный, как python-binance)
запущенный в executor — работает на Windows где aiohttp/ccxt падает из-за DNS.
Торговые операции (place_market_order, close_position) используют ccxt.
"""

import asyncio
import hashlib
import hmac
import math
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import ccxt.async_support as ccxt
import requests

from settings import get_exchange_keys, LEVERAGE

# Один executor для всех синхронных вызовов баланса
_executor = ThreadPoolExecutor(max_workers=10)

def _run_sync(fn, *args, **kwargs):
    """Запускает синхронную функцию в asyncio без блокировки."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))


# ── Базовый класс ─────────────────────────────────────────────

class BaseExchange(ABC):
    name: str

    @abstractmethod
    async def get_price(self, symbol: str) -> float: ...
    @abstractmethod
    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict: ...
    @abstractmethod
    async def close_position(self, symbol: str, side: str, qty: float) -> dict: ...
    @abstractmethod
    async def get_futures_balance(self) -> float: ...

    async def get_max_position_size(self, symbol: str) -> Optional[float]:
        return None

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """Объём торгов за 24ч в USD. None если недоступно."""
        return None

    async def get_position_size_usd(self) -> float:
        return await self.get_futures_balance() * LEVERAGE

    async def close(self): pass


# ── Helpers для подписи ───────────────────────────────────────

def _sign_hmac_sha256(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000)

def _req(method: str, url: str, headers: dict = None, params: dict = None, json: dict = None) -> dict:
    """Синхронный HTTP запрос через requests."""
    r = requests.request(method, url, headers=headers or {}, params=params, json=json, timeout=15)
    r.raise_for_status()
    return r.json()

import re as _re_ticker

def _gate_ticker(symbol: str) -> str:
    """Конвертирует символ в формат Gate: BASUSDT или BAS/USDT:USDT → BAS_USDT"""
    s = symbol.replace('/', '_').replace(':USDT', '')
    if s.endswith('_USDT'):
        return s
    s = _re_ticker.sub(r'USDT$', '', s).rstrip('_')
    return s + '_USDT'


def _mexc_ticker(symbol: str) -> str:
    """Конвертирует символ в формат MEXC: BASUSDT или BAS/USDT:USDT → BAS_USDT"""
    s = symbol.replace('/', '_').replace(':USDT', '')
    if s.endswith('_USDT'):
        return s
    s = _re_ticker.sub(r'USDT$', '', s).rstrip('_')
    return s + '_USDT'


# ── Базовый ccxt адаптер (торговые операции) ──────────────────

class CcxtExchange(BaseExchange):

    def __init__(self, exchange_id: str, extra: dict = None):
        self.name = exchange_id
        keys = get_exchange_keys(exchange_id)
        params = {
            'apiKey':          keys.get('api_key', ''),
            'secret':          keys.get('api_secret', ''),
            'options':         {'defaultType': 'future'},
            'enableRateLimit': True,
        }
        if keys.get('api_password'):
            params['password'] = keys['api_password']
        if extra:
            params.update(extra)
        self._client = getattr(ccxt, exchange_id)(params)

    async def get_price(self, symbol: str) -> float:
        """Получает цену через requests — обходит aiohttp DNS проблему на Windows."""
        return await _run_sync(self._price_sync, symbol)

    def _price_sync(self, symbol: str) -> float:
        """Переопределяется в каждом классе."""
        raise NotImplementedError(f"{self.name}: _price_sync не реализован")

    async def get_futures_balance(self) -> float:
        raise NotImplementedError

    async def get_max_position_size(self, symbol: str) -> Optional[float]:
        try:
            markets = await self._client.load_markets()
            market  = markets.get(self._fmt(symbol), {})
            v = market.get('limits', {}).get('cost', {}).get('max')
            return float(v) if v else None
        except Exception:
            return None

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        price   = await self.get_price(symbol)
        min_qty = await self._min_qty(symbol)
        qty     = _round(size_usd / price, min_qty)
        if qty < min_qty:
            raise ValueError(f"{self.name}: qty {qty:.8f} < min {min_qty} for {symbol}")
        await self._setup_symbol(symbol)   # изолированная маржа + плечо
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params=self._open_params(side)
        )
        return {
            'order_id': str(order['id']),
            'price':    float(order.get('average') or order.get('price') or price),
            'qty':      float(order.get('filled') or qty),
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params={'reduceOnly': True}
        )
        return {
            'order_id': str(order['id']),
            'price':    float(order.get('average') or order.get('price') or 0),
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }

    async def _setup_symbol(self, symbol: str):
        """Выставляет изолированную маржу и нужное плечо перед ордером.
        Каждая биржа переопределяет этот метод своей API-логикой."""
        # Базовая реализация через ccxt — переопределяется в каждом классе
        try:
            await self._client.set_leverage(LEVERAGE, self._fmt(symbol))
        except Exception as e:
            print(f"[{self.name}] set_leverage via ccxt: {e}")

    # Оставляем для обратной совместимости
    async def _set_leverage(self, symbol: str):
        await self._setup_symbol(symbol)

    async def _min_qty(self, symbol: str) -> float:
        markets = await self._client.load_markets()
        return float(markets.get(self._fmt(symbol), {}).get('limits', {}).get('amount', {}).get('min', 0.001))

    def _fmt(self, symbol: str) -> str:
        if '/' in symbol: return symbol
        return f"{symbol.replace('USDT', '')}/USDT:USDT"

    def _open_params(self, side: str) -> dict:
        return {}

    async def close(self):
        await self._client.close()


# ── Биржи — баланс через requests ────────────────────────────

class BinanceExchange(CcxtExchange):
    def __init__(self):
        super().__init__('binance', {'options': {'defaultType': 'future'}})
        keys = get_exchange_keys('binance')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']

    def _price_sync(self, symbol: str) -> float:
        ticker = symbol.replace('/', '').replace(':USDT', '')
        r = requests.get('https://fapi.binance.com/fapi/v1/ticker/bookTicker',
                         params={'symbol': ticker}, timeout=10)
        r.raise_for_status()
        d = r.json()
        price = d.get('askPrice') or d.get('bidPrice') or d.get('lastPrice')
        if not price:
            raise ValueError(f"binance: no price for {ticker}, response: {d}")
        return float(price)

    def _balance_sync(self) -> float:
        ts  = _ts()
        msg = f"timestamp={ts}&recvWindow=10000"
        sig = _sign_hmac_sha256(self._api_secret, msg)
        data = _req('GET', 'https://fapi.binance.com/fapi/v2/balance',
                    headers={'X-MBX-APIKEY': self._api_key},
                    params={'timestamp': ts, 'recvWindow': 10000, 'signature': sig})
        for asset in data:
            if asset.get('asset') == 'USDT':
                return float(asset.get('availableBalance') or 0)
        return 0.0

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '')
            r = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr',
                             params={'symbol': ticker}, timeout=10)
            r.raise_for_status()
            return float(r.json().get('quoteVolume') or 0)
        return await _run_sync(_sync)

    async def _setup_symbol(self, symbol: str):
        """Binance Futures: ISOLATED margin + leverage через прямые REST вызовы."""
        ticker = self._fmt(symbol).replace('/', '').replace(':USDT', '')

        def _sync():
            ts  = _ts()
            # 1. Выставляем marginType = ISOLATED
            msg = f"symbol={ticker}&marginType=ISOLATED&timestamp={ts}&recvWindow=5000"
            sig = _sign_hmac_sha256(self._api_secret, msg)
            try:
                requests.post('https://fapi.binance.com/fapi/v1/marginType',
                    headers={'X-MBX-APIKEY': self._api_key},
                    params={'symbol': ticker, 'marginType': 'ISOLATED',
                            'timestamp': ts, 'recvWindow': 5000, 'signature': sig},
                    timeout=10)
                # 200 или 400 "No need to change" — оба ок
            except Exception as e:
                print(f"[binance] marginType: {e}")

            # 2. Выставляем leverage
            ts2  = _ts()
            msg2 = f"symbol={ticker}&leverage={LEVERAGE}&timestamp={ts2}&recvWindow=5000"
            sig2 = _sign_hmac_sha256(self._api_secret, msg2)
            try:
                requests.post('https://fapi.binance.com/fapi/v1/leverage',
                    headers={'X-MBX-APIKEY': self._api_key},
                    params={'symbol': ticker, 'leverage': LEVERAGE,
                            'timestamp': ts2, 'recvWindow': 5000, 'signature': sig2},
                    timeout=10)
            except Exception as e:
                print(f"[binance] leverage: {e}")

        await _run_sync(_sync)
        print(f"[binance] {ticker}: ISOLATED, {LEVERAGE}×")

    def _open_params(self, side): return {'type': 'MARKET'}


class BybitExchange(CcxtExchange):
    def __init__(self):
        super().__init__('bybit')
        keys = get_exchange_keys('bybit')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']

    def _price_sync(self, symbol: str) -> float:
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDT'
        r = requests.get('https://api.bybit.com/v5/market/tickers',
                         params={'category': 'linear', 'symbol': ticker}, timeout=10)
        r.raise_for_status()
        items = r.json().get('result', {}).get('list', [])
        if not items:
            raise ValueError(f"bybit: no ticker data for {ticker}")
        price = items[0].get('ask1Price') or items[0].get('lastPrice') or items[0].get('bid1Price')
        if not price:
            raise ValueError(f"bybit: no price for {ticker}, data: {items[0]}")
        return float(price)

    def _balance_sync(self) -> float:
        # Bybit UNIFIED — берём totalEquity всего кошелька (все монеты в USD)
        for acc_type in ('UNIFIED', 'CONTRACT'):
            try:
                ts          = str(_ts())
                recv_window = '10000'
                params_str  = f"accountType={acc_type}"
                sign_str    = ts + self._api_key + recv_window + params_str
                sig = _sign_hmac_sha256(self._api_secret, sign_str)
                data = _req('GET', 'https://api.bybit.com/v5/account/wallet-balance',
                            headers={
                                'X-BAPI-API-KEY':     self._api_key,
                                'X-BAPI-SIGN':        sig,
                                'X-BAPI-TIMESTAMP':   ts,
                                'X-BAPI-RECV-WINDOW': recv_window,
                            },
                            params={'accountType': acc_type})
                if data.get('retCode') != 0:
                    continue
                for wallet in data.get('result', {}).get('list', []):
                    # totalEquity = суммарный баланс всего кошелька в USD
                    equity = float(wallet.get('totalEquity') or wallet.get('totalWalletBalance') or 0)
                    if equity > 0:
                        return equity
            except Exception as e:
                print(f"[bybit] {acc_type}: {e}")
        return 0.0

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDT'
            r = requests.get('https://api.bybit.com/v5/market/tickers',
                             params={'category': 'linear', 'symbol': ticker}, timeout=10)
            r.raise_for_status()
            items = r.json().get('result', {}).get('list', [])
            if items:
                return float(items[0].get('turnover24h') or 0)
            return None
        return await _run_sync(_sync)

    async def _setup_symbol(self, symbol: str):
        """Bybit: ISOLATED margin + leverage через v5 REST."""
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDT'

        def _sync():
            ts          = str(_ts())
            recv_window = '5000'

            # 1. Переключаем на ISOLATED (tradeMode=1)
            body = {'category': 'linear', 'symbol': ticker,
                    'tradeMode': 1, 'buyLeverage': str(LEVERAGE), 'sellLeverage': str(LEVERAGE)}
            import json as _json
            body_str   = _json.dumps(body, separators=(',', ':'))
            sign_str   = ts + self._api_key + recv_window + body_str
            sig = _sign_hmac_sha256(self._api_secret, sign_str)
            headers = {
                'X-BAPI-API-KEY':   self._api_key,
                'X-BAPI-SIGN':      sig,
                'X-BAPI-TIMESTAMP': ts,
                'X-BAPI-RECV-WINDOW': recv_window,
                'Content-Type': 'application/json',
            }
            try:
                r = requests.post('https://api.bybit.com/v5/position/switch-isolated',
                                  headers=headers, data=body_str, timeout=10)
                d = r.json()
                if d.get('retCode') not in (0, 110026):  # 110026 = already isolated
                    print(f"[bybit] switch-isolated: {d.get('retMsg')}")
            except Exception as e:
                print(f"[bybit] switch-isolated: {e}")

            # 2. Выставляем leverage
            ts2 = str(_ts())
            body2 = {'category': 'linear', 'symbol': ticker,
                     'buyLeverage': str(LEVERAGE), 'sellLeverage': str(LEVERAGE)}
            body2_str  = _json.dumps(body2, separators=(',', ':'))
            sign_str2  = ts2 + self._api_key + recv_window + body2_str
            sig2 = _sign_hmac_sha256(self._api_secret, sign_str2)
            headers2 = {**headers, 'X-BAPI-SIGN': sig2, 'X-BAPI-TIMESTAMP': ts2}
            try:
                r2 = requests.post('https://api.bybit.com/v5/position/set-leverage',
                                   headers=headers2, data=body2_str, timeout=10)
                d2 = r2.json()
                if d2.get('retCode') not in (0, 110043):  # 110043 = already set
                    print(f"[bybit] set-leverage: {d2.get('retMsg')}")
            except Exception as e:
                print(f"[bybit] set-leverage: {e}")

        await _run_sync(_sync)
        print(f"[bybit] {ticker}: ISOLATED, {LEVERAGE}×")

    def _open_params(self, side): return {'positionIdx': 0}

    async def close_position(self, symbol, side, qty):
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty,
            params={'reduceOnly': True, 'positionIdx': 0}
        )
        return {
            'order_id': str(order['id']),
            'price':    float(order.get('average') or 0),
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }


class MexcExchange(CcxtExchange):
    def __init__(self):
        super().__init__('mexc', {'options': {'defaultType': 'swap'}})
        keys = get_exchange_keys('mexc')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']

    def _price_sync(self, symbol: str) -> float:
        ticker = _mexc_ticker(symbol)
        r = requests.get('https://contract.mexc.com/api/v1/contract/ticker',
                         params={'symbol': ticker}, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', {})
        price = (data.get('ask1') or data.get('lastPrice') or
                 data.get('bid1') or data.get('indexPrice'))
        if price is None:
            raise ValueError(f"mexc: no price data for {ticker}, response: {data}")
        return float(price)

    def _balance_sync(self) -> float:
        ts  = str(_ts())
        sign_str = self._api_key + ts
        sig = _sign_hmac_sha256(self._api_secret, sign_str)
        data = _req('GET', 'https://contract.mexc.com/api/v1/private/account/assets',
                    headers={
                        'ApiKey':       self._api_key,
                        'Request-Time': ts,
                        'Signature':    sig,
                        'Content-Type': 'application/json',
                    })
        if not data.get('success'):
            print(f"[mexc] API error: {data.get('code')} {data.get('message')}")
            return 0.0
        for asset in (data.get('data') or []):
            if asset.get('currency') == 'USDT':
                return float(asset.get('availableBalance') or 0)
        return 0.0

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = symbol.replace('/', '_').replace(':USDT', '')
            r = requests.get(f'https://contract.mexc.com/api/v1/contract/ticker',
                             params={'symbol': ticker}, timeout=10)
            r.raise_for_status()
            data = r.json().get('data', {})
            vol   = float(data.get('volume24', 0) or 0)
            price = float(data.get('lastPrice', 0) or 0)
            return vol * price if vol and price else None
        return await _run_sync(_sync)

    async def _setup_symbol(self, symbol: str):
        """MEXC Futures: изолированная маржа + leverage через Contract API."""
        ticker = _mexc_ticker(symbol)

        def _sync():
            ts  = str(_ts())
            sig = _sign_hmac_sha256(self._api_secret, self._api_key + ts)
            headers = {
                'ApiKey':       self._api_key,
                'Request-Time': ts,
                'Signature':    sig,
                'Content-Type': 'application/json',
            }
            try:
                requests.post('https://contract.mexc.com/api/v1/private/position/change_margin_mode',
                    headers=headers, json={'symbol': ticker, 'marginMode': 1}, timeout=10)
            except Exception as e:
                print(f"[mexc] marginMode: {e}")
            try:
                requests.post('https://contract.mexc.com/api/v1/private/position/leverage',
                    headers=headers, json={'symbol': ticker, 'leverage': LEVERAGE, 'openType': 2}, timeout=10)
            except Exception as e:
                print(f"[mexc] leverage: {e}")

        await _run_sync(_sync)
        print(f"[mexc] {ticker}: ISOLATED, {LEVERAGE}×")

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        return await _run_sync(self._mexc_order_sync, symbol, side, size_usd)

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        price = await self.get_price(symbol)
        return await _run_sync(self._mexc_order_sync, symbol, side,
                               qty * price, reduce_only=True, price=price)


class BitgetExchange(CcxtExchange):
    def __init__(self):
        super().__init__('bitget', {'options': {'defaultType': 'swap'}})
        keys = get_exchange_keys('bitget')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']
        self._passphrase = keys.get('api_password', '')

    def _price_sync(self, symbol: str) -> float:
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDT'
        r = requests.get('https://api.bitget.com/api/v2/mix/market/ticker',
                         params={'symbol': ticker, 'productType': 'USDT-FUTURES'}, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', {})
        if isinstance(data, list): data = data[0] if data else {}
        price = data.get('askPrice') or data.get('lastPr') or data.get('bidPrice')
        if not price:
            raise ValueError(f"bitget: no price for {ticker}, data: {data}")
        return float(price)

    def _balance_sync(self) -> float:
        import base64
        ts       = str(_ts())
        method   = 'GET'
        path     = '/api/v2/mix/account/accounts?productType=USDT-FUTURES'
        sign_str = ts + method + path
        sig = base64.b64encode(
            hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()
        data = _req('GET', f'https://api.bitget.com{path}',
                    headers={
                        'ACCESS-KEY':        self._api_key,
                        'ACCESS-SIGN':       sig,
                        'ACCESS-TIMESTAMP':  ts,
                        'ACCESS-PASSPHRASE': self._passphrase,
                        'Content-Type':      'application/json',
                    })
        for item in (data.get('data') or []):
            if item.get('marginCoin') == 'USDT':
                return float(item.get('available') or item.get('usdtEquity') or 0)
        return 0.0

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDT'
            r = requests.get('https://api.bitget.com/api/v2/mix/market/ticker',
                             params={'symbol': ticker, 'productType': 'USDT-FUTURES'}, timeout=10)
            r.raise_for_status()
            data = r.json().get('data', {})
            if isinstance(data, list): data = data[0] if data else {}
            return float(data.get('usdtVolume') or data.get('quoteVolume') or 0) or None
        return await _run_sync(_sync)

    async def _setup_symbol(self, symbol: str):
        """Bitget: изолированная маржа + leverage через Mix API v2."""
        import base64, json as _json
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDT'

        def _sign_bitget(ts, method, path, body=''):
            msg = ts + method + path + body
            return base64.b64encode(
                hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).digest()
            ).decode()

        def _hdrs(ts, sig):
            return {
                'ACCESS-KEY':        self._api_key,
                'ACCESS-SIGN':       sig,
                'ACCESS-TIMESTAMP':  ts,
                'ACCESS-PASSPHRASE': self._passphrase,
                'Content-Type':      'application/json',
            }

        def _sync():
            # 1. Переключаем на isolated маржу
            ts   = str(_ts())
            path = '/api/v2/mix/account/set-margin-mode'
            body = _json.dumps({'symbol': ticker, 'productType': 'USDT-FUTURES', 'marginMode': 'isolated'})
            try:
                requests.post(f'https://api.bitget.com{path}',
                    headers=_hdrs(ts, _sign_bitget(ts, 'POST', path, body)),
                    data=body, timeout=10)
            except Exception as e:
                print(f"[bitget] marginMode: {e}")

            # 2. Выставляем leverage
            ts2  = str(_ts())
            path2 = '/api/v2/mix/account/set-leverage'
            body2 = _json.dumps({'symbol': ticker, 'productType': 'USDT-FUTURES',
                                 'marginCoin': 'USDT', 'leverage': str(LEVERAGE)})
            try:
                requests.post(f'https://api.bitget.com{path2}',
                    headers=_hdrs(ts2, _sign_bitget(ts2, 'POST', path2, body2)),
                    data=body2, timeout=10)
            except Exception as e:
                print(f"[bitget] leverage: {e}")

        await _run_sync(_sync)
        print(f"[bitget] {ticker}: ISOLATED, {LEVERAGE}×")

    def _fmt(self, symbol): return f"{symbol.replace('USDT','')}/USDT:USDT"


class KucoinExchange(BaseExchange):
    name = 'kucoin'

    def __init__(self):
        keys = get_exchange_keys('kucoin')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']
        self._passphrase = keys.get('api_password', '')
        self._client = ccxt.kucoinfutures({
            'apiKey':          self._api_key,
            'secret':          self._api_secret,
            'password':        self._passphrase,
            'enableRateLimit': True,
        })

    def _balance_sync(self) -> float:
        import base64
        ts      = str(_ts())
        method  = 'GET'
        path    = '/api/v1/account-overview?currency=USDT'
        sign_str = ts + method + path
        sig = base64.b64encode(
            hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()
        # Подписываем passphrase
        pp_sig = base64.b64encode(
            hmac.new(self._api_secret.encode(), self._passphrase.encode(), hashlib.sha256).digest()
        ).decode()
        data = _req('GET', f'https://api-futures.kucoin.com{path}',
                    headers={
                        'KC-API-KEY':         self._api_key,
                        'KC-API-SIGN':        sig,
                        'KC-API-TIMESTAMP':   ts,
                        'KC-API-PASSPHRASE':  pp_sig,
                        'KC-API-KEY-VERSION': '2',
                        'Content-Type':       'application/json',
                    })
        d = data.get('data', {})
        return float(d.get('availableBalance') or d.get('accountEquity') or 0)

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDTM'
            r = requests.get(f'https://api-futures.kucoin.com/api/v1/ticker',
                             params={'symbol': ticker}, timeout=10)
            r.raise_for_status()
            data = r.json().get('data', {})
            vol   = float(data.get('volumeOf24h', 0) or 0)
            price = float(data.get('price', 0) or 0)
            return vol * price if vol and price else None
        return await _run_sync(_sync)

    async def get_price(self, symbol: str) -> float:
        return await _run_sync(self._price_sync, symbol)

    def _price_sync(self, symbol: str) -> float:
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDTM'
        r = requests.get('https://api-futures.kucoin.com/api/v1/ticker',
                         params={'symbol': ticker}, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', {})
        price = data.get('bestAskPrice') or data.get('price') or data.get('bestBidPrice')
        if not price:
            raise ValueError(f"kucoin: no price for {ticker}, data: {data}")
        return float(price)

    async def get_max_position_size(self, symbol: str) -> Optional[float]:
        return None  # KuCoin — не используем ccxt для лимитов

    def _min_qty_sync(self, symbol: str) -> float:
        """Получает минимальный размер лота через публичный REST."""
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDTM'
        try:
            r = requests.get('https://api-futures.kucoin.com/api/v1/contracts/active', timeout=10)
            r.raise_for_status()
            for item in (r.json().get('data') or []):
                if item.get('symbol') == ticker:
                    return float(item.get('lotSize') or item.get('multiplier') or 1)
        except Exception:
            pass
        return 1.0

    def _place_order_sync(self, symbol: str, side: str, qty: float) -> dict:
        """Размещает рыночный ордер через KuCoin REST API."""
        import base64, json as _json, uuid
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDTM'
        ts     = str(_ts())
        method = 'POST'
        path   = '/api/v1/orders'
        body   = _json.dumps({
            'clientOid': str(uuid.uuid4()),
            'symbol':    ticker,
            'side':      'sell' if side == 'sell' else 'buy',
            'type':      'market',
            'size':      str(int(qty)),
            'leverage':  str(LEVERAGE),
        })
        sign_str = ts + method + path + body
        sig = base64.b64encode(
            hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()
        pp_sig = base64.b64encode(
            hmac.new(self._api_secret.encode(), self._passphrase.encode(), hashlib.sha256).digest()
        ).decode()
        headers = {
            'KC-API-KEY':         self._api_key,
            'KC-API-SIGN':        sig,
            'KC-API-TIMESTAMP':   ts,
            'KC-API-PASSPHRASE':  pp_sig,
            'KC-API-KEY-VERSION': '2',
            'Content-Type':       'application/json',
        }
        r = requests.post(f'https://api-futures.kucoin.com{path}',
                          headers=headers, data=body, timeout=15)
        r.raise_for_status()
        d = r.json()
        if d.get('code') not in ('200000', 200000):
            raise ValueError(f"kucoin order error: {d.get('msg')} code={d.get('code')}")
        order_id = d.get('data', {}).get('orderId', '')
        return {'order_id': order_id, 'price': 0.0, 'qty': qty, 'fee': 0.0}

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        price   = await self.get_price(symbol)
        min_qty = await _run_sync(self._min_qty_sync, symbol)
        qty     = _round(size_usd / price, min_qty)
        if qty < min_qty:
            raise ValueError(f"kucoin: qty {qty:.8f} < min {min_qty}")
        result  = await _run_sync(self._place_order_sync, symbol, side, qty)
        result['price'] = price  # приближённая цена
        return result

    async def _setup_symbol(self, symbol: str):
        """KuCoin Futures: leverage через ccxt (KuCoin не поддерживает cross/isolated переключение через API v1)."""
        import base64, json as _json
        ticker = symbol.replace('/', '').replace(':USDT', '') + 'USDTM'

        def _sync():
            ts  = str(_ts())
            method = 'POST'
            path   = '/api/v1/position/risk-limit-level/change'
            # KuCoin Futures использует риск-лимиты, не marginType
            # leverage выставляем через отдельный endpoint
            lev_path   = f'/api/v1/position/margin/auto-deposit-status'  # placeholder
            # Реальный endpoint для leverage в KuCoin Futures:
            lev_path2  = '/api/v1/position/leverage'  # POST
            body = _json.dumps({'symbol': ticker, 'leverage': str(LEVERAGE)})
            sign_str = ts + method + lev_path2 + body
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
            ).decode()
            pp_sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), self._passphrase.encode(), hashlib.sha256).digest()
            ).decode()
            headers = {
                'KC-API-KEY':         self._api_key,
                'KC-API-SIGN':        sig,
                'KC-API-TIMESTAMP':   ts,
                'KC-API-PASSPHRASE':  pp_sig,
                'KC-API-KEY-VERSION': '2',
                'Content-Type':       'application/json',
            }
            try:
                r = requests.post(f'https://api-futures.kucoin.com{lev_path2}',
                                  headers=headers, data=body, timeout=10)
                d = r.json()
                if d.get('code') not in ('200000', 200000):
                    print(f"[kucoin] leverage: {d.get('msg')}")
            except Exception as e:
                print(f"[kucoin] leverage: {e}")

        await _run_sync(_sync)
        # Также через ccxt для надёжности
        try:
            await self._client.set_leverage(LEVERAGE, self._fmt(symbol))
        except Exception as e:
            print(f"[kucoin] ccxt set_leverage: {e}")
        print(f"[kucoin] {ticker}: {LEVERAGE}×")

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params={'reduceOnly': True}
        )
        return {
            'order_id': str(order['id']),
            'price':    float(order.get('average') or order.get('price') or 0),
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }

    def _fmt(self, symbol: str) -> str:
        return f"{symbol.replace('USDT', '')}/USDT:USDT"

    async def close(self):
        await self._client.close()


class GateExchange(CcxtExchange):
    def __init__(self):
        super().__init__('gate', {'options': {'defaultType': 'future'}})
        keys = get_exchange_keys('gate')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']

    def _price_sync(self, symbol: str) -> float:
        ticker = _gate_ticker(symbol)
        r = requests.get('https://api.gateio.ws/api/v4/futures/usdt/tickers',
                         params={'contract': ticker}, timeout=10)
        r.raise_for_status()
        items = r.json()
        if not items:
            raise ValueError(f"gate: no ticker data for {ticker}")
        price = items[0].get('lowest_ask') or items[0].get('last') or items[0].get('highest_bid')
        if not price:
            raise ValueError(f"gate: no price for {ticker}, data: {items[0]}")
        return float(price)

    def _balance_sync(self) -> float:
        import hashlib as hl
        ts     = str(int(time.time()))
        method = 'GET'
        path   = '/api/v4/futures/usdt/accounts'
        body   = ''
        body_hash = hl.sha512(body.encode()).hexdigest()
        sign_str  = '\n'.join([method, path, '', body_hash, ts])
        sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha512).hexdigest()
        data = _req('GET', f'https://api.gateio.ws{path}',
                    headers={
                        'KEY':       self._api_key,
                        'SIGN':      sig,
                        'Timestamp': ts,
                        'Content-Type': 'application/json',
                    })
        return float(data.get('available') or data.get('total') or 0)

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = _gate_ticker(symbol)
            r = requests.get(f'https://api.gateio.ws/api/v4/futures/usdt/tickers',
                             params={'contract': ticker}, timeout=10)
            r.raise_for_status()
            items = r.json()
            if items:
                return float(items[0].get('volume_24h_quote') or items[0].get('volume_24h') or 0) or None
            return None
        return await _run_sync(_sync)

    async def _setup_symbol(self, symbol: str):
        """Gate.io Futures: dual_mode=false (single direction) + leverage через REST v4."""
        import hashlib as hl, json as _json
        ticker = _gate_ticker(symbol)

        def _sign_gate(method, path, query='', body=''):
            ts = str(int(time.time()))
            body_hash  = hl.sha512(body.encode()).hexdigest()
            sign_str   = '\n'.join([method, path, query, body_hash, ts])
            sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha512).hexdigest()
            return ts, sig

        def _sync():
            # 1. Выставляем leverage (Gate не разделяет isolated/cross для фьючерсов USDT —
            #    leverage просто задаётся на позицию)
            path  = f'/api/v4/futures/usdt/positions/{ticker}/leverage'
            body  = _json.dumps({'leverage': str(LEVERAGE), 'cross_leverage_limit': '0'})
            ts, sig = _sign_gate('POST', path, '', body)
            try:
                requests.post(f'https://api.gateio.ws{path}',
                    headers={
                        'KEY': self._api_key, 'SIGN': sig, 'Timestamp': ts,
                        'Content-Type': 'application/json',
                    },
                    data=body, timeout=10)
            except Exception as e:
                print(f"[gate] leverage: {e}")

        await _run_sync(_sync)
        print(f"[gate] {ticker}: {LEVERAGE}×")

    def _gate_order_sync(self, symbol: str, side: str, size_usd: float,
                         reduce_only: bool = False, price: float = None) -> dict:
        """Размещает рыночный ордер Gate через прямой REST."""
        import hashlib as hl, json as _json
        ticker = _gate_ticker(symbol)
        if price is None:
            price = self._price_sync(symbol)
        qty = max(1, int(size_usd / price))

        ts     = str(int(time.time()))
        method = 'POST'
        path   = '/api/v4/futures/usdt/orders'
        # Gate: size отрицательный = short/sell, положительный = long/buy
        # market order = price "0" + tif "ioc"
        size = -qty if side == 'sell' else qty
        body_d: dict = {
            'contract': ticker,
            'size':     size,
            'price':    '0',
            'tif':      'ioc',
            'text':     't-arb',
        }
        if reduce_only:
            body_d['reduce_only'] = True

        body      = _json.dumps(body_d)
        body_hash = hl.sha512(body.encode()).hexdigest()
        sign_str  = '\n'.join([method, path, '', body_hash, ts])
        sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha512).hexdigest()
        headers = {
            'KEY':          self._api_key,
            'SIGN':         sig,
            'Timestamp':    ts,
            'Content-Type': 'application/json',
            'Accept':       'application/json',
        }
        r = requests.post(f'https://api.gateio.ws{path}',
                          headers=headers, data=body, timeout=15)
        if not r.ok:
            raise ValueError(f"gate order {r.status_code}: {r.text}")
        d = r.json()
        if isinstance(d, dict) and d.get('label'):
            raise ValueError(f"gate order error: {d.get('label')} — {d.get('message')}")
        fill_price = float(d.get('fill_price') or price)
        filled_qty = abs(int(d.get('size', 0)) - int(d.get('left', 0))) or qty
        return {'order_id': str(d.get('id', '')), 'price': fill_price,
                'qty': filled_qty, 'fee': 0.0}

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        return await _run_sync(self._gate_order_sync, symbol, side, size_usd)

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        price = await self.get_price(symbol)
        return await _run_sync(self._gate_order_sync, symbol, side,
                               qty * price, reduce_only=True, price=price)


# ── Фабрика ───────────────────────────────────────────────────

_MAP = {
    'binance': BinanceExchange,
    'bybit':   BybitExchange,
    'bitget':  BitgetExchange,
    'kucoin':  KucoinExchange,
    'gate':    GateExchange,
    'mexc':    MexcExchange,
}


def create_exchange(name: str) -> BaseExchange:
    name = name.lower()
    if name not in _MAP:
        raise ValueError(f"Unknown exchange: {name}")
    return _MAP[name]()


async def get_all_balances(exchanges: list[str]) -> dict[str, float]:
    async def _fetch(name):
        try:
            ex = create_exchange(name)
        except ValueError as e:
            print(f"[Balance] {name}: {e}")
            return name, None
        try:
            bal = await ex.get_futures_balance()
            return name, bal
        except Exception as e:
            print(f"[Balance] {name}: {e}")
            return name, None
        finally:
            await ex.close()

    results = await asyncio.gather(*[_fetch(n) for n in exchanges])
    return {n: b for n, b in results if b is not None}


# ── Helpers ───────────────────────────────────────────────────

def _round(qty: float, step: float) -> float:
    if step <= 0: return qty
    p = max(0, -int(math.floor(math.log10(step))))
    return round(round(qty / step) * step, p)