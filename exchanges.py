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

from settings import get_exchange_keys, LEVERAGE, PROXY

# Один executor для всех синхронных вызовов баланса
_executor = ThreadPoolExecutor(max_workers=10)

# Прокси для запросов (если задан в .env)
_PROXIES = {'http': PROXY, 'https': PROXY} if PROXY else None

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

    async def get_funding_rate(self, symbol: str) -> Optional[dict]:
        """Возвращает текущий фандинг и время следующего начисления.
        {'rate': float, 'next_time': int (unix timestamp), 'interval_hours': int}
        """
        return None

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        """Возвращает открытую позицию на бирже или None если нет.
        {'size': float, 'side': 'long'|'short', 'entry_price': float,
         'unrealized_pnl': float, 'leverage': int}
        """
        return None

    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Возвращает реализованный PnL последней закрытой позиции с биржи.
        Это точные данные с биржи — использовать для итогового расчёта прибыли.
        """
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
    from settings import PROXY
    proxies = {'http': PROXY, 'https': PROXY} if PROXY else None
    r = requests.request(method, url, headers=headers or {}, params=params,
                         json=json, timeout=15, proxies=proxies)
    r.raise_for_status()
    return r.json()


def _req_raw(method: str, url: str, headers: dict = None, params: dict = None,
             data: str = None, json_body: dict = None, use_proxy: bool = False) -> requests.Response:
    """Синхронный HTTP запрос, возвращает Response объект."""
    from settings import PROXY
    proxies = {'http': PROXY, 'https': PROXY} if (PROXY and use_proxy) else None
    return requests.request(method, url, headers=headers or {}, params=params,
                            data=data, json=json_body, timeout=15, proxies=proxies)

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


def _bitget_ticker(symbol: str) -> str:
    """Конвертирует символ в формат Bitget: BASUSDT или BAS/USDT:USDT → BASUSDT (без подчёркивания)"""
    s = symbol.replace('/', '').replace(':USDT', '')
    s = _re_ticker.sub(r'USDT$', '', s)
    return s + 'USDT'


def _bybit_ticker(symbol: str) -> str:
    """Конвертирует символ в формат Bybit: BASUSDT или BAS/USDT:USDT → BASUSDT"""
    s = symbol.replace('/', '').replace(':USDT', '')
    s = _re_ticker.sub(r'USDT$', '', s)
    return s + 'USDT'


def _kucoin_ticker(symbol: str) -> str:
    """Конвертирует символ в формат KuCoin Futures: BASUSDT или BAS/USDT:USDT → BASUSDTM"""
    s = symbol.replace('/', '').replace(':USDT', '')
    s = _re_ticker.sub(r'USDT$', '', s)
    return s + 'USDTM'


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
        await self._setup_symbol(symbol)
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params=self._open_params(side)
        )
        fill_price = float(order.get('average') or order.get('price') or 0)
        fill_qty   = float(order.get('filled') or 0)
        return {
            'order_id': str(order['id']),
            'price':    fill_price if fill_price > 0 else price,
            'qty':      fill_qty   if fill_qty   > 0 else qty,
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        price = await self.get_price(symbol)
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params={'reduceOnly': True}
        )
        fill_price = float(order.get('average') or order.get('price') or 0)
        return {
            'order_id': str(order['id']),
            'price':    fill_price if fill_price > 0 else price,
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

    async def get_contract_info(self, symbol: str):
        """Отримуємо дані про мінімальний крок лота та множник контракту Binance."""
        ticker = symbol.replace('/', '').replace(':USDT', '')

        def _sync_info():
            r = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo', timeout=10)
            r.raise_for_status()
            data = r.json()
            for s in data.get('symbols', []):
                if s['symbol'] == ticker:
                    step = 0.001
                    min_qty = 0.001
                    for f in s.get('filters', []):
                        if f['filterType'] == 'LOT_SIZE':
                            step = float(f['stepSize'])
                            min_qty = float(f['minQty'])
                    return {
                        'step': step,
                        'multiplier': 1.0,
                        'min_qty': min_qty,
                    }
            return {'step': 0.001, 'multiplier': 1.0, 'min_qty': 0.001}

        return await _run_sync(_sync_info)

    def _binance_order_sync(self, symbol: str, side: str, size_usd: float,
                            reduce_only: bool = False, price: float = None) -> dict:
        """Размещает рыночный ордер Binance Futures через прямой REST."""
        ticker = symbol.replace('/', '').replace(':USDT', '')
        if price is None:
            price = self._price_sync(symbol)
        # Получаем минимальный шаг количества
        ts  = _ts()
        msg = f"symbol={ticker}&timestamp={ts}&recvWindow=5000"
        sig = _sign_hmac_sha256(self._api_secret, msg)
        try:
            ei = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo', timeout=10)
            symbols = ei.json().get('symbols', [])
            step = 0.001
            for s in symbols:
                if s['symbol'] == ticker:
                    for f in s.get('filters', []):
                        if f['filterType'] == 'LOT_SIZE':
                            step = float(f['stepSize'])
            qty = _round(size_usd / price, step)
        except Exception:
            qty = round(size_usd / price, 3)

        ts2  = _ts()
        params = {
            'symbol':     ticker,
            'side':       'SELL' if side == 'sell' else 'BUY',
            'type':       'MARKET',
            'quantity':   qty,
            'timestamp':  ts2,
            'recvWindow': 5000,
        }
        if reduce_only:
            params['reduceOnly'] = 'true'
        qs  = '&'.join(f"{k}={v}" for k, v in params.items())
        sig = _sign_hmac_sha256(self._api_secret, qs)
        params['signature'] = sig
        r = requests.post('https://fapi.binance.com/fapi/v1/order',
                          headers={'X-MBX-APIKEY': self._api_key},
                          params=params, timeout=15)
        r.raise_for_status()
        d = r.json()
        if 'code' in d and d['code'] < 0:
            raise ValueError(f"binance order error: {d.get('msg')} code={d['code']}")

        order_id = str(d.get('orderId', ''))

        # Binance Futures возвращает executedQty="0" и avgPrice="0" сразу после размещения
        # Запрашиваем статус ордера через 300мс
        import time as _time
        _time.sleep(0.3)
        ts3 = _ts()
        qs3 = f"symbol={ticker}&orderId={order_id}&timestamp={ts3}&recvWindow=5000"
        sig3 = _sign_hmac_sha256(self._api_secret, qs3)
        try:
            r2 = requests.get('https://fapi.binance.com/fapi/v1/order',
                              headers={'X-MBX-APIKEY': self._api_key},
                              params={'symbol': ticker, 'orderId': order_id,
                                      'timestamp': ts3, 'recvWindow': 5000,
                                      'signature': sig3}, timeout=10)
            d2 = r2.json()
            fill_price = float(d2.get('avgPrice') or 0)
            exec_qty   = float(d2.get('executedQty') or 0)
        except Exception:
            fill_price = 0
            exec_qty   = 0

        if fill_price == 0:
            fill_price = price
        if exec_qty == 0:
            exec_qty = qty

        return {
            'order_id': order_id,
            'price':    fill_price,
            'qty':      exec_qty,
            'fee':      0.0,
        }

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        return await _run_sync(self._binance_order_sync, symbol, side, size_usd)

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        """Закрывает позицию. qty — количество монет (не USD)."""
        def _close_sync():
            ticker = symbol.replace('/', '').replace(':USDT', '')
            # Получаем step size для правильного округления
            try:
                ei = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo', timeout=10)
                step = 0.001
                for s in ei.json().get('symbols', []):
                    if s['symbol'] == ticker:
                        for f in s.get('filters', []):
                            if f['filterType'] == 'LOT_SIZE':
                                step = float(f['stepSize'])
                qty_rounded = _round(qty, step)
            except Exception:
                qty_rounded = round(qty, 3)

            if qty_rounded <= 0:
                raise ValueError(f"binance close: qty={qty_rounded} <= 0")

            ts  = _ts()
            params = {
                'symbol':      ticker,
                'side':        'BUY' if side == 'buy' else 'SELL',
                'type':        'MARKET',
                'quantity':    qty_rounded,
                'reduceOnly':  'true',
                'timestamp':   ts,
                'recvWindow':  5000,
            }
            qs  = '&'.join(f"{k}={v}" for k, v in params.items())
            sig = _sign_hmac_sha256(self._api_secret, qs)
            params['signature'] = sig
            r = requests.post('https://fapi.binance.com/fapi/v1/order',
                              headers={'X-MBX-APIKEY': self._api_key},
                              params=params, timeout=15)
            price_now = self._price_sync(symbol)
            r.raise_for_status()
            d = r.json()
            if 'code' in d and d['code'] < 0:
                raise ValueError(f"binance close error: {d.get('msg')}")
            fill_price = float(d.get('avgPrice') or d.get('price') or 0)
            return {
                'order_id': str(d.get('orderId', '')),
                'price':    fill_price if fill_price > 0 else price_now,
                'fee':      0.0,
            }
        return await _run_sync(_close_sync)

    def _open_params(self, side): return {'type': 'MARKET'}

    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Получает реализованный PnL закрытой позиции с Binance Futures."""
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '')
            ts = _ts()
            qs = f"symbol={ticker}&limit=10&timestamp={ts}&recvWindow=5000"
            sig = _sign_hmac_sha256(self._api_secret, qs)
            r = requests.get('https://fapi.binance.com/fapi/v1/userTrades',
                             headers={'X-MBX-APIKEY': self._api_key},
                             params={'symbol': ticker, 'limit': 10, 'timestamp': ts,
                                     'recvWindow': 5000, 'signature': sig}, timeout=10)
            r.raise_for_status()
            trades = r.json()
            if not trades: return None
            return sum(float(t.get('realizedPnl', 0)) for t in trades)
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def get_funding_rate(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '')
            r = requests.get('https://fapi.binance.com/fapi/v1/premiumIndex',
                             params={'symbol': ticker}, timeout=10)
            r.raise_for_status()
            d = r.json()
            rate = float(d.get('lastFundingRate') or 0)
            next_ts = int(d.get('nextFundingTime', 0)) // 1000
            return {'rate': rate, 'next_time': next_ts, 'interval_hours': 8}
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = symbol.replace('/', '').replace(':USDT', '')
            ts = _ts()
            qs = f"symbol={ticker}&timestamp={ts}&recvWindow=5000"
            sig = _sign_hmac_sha256(self._api_secret, qs)
            r = requests.get('https://fapi.binance.com/fapi/v2/positionRisk',
                             headers={'X-MBX-APIKEY': self._api_key},
                             params={'symbol': ticker, 'timestamp': ts,
                                     'recvWindow': 5000, 'signature': sig}, timeout=10)
            r.raise_for_status()
            for p in r.json():
                amt = float(p.get('positionAmt', 0))
                if amt != 0:
                    return {
                        'size': abs(amt),
                        'side': 'long' if amt > 0 else 'short',
                        'entry_price': float(p.get('entryPrice', 0)),
                        'unrealized_pnl': float(p.get('unRealizedProfit', 0)),
                        'leverage': int(float(p.get('leverage', LEVERAGE))),
                    }
            return None
        try:
            return await _run_sync(_sync)
        except Exception:
            return None


class BybitExchange(BaseExchange):
    name = 'bybit'

    def __init__(self):
        keys = get_exchange_keys('bybit')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']

    async def get_price(self, symbol: str) -> float:
        return await _run_sync(self._price_sync, symbol)

    async def get_max_position_size(self, symbol: str) -> Optional[float]:
        return None

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = _bybit_ticker(symbol)
            r = requests.get('https://api.bybit.com/v5/market/tickers',
                             params={'category': 'linear', 'symbol': ticker}, timeout=10)
            r.raise_for_status()
            items = r.json().get('result', {}).get('list', [])
            return float(items[0].get('turnover24h') or 0) if items else None
        return await _run_sync(_sync)

    async def close(self):
        pass  # нет ccxt клиента — закрывать нечего

    def _price_sync(self, symbol: str) -> float:
        ticker = _bybit_ticker(symbol)
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

    async def _setup_symbol(self, symbol: str):
        """Bybit: ISOLATED margin + leverage через v5 REST."""
        ticker = _bybit_ticker(symbol)

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

    def _bybit_order_sync(self, symbol: str, side: str, size_usd: float,
                          reduce_only: bool = False, price: float = None) -> dict:
        """Прямой REST ордер Bybit Futures v5 — улучшенная математика и форматирование."""
        import json as _json
        import requests

        # 1. Получаем актуальный тикер (PTBUSDT)
        ticker = symbol.replace('/', '').replace('_', '')

        # 2. Получаем рыночные данные (Lot Size)
        try:
            r = requests.get('https://api.bybit.com/v5/market/instruments-info',
                             params={'category': 'linear', 'symbol': ticker}, timeout=10)
            res = r.json()
            instr = res.get('result', {}).get('list', [])[0]
            lot_filter = instr.get('lotSizeFilter', {})

            qty_step_str = lot_filter.get('qtyStep', '0.01')
            step = float(qty_step_str)

            # Определяем точность (количество знаков после запятой) из строки qtyStep
            # Это предотвращает передачу лишних нулей, которые вызывают ошибку 10001
            if '.' in qty_step_str:
                precision = len(qty_step_str.split('.')[-1])
            else:
                precision = 0
        except Exception as e:
            print(f"[Bybit] Warning: ошибка получения инструментов {e}")
            step = 0.01
            precision = 2

        # 3. Определяем цену, если не передана
        if price is None:
            try:
                # Рекомендуется брать цену из тикера для точности
                rt = requests.get('https://api.bybit.com/v5/market/tickers',
                                  params={'category': 'linear', 'symbol': ticker})
                price = float(rt.json()['result']['list'][0]['lastPrice'])
            except:
                price = self._price_sync(symbol)

        # 4. РАССЧИТЫВАЕМ QTY
        # Математически верное округление вниз до ближайшего шага
        raw_qty = size_usd / price
        qty = (raw_qty // step) * step

        # ФОРМАТИРОВАНИЕ: Превращаем в строку с ПРАВИЛЬНОЙ точностью
        # Это КРИТИЧЕСКИЙ момент для исправления ошибки 10001
        qty_str = format(qty, f'.{precision}f')

        if float(qty_str) <= 0:
            raise ValueError(f"bybit: рассчитанный qty={qty_str} слишком мал для {ticker}")

        # 5. ПОДГОТОВКА ЗАПРОСА
        ts = str(int(time.time() * 1000))
        recv_window = '5000'

        body = {
            'category': 'linear',
            'symbol': ticker,
            'side': 'Sell' if side.lower() == 'sell' else 'Buy',
            'orderType': 'Market',
            'qty': qty_str,
            'timeInForce': 'IOC',
        }

        if reduce_only:
            body['reduceOnly'] = True

        # Важно: separators гарантируют отсутствие пробелов в JSON
        body_str = _json.dumps(body, separators=(',', ':'))

        # 6. ПОДПИСЬ (Bybit v5: timestamp + api_key + recv_window + body)
        sign_str = ts + self._api_key + recv_window + body_str
        sig = _sign_hmac_sha256(self._api_secret, sign_str)

        headers = {
            'X-BAPI-API-KEY': self._api_key,
            'X-BAPI-SIGN': sig,
            'X-BAPI-TIMESTAMP': ts,
            'X-BAPI-RECV-WINDOW': recv_window,
            'Content-Type': 'application/json',
        }

        # 7. ОТПРАВКА
        r = requests.post('https://api.bybit.com/v5/order/create',
                          headers=headers, data=body_str, timeout=15)

        d = r.json()
        if d.get('retCode') != 0:
            # Логируем отправленный qty для отладки
            raise ValueError(f"bybit order error: {d.get('retMsg')} (code={d.get('retCode')}, qty={qty_str})")

        return {
            'order_id': d.get('result', {}).get('orderId', ''),
            'price': price,
            'qty': float(qty_str),
            'fee': 0.0
        }

    def _open_params(self, side): return {'positionIdx': 0}

    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Получает реализованный PnL закрытой позиции с Bybit."""
        def _sync():
            ticker = _bybit_ticker(symbol)
            ts = str(_ts())
            recv = '5000'
            qs = f"category=linear&symbol={ticker}&limit=10"
            sign_str = ts + self._api_key + recv + qs
            sig = _sign_hmac_sha256(self._api_secret, sign_str)
            r = requests.get('https://api.bybit.com/v5/position/closed-pnl',
                             headers={'X-BAPI-API-KEY': self._api_key,
                                      'X-BAPI-SIGN': sig,
                                      'X-BAPI-TIMESTAMP': ts,
                                      'X-BAPI-RECV-WINDOW': recv},
                             params={'category': 'linear', 'symbol': ticker, 'limit': 10},
                             timeout=10)
            r.raise_for_status()
            items = r.json().get('result', {}).get('list', [])
            if not items: return None
            # Берём самую последнюю закрытую позицию
            return float(items[0].get('closedPnl', 0))
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def get_funding_rate(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = _bybit_ticker(symbol)
            r = requests.get('https://api.bybit.com/v5/market/tickers',
                             params={'category': 'linear', 'symbol': ticker}, timeout=10)
            r.raise_for_status()
            items = r.json().get('result', {}).get('list', [])
            if not items: return None
            d = items[0]
            rate    = float(d.get('fundingRate') or 0)
            next_ts = int(d.get('nextFundingTime', 0)) // 1000
            return {'rate': rate, 'next_time': next_ts, 'interval_hours': 8}
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = _bybit_ticker(symbol)
            ts = str(_ts())
            recv = '5000'
            qs = f"category=linear&symbol={ticker}"
            sign_str = ts + self._api_key + recv + qs
            sig = _sign_hmac_sha256(self._api_secret, sign_str)
            r = requests.get('https://api.bybit.com/v5/position/list',
                             headers={'X-BAPI-API-KEY': self._api_key,
                                      'X-BAPI-SIGN': sig,
                                      'X-BAPI-TIMESTAMP': ts,
                                      'X-BAPI-RECV-WINDOW': recv},
                             params={'category': 'linear', 'symbol': ticker}, timeout=10)
            r.raise_for_status()
            items = r.json().get('result', {}).get('list', [])
            for p in items:
                size = float(p.get('size', 0))
                if size > 0:
                    side = p.get('side', '').lower()
                    return {
                        'size': size,
                        'side': side,
                        'entry_price': float(p.get('avgPrice', 0)),
                        'unrealized_pnl': float(p.get('unrealisedPnl', 0)),
                        'leverage': int(float(p.get('leverage', LEVERAGE))),
                    }
            return None
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        return await _run_sync(self._bybit_order_sync, symbol, side, size_usd)

    async def close_position(self, symbol, side, qty):
        price = await self.get_price(symbol)
        result = await _run_sync(self._bybit_order_sync, symbol, side,
                                 qty * price, reduce_only=True, price=price)
        result['price'] = price
        return result

    async def get_contract_info(self, symbol: str):
        """Отримуємо дані про мінімальний крок лота та множник контракту Bybit."""
        ticker = _bybit_ticker(symbol)

        def _sync_info():
            r = requests.get('https://api.bybit.com/v5/market/instruments-info',
                             params={'category': 'linear', 'symbol': ticker}, timeout=10)
            r.raise_for_status()
            items = r.json().get('result', {}).get('list', [])
            if not items:
                return {'step': 0.001, 'multiplier': 1.0, 'min_qty': 0.001}
            d = items[0]
            lot_filter = d.get('lotSizeFilter', {})
            step = float(lot_filter.get('qtyStep', 0.001))
            min_qty = float(lot_filter.get('minOrderQty', 0.001))
            return {
                'step': step,
                'multiplier': 1.0,
                'min_qty': min_qty,
            }

        return await _run_sync(_sync_info)


class MexcExchange(CcxtExchange):
    def __init__(self):
        super().__init__('mexc', {'options': {'defaultType': 'swap'}})
        keys = get_exchange_keys('mexc')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']
        self._user_id    = keys.get('user_id', '')

    def _mexc_sign(self, ts: str, body_str: str = '') -> str:
        """MEXC Contract API подпись: HMAC-SHA256(secret, apiKey + ts)
        userId только для MetaScalp UI, не входит в подпись."""
        return _sign_hmac_sha256(self._api_secret, self._api_key + ts)

    def _mexc_headers(self, ts: str, body_str: str = '') -> dict:
        return {
            'ApiKey':       self._api_key,
            'Request-Time': ts,
            'Signature':    self._mexc_sign(ts),
            'Content-Type': 'application/json',
        }

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
        ts   = str(_ts())
        from settings import PROXY
        proxies = {'http': PROXY, 'https': PROXY} if PROXY else None
        try:
            r = requests.get('https://contract.mexc.com/api/v1/private/account/assets',
                             headers=self._mexc_headers(ts), timeout=15, proxies=proxies)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[mexc] balance error: {e}")
            return 0.0
        if not data.get('success'):
            print(f"[mexc] API error: {data.get('code')} {data.get('message')}")
            return 0.0
        for asset in (data.get('data') or []):
            if asset.get('currency') == 'USDT':
                return float(asset.get('availableBalance') or 0)
        return 0.0

    def _mexc_post(self, path: str, body_str: str) -> dict:
        """POST запрос к MEXC Contract API с прокси если задан."""
        from settings import PROXY
        proxies = {'http': PROXY, 'https': PROXY} if PROXY else None
        ts = str(_ts())
        r  = requests.post(f'https://contract.mexc.com{path}',
                           headers=self._mexc_headers(ts, body_str),
                           data=body_str, timeout=15, proxies=proxies)
        if not r.ok:
            print(f"[mexc] {path} failed {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
        return r.json()

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        def _sync():
            ticker = _mexc_ticker(symbol)
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
        import json as _json
        ticker = _mexc_ticker(symbol)

        def _sync():
            try:
                self._mexc_post('/api/v1/private/position/change_margin_mode',
                    _json.dumps({'symbol': ticker, 'marginMode': 1}, separators=(',', ':')))
            except Exception as e:
                print(f"[mexc] marginMode: {e}")
            try:
                self._mexc_post('/api/v1/private/position/leverage',
                    _json.dumps({'symbol': ticker, 'leverage': LEVERAGE, 'openType': 2}, separators=(',', ':')))
            except Exception as e:
                print(f"[mexc] leverage: {e}")

        await _run_sync(_sync)
        print(f"[mexc] {ticker}: ISOLATED, {LEVERAGE}×")

    def _mexc_order_sync(self, symbol: str, side: str, size_usd: float,
                         reduce_only: bool = False, price: float = None) -> dict:
        """Размещает рыночный ордер MEXC через Contract API."""
        import json as _json
        ticker = _mexc_ticker(symbol)
        if price is None:
            price = self._price_sync(symbol)
        qty = max(1, int(size_usd / price))

        if reduce_only:
            order_side = 4 if side == 'sell' else 2
        else:
            order_side = 3 if side == 'sell' else 1

        body = {'symbol': ticker, 'price': price, 'vol': qty,
                'side': order_side, 'type': 5, 'openType': 2, 'leverage': LEVERAGE}
        body_str = _json.dumps(body, separators=(',', ':'))
        d = self._mexc_post('/api/v1/private/order/submit', body_str)
        if not d.get('success'):
            raise ValueError(f"mexc order error: {d.get('code')} {d.get('message')}")
        return {'order_id': str(d.get('data', '')), 'price': price, 'qty': float(qty), 'fee': 0.0}

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        return await _run_sync(self._mexc_order_sync, symbol, side, size_usd)

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        def _close_sync():
            import json as _json
            ticker   = _mexc_ticker(symbol)
            qty_int  = max(1, int(qty))
            order_side = 4 if side == 'sell' else 2
            body     = {'symbol': ticker, 'vol': qty_int, 'side': order_side, 'type': 5, 'openType': 2}
            body_str = _json.dumps(body, separators=(',', ':'))
            d = self._mexc_post('/api/v1/private/order/submit', body_str)
            if not d.get('success'):
                raise ValueError(f"mexc close error: {d.get('code')} {d.get('message')}")
            return {'order_id': str(d.get('data', '')), 'price': 0.0, 'fee': 0.0}
        return await _run_sync(_close_sync)

    async def get_contract_info(self, symbol: str):
        """Отримуємо дані про мінімальний крок лота та множник контракту MEXC."""
        ticker = _mexc_ticker(symbol)

        def _sync_info():
            r = requests.get('https://contract.mexc.com/api/v1/contract/detail',
                             params={'symbol': ticker}, timeout=10)
            r.raise_for_status()
            data = r.json().get('data', {})
            if not data:
                return {'step': 1.0, 'multiplier': 1.0, 'min_qty': 1.0}
            return {
                'step': 1.0,
                'multiplier': float(data.get('multiplier', 1) or 1),
                'min_qty': 1.0,
            }

        return await _run_sync(_sync_info)

    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Получает реализованный PnL закрытой позиции с MEXC Futures."""
        import json as _json
        ticker = _mexc_ticker(symbol)

        def _sync():
            ts = str(_ts())
            path = '/api/v1/private/position/list/history_positions'
            params = {'symbol': ticker, 'page_num': 1, 'page_size': 10}
            qs = '&'.join(f'{k}={v}' for k, v in params.items())
            r = requests.get(f'https://contract.mexc.com{path}?{qs}',
                             headers=self._mexc_headers(ts), timeout=10)
            if not r.ok:
                return None
            data = r.json()
            if not data.get('success') or not data.get('data', {}).get('resultList'):
                return None
            total_pnl = 0.0
            for pos in data['data']['resultList']:
                if pos.get('symbol') == ticker:
                    pnl = float(pos.get('closeProfitLoss', 0) or 0)
                    total_pnl += pnl
            return total_pnl if total_pnl != 0 else None

        try:
            return await _run_sync(_sync)
        except Exception:
            return None


class BitgetExchange(BaseExchange):
    name = 'bitget'

    def __init__(self):
        keys = get_exchange_keys('bitget')
        self._api_key = keys['api_key']
        self._api_secret = keys['api_secret']
        self._passphrase = keys.get('api_password', '')

    async def get_price(self, symbol: str) -> float:
        return await _run_sync(self._price_sync, symbol)

    def _price_sync(self, symbol: str) -> float:
        ticker = _bitget_ticker(symbol)
        r = requests.get('https://api.bitget.com/api/v2/mix/market/ticker',
                         params={'symbol': ticker, 'productType': 'USDT-FUTURES'}, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', {})
        if isinstance(data, list): data = data[0] if data else {}
        price = data.get('askPrice') or data.get('lastPr') or data.get('bidPrice')
        return float(price)

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    def _balance_sync(self) -> float:
        import base64
        ts = str(_ts())
        path = '/api/v2/mix/account/accounts?productType=USDT-FUTURES'
        sign_str = ts + 'GET' + path
        sig = base64.b64encode(hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()).decode()
        data = _req('GET', f'https://api.bitget.com{path}',
                    headers={'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig, 'ACCESS-TIMESTAMP': ts,
                             'ACCESS-PASSPHRASE': self._passphrase, 'Content-Type': 'application/json'})
        for item in (data.get('data') or []):
            if item.get('marginCoin') == 'USDT':
                return float(item.get('available') or item.get('usdtEquity') or 0)
        return 0.0

    async def _setup_symbol(self, symbol: str):
        """Bitget: Принудительная установка Изолированной маржи, Плеча и Режима Хеджирования."""
        import base64, json as _json
        ticker = _bitget_ticker(symbol)

        def _do_bitget_post(path, body_dict):
            ts = str(_ts())
            body_str = _json.dumps(body_dict, separators=(',', ':'))
            sign_str = ts + 'POST' + path + body_str
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()).decode()
            headers = {'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig, 'ACCESS-TIMESTAMP': ts,
                       'ACCESS-PASSPHRASE': self._passphrase, 'Content-Type': 'application/json'}
            r = requests.post(f'https://api.bitget.com{path}', headers=headers, data=body_str, timeout=10)
            if not r.ok:
                raise ValueError(f"bitget POST {path}: {r.status_code} {r.text}")
            res = r.json()
            if res.get('code') != '00000':
                raise ValueError(f"bitget POST {path}: {res.get('msg')} ({res.get('code')})")
            return res

        def _do_bitget_get(path, params=None):
            ts = str(_ts())
            qs = '&'.join(f'{k}={v}' for k, v in (params or {}).items())
            full_path = f'{path}?{qs}' if qs else path
            sign_str = ts + 'GET' + full_path
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()).decode()
            headers = {'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig, 'ACCESS-TIMESTAMP': ts,
                       'ACCESS-PASSPHRASE': self._passphrase, 'Content-Type': 'application/json'}
            r = requests.get(f'https://api.bitget.com{full_path}', headers=headers, timeout=10)
            if not r.ok:
                raise ValueError(f"bitget GET {path}: {r.status_code} {r.text}")
            return r.json()

        def _sync():
            # 1. Переключаем на Режим Хеджирования (Hedge Mode)
            try:
                _do_bitget_post('/api/v2/mix/account/set-position-mode',
                                {'productType': 'USDT-FUTURES', 'posMode': 'hedge_mode'})
            except Exception as e:
                print(f"[bitget] set-position-mode: {e}")

            # 2. Устанавливаем изолированную маржу
            try:
                _do_bitget_post('/api/v2/mix/account/set-margin-mode',
                                {'symbol': ticker, 'productType': 'USDT-FUTURES', 'marginMode': 'isolated'})
            except Exception as e:
                print(f"[bitget] set-margin-mode: {e}")

            # 3. Устанавливаем плечо — ОБЯЗАТЕЛЬНО с проверкой
            _do_bitget_post('/api/v2/mix/account/set-leverage',
                            {'symbol': ticker, 'productType': 'USDT-FUTURES', 'marginCoin': 'USDT',
                             'leverage': str(LEVERAGE)})

            # 4. Верификация: читаем текущее плечо из позиции
            import time as _time
            _time.sleep(0.3)
            try:
                pos_data = _do_bitget_get('/api/v2/mix/position/single-position',
                                          {'symbol': ticker, 'productType': 'USDT-FUTURES', 'marginCoin': 'USDT'})
                positions = pos_data.get('data', [])
                if positions:
                    actual_leverage = int(float(positions[0].get('leverage', 0)))
                    if actual_leverage != LEVERAGE:
                        raise ValueError(
                            f"bitget leverage mismatch: wanted {LEVERAGE}x, got {actual_leverage}x for {ticker}. "
                            f"Trade BLOCKED — check exchange settings manually."
                        )
                    print(f"[bitget] {ticker}: leverage verified ✅ {LEVERAGE}×")
                else:
                    print(f"[bitget] {ticker}: no open position yet, leverage set to {LEVERAGE}×")
            except ValueError:
                raise
            except Exception as e:
                print(f"[bitget] leverage verification skipped: {e}")

        await _run_sync(_sync)

    def _bitget_order_sync(self, symbol: str, side: str, size_usd: float, reduce_only: bool = False,
                           price: float = None) -> dict:
        import base64, json as _json
        ticker = _bitget_ticker(symbol)
        if price is None: price = self._price_sync(symbol)

        # Расчет qty
        qty = round(size_usd / price, 1)  # Упрощенное округление

        ts = str(_ts())
        path = '/api/v2/mix/order/place-order'

        # Логика для Hedge Mode
        if reduce_only:
            trade_side = 'close'
            pos_side = 'long' if side.lower() == 'sell' else 'short'
        else:
            trade_side = 'open'
            pos_side = 'long' if side.lower() == 'buy' else 'short'

        body = {
            'symbol': ticker, 'productType': 'USDT-FUTURES', 'marginCoin': 'USDT',
            'marginMode': 'isolated', 'side': side.lower(), 'orderType': 'market',
            'size': str(qty), 'tradeSide': trade_side, 'posSide': pos_side
        }

        body_str = _json.dumps(body, separators=(',', ':'))
        sign_str = ts + 'POST' + path + body_str
        sig = base64.b64encode(hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()).decode()

        r = requests.post(f'https://api.bitget.com{path}',
                          headers={'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig, 'ACCESS-TIMESTAMP': ts,
                                   'ACCESS-PASSPHRASE': self._passphrase, 'Content-Type': 'application/json'},
                          data=body_str, timeout=15)

        res = r.json()
        if res.get('code') != '00000':
            raise ValueError(f"bitget order failed: {res.get('msg')} ({res.get('code')})")

        return {'order_id': res.get('data', {}).get('orderId'), 'price': price, 'qty': qty, 'fee': 0.0}

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        await self._setup_symbol(symbol)
        return await _run_sync(self._bitget_order_sync, symbol, side, size_usd)

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        price = await self.get_price(symbol)
        return await _run_sync(self._bitget_order_sync, symbol, side, qty * price, reduce_only=True, price=price)

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        import base64
        def _sync():
            ticker = _bitget_ticker(symbol)
            ts = str(_ts())
            path = f'/api/v2/mix/position/single-position?symbol={ticker}&productType=USDT-FUTURES&marginCoin=USDT'
            sign_str = ts + 'GET' + path
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()).decode()
            r = requests.get(f'https://api.bitget.com{path}',
                             headers={'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig, 'ACCESS-TIMESTAMP': ts,
                                      'ACCESS-PASSPHRASE': self._passphrase, 'Content-Type': 'application/json'},
                             timeout=10)
            data = r.json().get('data', [])
            for p in (data if isinstance(data, list) else [data]):
                size = float(p.get('total', 0) or 0)
                if size > 0:
                    return {'size': size, 'side': p.get('holdSide'), 'entry_price': float(p.get('openPriceAvg', 0)),
                            'unrealized_pnl': float(p.get('unrealizedPL', 0)),
                            'leverage': int(float(p.get('leverage', 5)))}
            return None

        return await _run_sync(_sync)

    async def get_funding_rate(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = _bitget_ticker(symbol)
            r = requests.get('https://api.bitget.com/api/v2/mix/market/current-fund-rate',
                             params={'symbol': ticker, 'productType': 'USDT-FUTURES'}, timeout=10)
            r.raise_for_status()
            data = r.json().get('data', [])
            if not data: return None
            d = data[0] if isinstance(data, list) else data
            rate    = float(d.get('fundingRate') or 0)
            next_ts = int(d.get('nextFundingTime', 0)) // 1000
            return {'rate': rate, 'next_time': next_ts, 'interval_hours': 8}
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        import base64
        def _sync():
            ticker = _bitget_ticker(symbol)
            ts   = str(_ts())
            path = f'/api/v2/mix/position/single-position?symbol={ticker}&productType=USDT-FUTURES&marginCoin=USDT'
            msg  = ts + 'GET' + path
            sig  = base64.b64encode(
                hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).digest()
            ).decode()
            r = requests.get(f'https://api.bitget.com{path}',
                             headers={'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig,
                                      'ACCESS-TIMESTAMP': ts, 'ACCESS-PASSPHRASE': self._passphrase,
                                      'Content-Type': 'application/json'}, timeout=10)
            r.raise_for_status()
            data = r.json().get('data', [])
            for p in (data if isinstance(data, list) else [data]):
                size = float(p.get('total', 0) or 0)
                if size > 0:
                    side = 'long' if p.get('holdSide') == 'long' else 'short'
                    return {
                        'size': size,
                        'side': side,
                        'entry_price': float(p.get('openPriceAvg', 0)),
                        'unrealized_pnl': float(p.get('unrealizedPL', 0)),
                        'leverage': int(float(p.get('leverage', LEVERAGE))),
                    }
            return None
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    def _fmt(self, symbol): return f"{symbol.replace('USDT','')}/USDT:USDT"

    async def get_contract_info(self, symbol: str):
        """Отримуємо дані про мінімальний крок лота та множник контракту Bitget."""
        import base64
        def _sync_info():
            ticker = _bitget_ticker(symbol)
            ts = str(_ts())
            path = f'/api/v2/mix/market/contracts?productType=USDT-FUTURES&symbol={ticker}'
            sign_str = ts + 'GET' + path
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
            ).decode()
            r = requests.get(f'https://api.bitget.com{path}',
                             headers={'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig,
                                      'ACCESS-TIMESTAMP': ts, 'ACCESS-PASSPHRASE': self._passphrase,
                                      'Content-Type': 'application/json'}, timeout=10)
            if not r.ok:
                return {'step': 0.1, 'multiplier': 1.0, 'min_qty': 0.1}
            data = r.json().get('data', [])
            if not data:
                return {'step': 0.1, 'multiplier': 1.0, 'min_qty': 0.1}
            d = data[0] if isinstance(data, list) else data
            step = float(d.get('volumePlace', 0.1) or 0.1)
            min_qty = float(d.get('minTradeNum', 0.1) or 0.1)
            return {
                'step': step,
                'multiplier': 1.0,
                'min_qty': min_qty,
            }
        try:
            return await _run_sync(_sync_info)
        except Exception:
            return {'step': 0.1, 'multiplier': 1.0, 'min_qty': 0.1}

    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Получает реализованный PnL закрытой позиции с Bitget Futures."""
        import base64
        def _sync():
            ticker = _bitget_ticker(symbol)
            ts = str(_ts())
            path = f'/api/v2/mix/position/history-position?productType=USDT-FUTURES&symbol={ticker}'
            sign_str = ts + 'GET' + path
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
            ).decode()
            r = requests.get(f'https://api.bitget.com{path}',
                             headers={'ACCESS-KEY': self._api_key, 'ACCESS-SIGN': sig,
                                      'ACCESS-TIMESTAMP': ts, 'ACCESS-PASSPHRASE': self._passphrase,
                                      'Content-Type': 'application/json'}, timeout=10)
            if not r.ok:
                return None
            data = r.json()
            if data.get('code') != '00000' or not data.get('data', {}).get('list'):
                return None
            total_pnl = 0.0
            for pos in data['data']['list']:
                pnl = float(pos.get('netProfit', 0) or 0)
                total_pnl += pnl
            return total_pnl if total_pnl != 0 else None
        try:
            return await _run_sync(_sync)
        except Exception:
            return None


class KucoinExchange(BaseExchange):
    name = 'kucoin'

    def __init__(self):
        keys = get_exchange_keys('kucoin')
        self._api_key    = keys['api_key']
        self._api_secret = keys['api_secret']
        self._passphrase = keys.get('api_password', '')

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
            ticker = _kucoin_ticker(symbol)
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
        ticker = _kucoin_ticker(symbol)
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
        ticker = _kucoin_ticker(symbol)
        try:
            r = requests.get('https://api-futures.kucoin.com/api/v1/contracts/active', timeout=10)
            r.raise_for_status()
            for item in (r.json().get('data') or []):
                if item.get('symbol') == ticker:
                    return float(item.get('lotSize') or item.get('multiplier') or 1)
        except Exception:
            pass
        return 1.0

    def _place_order_sync(self, symbol: str, side: str, qty: float,
                          reduce_only: bool = False) -> dict:
        """Размещает рыночный ордер через KuCoin REST API."""
        import base64, json as _json, uuid
        ticker = _kucoin_ticker(symbol)
        ts     = str(_ts())
        method = 'POST'
        path   = '/api/v1/orders'
        body_d = {
            'clientOid': str(uuid.uuid4()),
            'symbol':    ticker,
            'side':      'sell' if side == 'sell' else 'buy',
            'type':      'market',
            'size':      str(int(qty)),
            'leverage':  str(LEVERAGE),
        }
        if reduce_only:
            body_d['closeOrder'] = True
        body = _json.dumps(body_d)
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
        return {'order_id': order_id, 'price': None, 'qty': qty, 'fee': 0.0}  # price заполнится в place_market_order

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
        ticker = _kucoin_ticker(symbol)

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
        print(f"[kucoin] {ticker}: {LEVERAGE}×")

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        price = await self.get_price(symbol)
        qty_int = max(1, int(qty))
        result = await _run_sync(self._place_order_sync, symbol, side, qty_int,
                                 reduce_only=True)
        result['price'] = price
        return result

    def _fmt(self, symbol: str) -> str:
        return f"{symbol.replace('USDT', '')}/USDT:USDT"

    async def close(self):
        pass  # нет ccxt клиента

    async def get_funding_rate(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = _kucoin_ticker(symbol)
            r = requests.get(f'https://api-futures.kucoin.com/api/v1/funding-rate/{ticker}/current',
                             timeout=10)
            r.raise_for_status()
            d = r.json().get('data', {})
            rate    = float(d.get('value') or 0)
            next_ts = int(d.get('timePoint', 0)) // 1000
            return {'rate': rate, 'next_time': next_ts, 'interval_hours': 8}
        try:
            return await _run_sync(_sync)
        except Exception:
            return None

    async def get_contract_info(self, symbol: str):
        """Отримуємо дані про мінімальний крок лота та множник контракту KuCoin."""
        def _sync_info():
            ticker = _kucoin_ticker(symbol)
            r = requests.get('https://api-futures.kucoin.com/api/v1/contracts/active', timeout=10)
            r.raise_for_status()
            for item in (r.json().get('data') or []):
                if item.get('symbol') == ticker:
                    step = float(item.get('lotSize', 1) or 1)
                    min_qty = step
                    return {
                        'step': step,
                        'multiplier': float(item.get('multiplier', 1) or 1),
                        'min_qty': min_qty,
                    }
            return {'step': 1.0, 'multiplier': 1.0, 'min_qty': 1.0}
        try:
            return await _run_sync(_sync_info)
        except Exception:
            return {'step': 1.0, 'multiplier': 1.0, 'min_qty': 1.0}

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        import base64
        def _sync():
            ticker = _kucoin_ticker(symbol)
            ts  = str(_ts())
            path = f'/api/v1/position?symbol={ticker}'
            sign_str = ts + 'GET' + path
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
            ).decode()
            pp_sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), self._passphrase.encode(), hashlib.sha256).digest()
            ).decode()
            r = requests.get(f'https://api-futures.kucoin.com{path}',
                             headers={'KC-API-KEY': self._api_key, 'KC-API-SIGN': sig,
                                      'KC-API-TIMESTAMP': ts, 'KC-API-PASSPHRASE': pp_sig,
                                      'KC-API-KEY-VERSION': '2'}, timeout=10)
            r.raise_for_status()
            d = r.json().get('data', {})
            size = float(d.get('currentQty', 0) or 0)
            if size == 0: return None
            return {
                'size': abs(size),
                'side': 'long' if size > 0 else 'short',
                'entry_price': float(d.get('avgEntryPrice', 0)),
                'unrealized_pnl': float(d.get('unrealisedPnl', 0)),
                'leverage': int(float(d.get('realLeverage', LEVERAGE))),
            }
        try:
            return await _run_sync(_sync)
        except Exception:
            return None


    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Получает реализованный PnL закрытой позиции с KuCoin Futures."""
        import base64, json as _json
        ticker = _kucoin_ticker(symbol)

        def _sync():
            ts = str(_ts())
            method = 'GET'
            path = f'/api/v1/fill-data?symbol={ticker}&startAt={int(_ts()) - 86400000}&endAt={_ts()}'
            sign_str = ts + method + path
            sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
            ).decode()
            pp_sig = base64.b64encode(
                hmac.new(self._api_secret.encode(), self._passphrase.encode(), hashlib.sha256).digest()
            ).decode()
            r = requests.get(f'https://api-futures.kucoin.com{path}',
                             headers={'KC-API-KEY': self._api_key, 'KC-API-SIGN': sig,
                                      'KC-API-TIMESTAMP': ts, 'KC-API-PASSPHRASE': pp_sig,
                                      'KC-API-KEY-VERSION': '2'}, timeout=10)
            if not r.ok:
                return None
            data = r.json()
            if data.get('code') not in ('200000', 200000) or not data.get('data'):
                return None
            total_pnl = 0.0
            for fill in data['data']:
                pnl = float(fill.get('realisedPnl', 0) or 0)
                total_pnl += pnl
            return total_pnl if total_pnl != 0 else None

        try:
            return await _run_sync(_sync)
        except Exception:
            return None


class GateExchange(CcxtExchange):
    def __init__(self):
        super().__init__('gate', {'options': {'defaultType': 'future'}})
        keys = get_exchange_keys('gate')
        self._api_key = keys['api_key']
        self._api_secret = keys['api_secret']

    async def _setup_symbol(self, symbol: str):
        """
        КРОК 1: Налаштування маржі та плеча ПЕРЕД відкриттям угоди.
        Для Isolated Margin на Gate обов'язково передаємо cross_leverage_limit=0.
        """
        import hashlib as hl
        ticker = _gate_ticker(symbol)

        def _sync_setup():
            ts = str(int(time.time()))
            method = 'POST'
            # Ендпоінт для зміни плеча та режиму маржі
            path = f'/api/v4/futures/usdt/positions/{ticker}/leverage'
            # Для ISOLATED margin передаємо тільки leverage
            query = f'leverage={LEVERAGE}'

            # Підпис для Gate V4 POST запиту з Query String
            body_hash = hl.sha512(b'').hexdigest()
            sign_str = '\n'.join([method, path, query, body_hash, ts])
            sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hl.sha512).hexdigest()

            url = f'https://api.gateio.ws{path}?{query}'
            headers = {
                'KEY': self._api_key,
                'SIGN': sig,
                'Timestamp': ts,
                'Content-Type': 'application/json'
            }

            try:
                r = requests.post(url, headers=headers, timeout=10)
                if r.ok:
                    print(f"[gate] {ticker}: Встановлено плечо {LEVERAGE}x та Isolated Margin ✅")
                else:
                    # Якщо позиція вже налаштована, біржа може повернути попередження
                    print(f"[gate] {ticker} setup notice: {r.text}")
            except Exception as e:
                print(f"[gate] Setup error for {ticker}: {e}")

        await _run_sync(_sync_setup)

    async def get_contract_info(self, symbol: str):
        """Отримуємо дані про множник контракту (multiplier) та мінімальний крок лота."""
        ticker = _gate_ticker(symbol)

        def _sync_info():
            r = requests.get(f'https://api.gateio.ws/api/v4/futures/usdt/contracts/{ticker}', timeout=10)
            r.raise_for_status()
            d = r.json()
            return {
                'step': max(float(d.get('order_size_min', 1) or 1), 1.0),
                'multiplier': float(d.get('quanto_multiplier') or d.get('multiplier') or 1),
                'min_qty': max(float(d.get('order_size_min', 1) or 1), 1.0)
            }

        return await _run_sync(_sync_info)

    def _gate_order_sync(self, ticker: str, side: str, qty: float, price: float) -> dict:
        """КРОК 2: Відправка ринкового ордера (Size на Gate — це кількість контрактів)."""
        import hashlib as hl
        import json as _json

        ts = str(int(time.time()))
        method = 'POST'
        path = '/api/v4/futures/usdt/orders'

        # На Gate 'size' — це ціле число контрактів. Позитивне — Buy, негативне — Sell.
        order_size = int(qty) if side.lower() in ('buy', 'long') else -int(qty)

        body_d = {
            'contract': ticker,
            'size': order_size,
            'price': '0',  # '0' означає ринковий ордер (Market)
            'tif': 'ioc',
            'text': 't-arb'
        }

        body_json = _json.dumps(body_d)
        body_hash = hl.sha512(body_json.encode()).hexdigest()
        sign_str = '\n'.join([method, path, '', body_hash, ts])
        sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hl.sha512).hexdigest()

        headers = {
            'KEY': self._api_key,
            'SIGN': sig,
            'Timestamp': ts,
            'Content-Type': 'application/json'
        }

        r = requests.post(f'https://api.gateio.ws{path}', headers=headers, data=body_json, timeout=15)
        if not r.ok:
            raise ValueError(f"Gate Order Error: {r.text}")

        res = r.json()
        return {
            'order_id': str(res.get('id', '')),
            'price': float(res.get('fill_price') or price),
            'qty': abs(int(res.get('size', 0))),
            'fee': 0.0
        }

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        """Повний цикл відкриття позиції на Gate."""
        # 1. Налаштовуємо плече та Isolated Margin
        await self._setup_symbol(symbol)

        # 2. Невелика пауза для стабілізації налаштувань на сервері
        await asyncio.sleep(0.2)

        # 3. Отримуємо актуальну ціну та параметри контракту
        price = await self.get_price(symbol)
        info = await self.get_contract_info(symbol)

        # 4. Рахуємо кількість контрактів (Contracts)
        qty_raw = size_usd / (price * info['multiplier'])
        qty = (qty_raw // info['step']) * info['step']

        if qty <= 0:
            raise ValueError(f"Gate: Розрахована кількість лотів = 0 для {symbol}")

        # 5. Виконуємо ордер
        ticker = _gate_ticker(symbol)
        return await _run_sync(self._gate_order_sync, ticker, side, qty, price)

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        """Закриття позиції (використовує існуючий qty контрактів)."""
        price = await self.get_price(symbol)
        ticker = _gate_ticker(symbol)
        # При закритті шлемо qty як ціле число контрактів
        return await _run_sync(self._gate_order_sync, ticker, side, int(qty), price)

    def _price_sync(self, symbol: str) -> float:
        ticker = _gate_ticker(symbol)
        r = requests.get('https://api.gateio.ws/api/v4/futures/usdt/tickers',
                         params={'contract': ticker}, timeout=10)
        r.raise_for_status()
        items = r.json()
        if not items:
            raise ValueError(f"gate: немає даних для {ticker}")
        price = items[0].get('lowest_ask') or items[0].get('last') or items[0].get('highest_bid')
        return float(price)

    def _balance_sync(self) -> float:
        import hashlib as hl
        ts = str(int(time.time()))
        method = 'GET'
        path = '/api/v4/futures/usdt/accounts'
        body_hash = hl.sha512(b'').hexdigest()
        sign_str = '\n'.join([method, path, '', body_hash, ts])
        sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hl.sha512).hexdigest()
        data = _req('GET', f'https://api.gateio.ws{path}',
                    headers={'KEY': self._api_key, 'SIGN': sig, 'Timestamp': ts, 'Content-Type': 'application/json'})
        return float(data.get('available') or data.get('total') or 0)

    async def get_futures_balance(self) -> float:
        return await _run_sync(self._balance_sync)

    async def get_funding_rate(self, symbol: str) -> Optional[dict]:
        def _sync():
            ticker = _gate_ticker(symbol)
            r = requests.get('https://api.gateio.ws/api/v4/futures/usdt/contracts/' + ticker, timeout=10)
            r.raise_for_status()
            d = r.json()
            return {
                'rate': float(d.get('funding_rate', 0)),
                'next_time': int(d.get('funding_next_apply', 0)),
                'interval_hours': int(d.get('funding_interval', 28800)) // 3600
            }

        try:
            return await _run_sync(_sync)
        except:
            return None

    async def get_open_position(self, symbol: str) -> Optional[dict]:
        import hashlib as hl
        def _sync():
            ticker = _gate_ticker(symbol)
            ts = str(int(time.time()))
            path = f'/api/v4/futures/usdt/positions/{ticker}'
            body_hash = hl.sha512(b'').hexdigest()
            sign_str = '\n'.join(['GET', path, '', body_hash, ts])
            sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hl.sha512).hexdigest()
            r = requests.get(f'https://api.gateio.ws{path}',
                             headers={'KEY': self._api_key, 'SIGN': sig, 'Timestamp': ts}, timeout=10)
            if r.status_code == 404: return None
            r.raise_for_status()
            d = r.json()
            size = float(d.get('size', 0))
            if size == 0: return None
            return {
                'size': abs(size),
                'side': 'long' if size > 0 else 'short',
                'entry_price': float(d.get('entry_price', 0)),
                'unrealized_pnl': float(d.get('unrealised_pnl', 0)),
                'leverage': int(float(d.get('leverage', LEVERAGE)))
            }

        try:
            return await _run_sync(_sync)
        except:
            return None

    async def get_closed_pnl(self, symbol: str) -> Optional[float]:
        """Получает реализованный PnL закрытой позиции с Gate.io Futures."""
        import hashlib as hl
        ticker = _gate_ticker(symbol)

        def _sync():
            ts = str(int(time.time()))
            method = 'GET'
            path = f'/api/v4/futures/usdt/my_trades'
            # Gate API v4: /my_trades возвращает сделки, суммируем realised PnL
            query = f'contract={ticker}&limit=20'
            body_hash = hl.sha512(b'').hexdigest()
            sign_str = '\n'.join([method, path, query, body_hash, ts])
            sig = hmac.new(self._api_secret.encode(), sign_str.encode(), hl.sha512).hexdigest()
            r = requests.get(f'https://api.gateio.ws{path}?{query}',
                             headers={'KEY': self._api_key, 'SIGN': sig, 'Timestamp': ts}, timeout=10)
            if not r.ok:
                return None
            trades = r.json()
            if not trades:
                return None
            total_pnl = 0.0
            for t in trades:
                pnl = float(t.get('pnl', 0) or 0)
                total_pnl += pnl
            return total_pnl if total_pnl != 0 else None

        try:
            return await _run_sync(_sync)
        except Exception:
            return None


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