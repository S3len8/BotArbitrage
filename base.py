"""
exchanges/base.py — адаптеры бирж с поддержкой плеча и отслеживанием балансов.

Логика размера позиции:
  balance_usd = реальный баланс на бирже (futures wallet)
  position_size = balance_usd × LEVERAGE
  final_size = min(position_size_биржа1, position_size_биржа2, max_size_из_сигнала)
"""

import asyncio
import math
import ccxt.async_support as ccxt
from abc import ABC, abstractmethod
from typing import Optional

from config.settings import get_exchange_keys, LEVERAGE


# ============================================================
# Абстрактный класс
# ============================================================

class BaseExchange(ABC):
    name: str

    @abstractmethod
    async def get_price(self, symbol: str) -> float: ...

    @abstractmethod
    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict: ...

    @abstractmethod
    async def close_position(self, symbol: str, side: str, qty: float) -> dict: ...

    @abstractmethod
    async def get_futures_balance(self) -> float:
        """Баланс futures/swap кошелька в USDT."""
        ...

    @abstractmethod
    async def get_max_position_size(self, symbol: str) -> Optional[float]:
        """
        Максимальный допустимый размер позиции в USD для данного символа.
        Берётся из лимитов биржи (notional limit).
        Возвращает None если лимит неизвестен.
        """
        ...

    async def get_position_size_usd(self) -> float:
        """
        Рассчитывает доступный размер позиции с плечом:
        balance × LEVERAGE
        """
        balance = await self.get_futures_balance()
        return balance * LEVERAGE

    async def close(self): pass


# ============================================================
# Универсальный CCXT адаптер
# ============================================================

class CcxtExchange(BaseExchange):

    def __init__(self, exchange_id: str, extra_params: dict = None):
        self.name = exchange_id
        keys = get_exchange_keys(exchange_id)

        params = {
            'apiKey':          keys.get('api_key', ''),
            'secret':          keys.get('api_secret', ''),
            'options':         {'defaultType': 'future'},
            'enableRateLimit': True,
        }
        if 'api_password' in keys:
            params['password'] = keys['api_password']
        if extra_params:
            params.update(extra_params)

        cls = getattr(ccxt, exchange_id)
        self._client = cls(params)

    async def get_price(self, symbol: str) -> float:
        ticker = await self._client.fetch_ticker(self._fmt(symbol))
        return float(ticker['ask'] or ticker['last'])

    async def get_futures_balance(self) -> float:
        balance = await self._client.fetch_balance({'type': 'future'})
        return float(balance.get('USDT', {}).get('free', 0))

    async def get_max_position_size(self, symbol: str) -> Optional[float]:
        """
        Получаем максимальный notional размер позиции из market info.
        Большинство бирж возвращают это в market['limits']['cost']['max'].
        """
        try:
            markets = await self._client.load_markets()
            market = markets.get(self._fmt(symbol), {})
            max_cost = market.get('limits', {}).get('cost', {}).get('max')
            return float(max_cost) if max_cost else None
        except Exception:
            return None

    async def place_market_order(self, symbol: str, side: str, size_usd: float) -> dict:
        price = await self.get_price(symbol)
        min_qty = await self._get_min_qty(symbol)
        qty = _round_qty(size_usd / price, min_qty)

        if qty < min_qty:
            raise ValueError(
                f"{self.name}: qty {qty:.8f} < min {min_qty} "
                f"for {symbol} (${size_usd:.2f} @ {price:.6f})"
            )

        # Устанавливаем плечо перед открытием
        await self._set_leverage(symbol)

        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params=self._order_params(side)
        )
        return {
            'order_id': str(order['id']),
            'price':    float(order.get('average') or order.get('price') or price),
            'qty':      float(order.get('filled') or qty),
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        order = await self._client.create_market_order(
            self._fmt(symbol), side, qty, params=self._close_params()
        )
        return {
            'order_id': str(order['id']),
            'price':    float(order.get('average') or order.get('price') or 0),
            'fee':      float((order.get('fee') or {}).get('cost') or 0),
        }

    async def _set_leverage(self, symbol: str):
        try:
            await self._client.set_leverage(LEVERAGE, self._fmt(symbol))
        except Exception as e:
            print(f"[{self.name}] set_leverage warning: {e}")

    def _fmt(self, symbol: str) -> str:
        if '/' in symbol:
            return symbol
        base = symbol.replace('USDT', '')
        return f"{base}/USDT:USDT"

    def _order_params(self, side: str) -> dict:
        return {}

    def _close_params(self) -> dict:
        return {'reduceOnly': True}

    async def _get_min_qty(self, symbol: str) -> float:
        markets = await self._client.load_markets()
        market = markets.get(self._fmt(symbol), {})
        return float(market.get('limits', {}).get('amount', {}).get('min', 0.001))

    async def close(self):
        await self._client.close()


# ============================================================
# Специфичные адаптеры
# ============================================================

class BybitExchange(CcxtExchange):
    def __init__(self):
        super().__init__('bybit')

    def _order_params(self, side):
        return {'positionIdx': 0}

    def _close_params(self):
        return {'reduceOnly': True, 'positionIdx': 0}

    async def get_futures_balance(self) -> float:
        b = await self._client.fetch_balance({'accountType': 'CONTRACT'})
        return float(b.get('USDT', {}).get('free', 0))


class BitgetExchange(CcxtExchange):
    def __init__(self):
        super().__init__('bitget', {'options': {'defaultType': 'swap'}})

    def _fmt(self, symbol: str) -> str:
        base = symbol.replace('USDT', '')
        return f"{base}/USDT:USDT"


class BinanceExchange(CcxtExchange):
    def __init__(self):
        super().__init__('binance')

    def _order_params(self, side):
        return {'type': 'MARKET'}


class KucoinExchange(CcxtExchange):
    def __init__(self):
        super().__init__('kucoin')

    def _fmt(self, symbol: str) -> str:
        base = symbol.replace('USDT', '')
        return f"{base}/USDT:USDT"

    async def get_futures_balance(self) -> float:
        b = await self._client.fetch_balance({'type': 'futures'})
        return float(b.get('USDT', {}).get('free', 0))


class GateExchange(CcxtExchange):
    def __init__(self):
        super().__init__('gate')


class MexcExchange(CcxtExchange):
    def __init__(self):
        super().__init__('mexc')


class OurbitExchange(CcxtExchange):
    def __init__(self):
        try:
            super().__init__('ourbit')
        except AttributeError:
            raise NotImplementedError("OURBIT не найден в ccxt. Нужен кастомный адаптер.")


# ============================================================
# Фабрика
# ============================================================

_EXCHANGE_MAP = {
    'binance': BinanceExchange,
    'bybit':   BybitExchange,
    'bitget':  BitgetExchange,
    'kucoin':  KucoinExchange,
    'gate':    GateExchange,
    'mexc':    MexcExchange,
    'ourbit':  OurbitExchange,
}


def create_exchange(name: str) -> BaseExchange:
    name = name.lower()
    if name not in _EXCHANGE_MAP:
        raise ValueError(f"Unknown exchange: {name}")
    return _EXCHANGE_MAP[name]()


# ============================================================
# Вспомогательные функции
# ============================================================

def _round_qty(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    precision = max(0, -int(math.floor(math.log10(step))))
    return round(round(qty / step) * step, precision)


async def get_all_balances(exchanges: list[str]) -> dict[str, float]:
    """Получает балансы со всех указанных бирж параллельно."""
    async def _fetch(name: str):
        ex = create_exchange(name)
        try:
            bal = await ex.get_futures_balance()
            return name, bal
        except Exception as e:
            print(f"[Balance] {name}: {e}")
            return name, None
        finally:
            await ex.close()

    results = await asyncio.gather(*[_fetch(n) for n in exchanges])
    return {name: bal for name, bal in results if bal is not None}
