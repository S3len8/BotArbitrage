"""
risk_manager.py — проверки перед входом + расчёт итогового размера позиции.

Проверки:
1. Лимит открытых позиций
2. Дубль по тикеру
3. Биржа выключена в GUI
4. Цены, балансы, лимиты бирж
5. Реальный спред
6. Объём за 24ч (MIN_VOLUME_USD)
7. Итоговый размер позиции
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

from settings import MAX_OPEN_POSITIONS, MIN_SPREAD_PCT, LEVERAGE, MIN_VOLUME_USD
from exchanges import BaseExchange
from db import get_open_trade, get_all_open_trades
from signal_parser import OpenSignal

# Глобальный список выключенных бирж — управляется из GUI
_disabled_exchanges: set[str] = set()

# Выделенный капитал на каждой бирже (чистые деньги без плеча, 0 = весь баланс)
_allocated_capital: dict[str, float] = {}


def set_exchange_enabled(exchange: str, enabled: bool):
    if enabled:
        _disabled_exchanges.discard(exchange.lower())
    else:
        _disabled_exchanges.add(exchange.lower())


def set_exchange_allocated(exchange: str, amount: float):
    """Задать выделенный капитал для биржи. 0 = использовать весь баланс."""
    _allocated_capital[exchange.lower()] = amount


def get_allocated(exchange: str) -> float:
    """Возвращает выделенный капитал. 0 если не задан."""
    return _allocated_capital.get(exchange.lower(), 0.0)


def get_disabled_exchanges() -> list[str]:
    return list(_disabled_exchanges)


def is_exchange_enabled(exchange: str) -> bool:
    return exchange.lower() not in _disabled_exchanges


@dataclass
class RiskResult:
    ok: bool
    reason: str = ""
    final_size_usd: Optional[float] = None

    def __bool__(self): return self.ok


async def check_signal(signal: OpenSignal, short_ex: BaseExchange, long_ex: BaseExchange) -> RiskResult:

    # 1. Лимит открытых позиций
    open_trades = get_all_open_trades()
    if len(open_trades) >= MAX_OPEN_POSITIONS:
        return RiskResult(ok=False, reason=f"Лимит позиций: {len(open_trades)}/{MAX_OPEN_POSITIONS}. {signal.ticker} пропущен.")

    # 2. Дубль по тикеру
    if get_open_trade(signal.ticker):
        return RiskResult(ok=False, reason=f"Позиция по {signal.ticker} уже открыта.")

    # 3. Проверка выключенных бирж
    if not is_exchange_enabled(signal.short_exchange):
        return RiskResult(ok=False, reason=f"Биржа {signal.short_exchange.upper()} выключена в GUI. {signal.ticker} пропущен.")
    if not is_exchange_enabled(signal.long_exchange):
        return RiskResult(ok=False, reason=f"Биржа {signal.long_exchange.upper()} выключена в GUI. {signal.ticker} пропущен.")

    # 4. Параллельно: цены + балансы + лимиты + объёмы
    short_price, long_price, bal_short, bal_long, max_short, max_long, vol_short, vol_long = await asyncio.gather(
        short_ex.get_price(signal.symbol),
        long_ex.get_price(signal.symbol),
        short_ex.get_futures_balance(),
        long_ex.get_futures_balance(),
        short_ex.get_max_position_size(signal.symbol),
        long_ex.get_max_position_size(signal.symbol),
        short_ex.get_24h_volume(signal.symbol),
        long_ex.get_24h_volume(signal.symbol),
        return_exceptions=True,
    )

    for val, label in [(short_price, 'short price'), (long_price, 'long price')]:
        if isinstance(val, Exception):
            return RiskResult(ok=False, reason=f"Ошибка получения {label}: {val}")

    # 5. Реальный спред
    real_spread = (short_price / long_price - 1) * 100 if long_price > 0 else 0
    if real_spread < MIN_SPREAD_PCT:
        return RiskResult(ok=False, reason=f"Спред упал до {real_spread:.3f}% (мин {MIN_SPREAD_PCT}%). {signal.ticker} пропущен.")

    # 6. Проверка объёма (если MIN_VOLUME_USD > 0)
    if MIN_VOLUME_USD > 0:
        for vol, exname in [(vol_short, signal.short_exchange), (vol_long, signal.long_exchange)]:
            if isinstance(vol, Exception):
                print(f"[Risk] Объём {exname} недоступен: {vol}")
                continue
            if vol is not None and vol < MIN_VOLUME_USD:
                return RiskResult(ok=False, reason=f"Низкий объём на {exname.upper()}: ${vol:,.0f} < минимум ${MIN_VOLUME_USD:,.0f}. {signal.ticker} пропущен.")

    # 7. Итоговый размер — минимум из всех ограничений
    candidates = []

    # Выделенный капитал × плечо (приоритет если задан)
    alloc_short = get_allocated(signal.short_exchange)
    alloc_long  = get_allocated(signal.long_exchange)

    if alloc_short > 0:
        candidates.append((f'{signal.short_exchange}_allocated×lev', alloc_short * LEVERAGE))
    elif not isinstance(bal_short, Exception) and bal_short > 0:
        candidates.append(('balance_short×lev', bal_short * LEVERAGE))

    if alloc_long > 0:
        candidates.append((f'{signal.long_exchange}_allocated×lev', alloc_long * LEVERAGE))
    elif not isinstance(bal_long, Exception) and bal_long > 0:
        candidates.append(('balance_long×lev', bal_long * LEVERAGE))
    if not isinstance(max_short, Exception) and max_short:
        candidates.append((f'{signal.short_exchange}_max', max_short))
    if not isinstance(max_long,  Exception) and max_long:
        candidates.append((f'{signal.long_exchange}_max',  max_long))
    if signal.max_size_short:
        candidates.append(('signal_max_short', signal.max_size_short))
    if signal.max_size_long:
        candidates.append(('signal_max_long',  signal.max_size_long))

    if not candidates:
        return RiskResult(ok=False, reason=f"Не удалось определить размер позиции для {signal.ticker}")

    final_size = min(v for _, v in candidates)
    limiting   = min(candidates, key=lambda x: x[1])

    vol_info = ""
    if not isinstance(vol_short, Exception) and vol_short:
        vol_info += f" | vol_short=${vol_short:,.0f}"
    if not isinstance(vol_long, Exception) and vol_long:
        vol_info += f" | vol_long=${vol_long:,.0f}"

    return RiskResult(
        ok=True,
        reason=(f"✅ {signal.ticker}: спред {real_spread:.2f}%, "
                f"размер ${final_size:.2f} (лимит: {limiting[0]}), "
                f"плечо {LEVERAGE}×, позиций {len(open_trades)}/{MAX_OPEN_POSITIONS}{vol_info}"),
        final_size_usd=final_size,
    )
