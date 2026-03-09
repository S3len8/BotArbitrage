"""
core/risk_manager.py — проверки перед входом + расчёт итогового размера позиции.

Логика расчёта размера:
  1. balance_short × LEVERAGE  → max_from_balance_short
  2. balance_long  × LEVERAGE  → max_from_balance_long
  3. exchange_max_position_short → лимит биржи для символа
  4. exchange_max_position_long
  5. signal_max_size_short, signal_max_size_long → из сигнала
  final_size = min(все перечисленные)
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

from config.settings import MAX_OPEN_POSITIONS, MIN_SPREAD_PCT, LEVERAGE
from exchanges.base import BaseExchange
from storage.db import get_open_trade, get_all_open_trades
from telegram.signal_parser import OpenSignal


@dataclass
class RiskCheckResult:
    ok: bool
    reason: str = ""
    final_size_usd: Optional[float] = None  # итоговый размер позиции

    def __bool__(self):
        return self.ok


async def check_signal(
    signal: OpenSignal,
    short_exchange: BaseExchange,
    long_exchange: BaseExchange,
) -> RiskCheckResult:

    # 1. Лимит открытых позиций
    open_trades = get_all_open_trades()
    if len(open_trades) >= MAX_OPEN_POSITIONS:
        return RiskCheckResult(
            ok=False,
            reason=f"Лимит позиций: {len(open_trades)}/{MAX_OPEN_POSITIONS} открыто. {signal.ticker} пропущен."
        )

    # 2. Уже открыта позиция по тикеру
    if get_open_trade(signal.ticker):
        return RiskCheckResult(
            ok=False,
            reason=f"Позиция по {signal.ticker} уже открыта. Ждём CLOSE."
        )

    # 3. Параллельно получаем: цены + балансы + лимиты бирж
    (
        short_price, long_price,
        balance_short, balance_long,
        max_pos_short, max_pos_long,
    ) = await asyncio.gather(
        short_exchange.get_price(signal.symbol),
        long_exchange.get_price(signal.symbol),
        short_exchange.get_futures_balance(),
        long_exchange.get_futures_balance(),
        short_exchange.get_max_position_size(signal.symbol),
        long_exchange.get_max_position_size(signal.symbol),
        return_exceptions=True,
    )

    # Проверяем что цены получены
    for val, name in [(short_price, 'short price'), (long_price, 'long price')]:
        if isinstance(val, Exception):
            return RiskCheckResult(ok=False, reason=f"Ошибка цены ({name}): {val}")

    # 4. Проверка реального спреда
    if long_price > 0:
        real_spread = (short_price / long_price - 1) * 100
    else:
        return RiskCheckResult(ok=False, reason="Некорректная long-цена")

    if real_spread < MIN_SPREAD_PCT:
        return RiskCheckResult(
            ok=False,
            reason=(
                f"Спред упал до {real_spread:.3f}% (мин: {MIN_SPREAD_PCT}%). "
                f"{signal.ticker} пропущен. "
                f"short={short_price:.6f}@{signal.short_exchange}, "
                f"long={long_price:.6f}@{signal.long_exchange}"
            )
        )

    # 5. Рассчитываем итоговый размер позиции
    # Каждый источник — ограничение сверху
    size_candidates = []

    # Из баланса × плечо
    if not isinstance(balance_short, Exception) and balance_short > 0:
        size_candidates.append(('balance_short×lev', balance_short * LEVERAGE))
    if not isinstance(balance_long, Exception) and balance_long > 0:
        size_candidates.append(('balance_long×lev', balance_long * LEVERAGE))

    # Из лимита биржи на символ
    if not isinstance(max_pos_short, Exception) and max_pos_short:
        size_candidates.append((f'{signal.short_exchange}_max_pos', max_pos_short))
    if not isinstance(max_pos_long, Exception) and max_pos_long:
        size_candidates.append((f'{signal.long_exchange}_max_pos', max_pos_long))

    # Из сигнала (Max Size)
    if signal.max_size_short:
        size_candidates.append(('signal_max_short', signal.max_size_short))
    if signal.max_size_long:
        size_candidates.append(('signal_max_long', signal.max_size_long))

    if not size_candidates:
        return RiskCheckResult(
            ok=False,
            reason=f"Не удалось определить размер позиции для {signal.ticker}"
        )

    final_size = min(v for _, v in size_candidates)
    limiting_factor = min(size_candidates, key=lambda x: x[1])

    if final_size <= 0:
        return RiskCheckResult(ok=False, reason=f"Размер позиции = 0 для {signal.ticker}")

    return RiskCheckResult(
        ok=True,
        reason=(
            f"✅ {signal.ticker}: спред {real_spread:.2f}%, "
            f"размер ${final_size:.2f} (ограничен: {limiting_factor[0]}), "
            f"плечо {LEVERAGE}x, "
            f"позиций {len(open_trades)}/{MAX_OPEN_POSITIONS}"
        ),
        final_size_usd=final_size,
    )
