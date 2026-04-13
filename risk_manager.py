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

from settings import MAX_OPEN_POSITIONS, LEVERAGE, MIN_VOLUME_USD, MAX_FUNDING_DIFF_PCT, MAX_FUNDING_ABS_PCT
MIN_SPREAD_PCT = 3.5  # HARDCODED минимум спреда
from exchanges import BaseExchange
from db import get_open_trade, get_all_open_trades
from signal_parser import OpenSignal

# Глобальный список выключенных бирж — управляется из GUI
_disabled_exchanges: set[str] = set()

# Выделенный капитал на каждой бирже (чистые деньги без плеча, 0 = весь баланс)
_allocated_capital: dict[str, float] = {}

# Добавьте в начало файла:
_ignored_tickers: set[str] = set()


def set_ignored_tickers(tickers: list[str]):
    global _ignored_tickers
    _ignored_tickers = {t.upper() for t in tickers}


def is_ticker_ignored(ticker: str) -> bool:
    return ticker.upper() in _ignored_tickers


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
    final_size_short: Optional[float] = None  # бюджет для SHORT
    final_size_long: Optional[float] = None   # бюджет для LONG

    def __bool__(self): return self.ok


async def check_signal(signal: OpenSignal, short_ex: BaseExchange, long_ex: BaseExchange) -> RiskResult:
    # В функцию check_signal(signal, ...) в самое начало добавьте:
    # 0. Проверка черного списка
    if is_ticker_ignored(signal.ticker):
        return RiskResult(ok=False, reason=f"Тикер {signal.ticker} находится в черном списке.")

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
            return RiskResult(ok=False,
                              reason=f"Спред упал до {real_spread:.3f}% (мин {MIN_SPREAD_PCT}%). {signal.ticker} пропущен.")

        # --- НОВОЕ УСЛОВИЕ: Проверка отклонения от справедливой цены ---
        # Справедливая цена (fair_price) — это среднее арифметическое цен на обеих биржах
        fair_price = (short_price + long_price) / 2

        # Расчет отклонения для каждой биржи в процентах
        deviation_short = abs(short_price - fair_price) / fair_price * 100
        deviation_long = abs(long_price - fair_price) / fair_price * 100

        # Если отклонение на любой из бирж > 9%, входим в режим ожидания
        if deviation_short > 9 or deviation_long > 9:
            return RiskResult(
                ok=False,
                reason=(f"Аномальное отклонение цены (>9%): Short({deviation_short:.1f}%), "
                        f"Long({deviation_long:.1f}%). Ожидаем выравнивания цен.")
            )
        # -----------------------------------------------------------------------

    # 5b. Фандинг-фильтр — Стратегия #4
    fs = signal.funding_short
    fl = signal.funding_long
    if fs is not None and fl is not None:
        diff_pct      = abs(fs - fl) * 100
        both_negative = fs < 0 and fl < 0
        # Нам платят на обеих позициях:
        #   Short получает фандинг когда rate > 0 (short продаёт → получает)
        #   Long  получает фандинг когда rate < 0 (long покупает → получает)
        both_pay_us   = (fs > 0) and (fl < 0)
        # Одна из бирж платит 0% — нет комиссии на этой ноге, пропускаем
        one_zero      = (fs == 0) or (fl == 0)

        passes = diff_pct < 0.2 or (both_negative and diff_pct < 1.0) or both_pay_us or one_zero

        if not passes:
            return RiskResult(ok=False, reason=(
                f"Фандинг не прошёл: short={fs*100:+.3f}% long={fl*100:+.3f}% "
                f"diff={diff_pct:.3f}% — нужно diff<0.2% ИЛИ (оба отриц. + diff<1%) ИЛИ (оба платят нам) ИЛИ одна сторона 0%. "
                f"{signal.ticker} пропущен."
            ))
        if both_pay_us:
            print(f"[Risk] {signal.ticker}: ФАНДИГ ПЛАТИТ НАМ с обеих сторон! "
                  f"short={fs*100:+.3f}% long={fl*100:+.3f}% — вход разрешён при любом diff")
        if one_zero:
            print(f"[Risk] {signal.ticker}: Одна сторона 0% — вход разрешён без ограничений diff")

    # 6. Проверка объёма (если MIN_VOLUME_USD > 0)
    if MIN_VOLUME_USD > 0:
        for vol, exname in [(vol_short, signal.short_exchange), (vol_long, signal.long_exchange)]:
            if isinstance(vol, Exception):
                print(f"[Risk] Объём {exname} недоступен: {vol}")
                continue
            if vol is not None and vol < MIN_VOLUME_USD:
                return RiskResult(ok=False, reason=f"Низкий объём на {exname.upper()}: ${vol:,.0f} < минимум ${MIN_VOLUME_USD:,.0f}. {signal.ticker} пропущен.")

    # 7. Итоговый размер — для КАЖДОЙ биржи отдельно
    # Если allocated задано — используем allocated × leverage
    # Иначе — balance × leverage (если баланс доступен)
    # Ограничиваем max_position с каждой стороны
    alloc_short = get_allocated(signal.short_exchange)
    alloc_long  = get_allocated(signal.long_exchange)

    # SHORT budget
    if alloc_short > 0:
        final_size_short = alloc_short * LEVERAGE
        limit_short = f'{signal.short_exchange}_allocated×lev'
    elif not isinstance(bal_short, Exception) and bal_short > 0:
        final_size_short = bal_short * LEVERAGE
        limit_short = 'balance_short×lev'
    else:
        final_size_short = 0
        limit_short = 'no_balance'

    # LONG budget
    if alloc_long > 0:
        final_size_long = alloc_long * LEVERAGE
        limit_long = f'{signal.long_exchange}_allocated×lev'
    elif not isinstance(bal_long, Exception) and bal_long > 0:
        final_size_long = bal_long * LEVERAGE
        limit_long = 'balance_long×lev'
    else:
        final_size_long = 0
        limit_long = 'no_balance'

    # Ограничиваем max_position
    if not isinstance(max_short, Exception) and max_short:
        if max_short < final_size_short:
            final_size_short = max_short
            limit_short = f'{signal.short_exchange}_max'
    if not isinstance(max_long, Exception) and max_long:
        if max_long < final_size_long:
            final_size_long = max_long
            limit_long = f'{signal.long_exchange}_max'

    # Ограничиваем signal max_size
    if signal.max_size_short and signal.max_size_short < final_size_short:
        final_size_short = signal.max_size_short
        limit_short = 'signal_max_short'
    if signal.max_size_long and signal.max_size_long < final_size_long:
        final_size_long = signal.max_size_long
        limit_long = 'signal_max_long'

    if final_size_short <= 0 or final_size_long <= 0:
        return RiskResult(ok=False, reason=f"Не удалось определить размер позиции для {signal.ticker}")

    print(f"[Risk] {signal.ticker}: short={limit_short}=${final_size_short:.2f}, long={limit_long}=${final_size_long:.2f}")

    vol_info = ""
    if not isinstance(vol_short, Exception) and vol_short:
        vol_info += f" | vol_short=${vol_short:,.0f}"
    if not isinstance(vol_long, Exception) and vol_long:
        vol_info += f" | vol_long=${vol_long:,.0f}"

    return RiskResult(
        ok=True,
        reason=(f"✅ {signal.ticker}: спред {real_spread:.2f}%, "
                f"short ${final_size_short:.2f}, long ${final_size_long:.2f}, "
                f"плечо {LEVERAGE}×, позиций {len(open_trades)}/{MAX_OPEN_POSITIONS}{vol_info}"),
        final_size_usd=min(final_size_short, final_size_long),
        final_size_short=final_size_short,
        final_size_long=final_size_long,
    )