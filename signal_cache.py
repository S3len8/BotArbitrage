"""
signal_cache.py — кэш сигналов для ожидания подходящего спреда.

Логика:
1. Если спред сигнала < MIN_SPREAD_PCT → сохраняем сигнал в кэш
2. Фоновый монитор каждые 5 сек проверяет живые цены кэшированных сигналов
3. Если спред вырос >= MIN_SPREAD_PCT → запускаем риск-проверку и входим
4. Если пришёл CLOSE сигнал → удаляем из кэша
5. Если сигнал устарел (CACHE_TTL) → удаляем
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

from settings import MIN_SPREAD_PCT, LEVERAGE
from signal_parser import OpenSignal
from exchanges import create_exchange
from notifier import notify

# ── Настройки ─────────────────────────────────────────────────
CACHE_TTL_SECONDS = 15 * 60  # 15 минут — время жизни кэшированного сигнала
MONITOR_INTERVAL  = 5        # проверка каждые 5 секунд
SPREAD_ENTRY_DROP = 1.2      # если спред упал на 1.2% от входа → закрываем

# ── Кэш сигналов ──────────────────────────────────────────────
_cached_signals: dict[str, "CachedSignal"] = {}
_monitor_task: asyncio.Task | None = None


@dataclass
class CachedSignal:
    """Сигнал ожидающий подходящего спреда."""
    signal: OpenSignal
    cached_at: float = field(default_factory=time.time)
    last_check: float = field(default_factory=time.time)
    last_spread: float = 0.0
    attempts: int = 0
    max_attempts: int = 180  # ~15 минут при проверке каждые 5 сек


def add_signal(signal: OpenSignal, current_spread: float):
    """Сохраняет сигнал в кэш для мониторинга."""
    cached = CachedSignal(
        signal=signal,
        last_spread=current_spread,
    )
    _cached_signals[signal.ticker] = cached
    print(f"[SignalCache] {signal.ticker} добавлен в кэш: спред {current_spread:.2f}% < {MIN_SPREAD_PCT}%")


def remove_signal(ticker: str):
    """Удаляет сигнал из кэша."""
    removed = _cached_signals.pop(ticker, None)
    if removed:
        print(f"[SignalCache] {ticker} удалён из кэша")
    return removed is not None


def get_signal(ticker: str) -> Optional[OpenSignal]:
    """Возвращает сигнал из кэша если есть."""
    cached = _cached_signals.get(ticker)
    return cached.signal if cached else None


def is_cached(ticker: str) -> bool:
    return ticker in _cached_signals


def get_cached_count() -> int:
    return len(_cached_signals)


# ── Мониторинг кэшированных сигналов ──────────────────────────

async def start_signal_monitor():
    """Запускает фоновый мониторинг кэшированных сигналов."""
    global _monitor_task
    if _monitor_task and not _monitor_task.done():
        return
    _monitor_task = asyncio.create_task(_monitor_loop())
    print("[SignalCache] Монитор запущен")


async def stop_signal_monitor():
    """Останавливает монитор."""
    global _monitor_task
    if _monitor_task and not _monitor_task.done():
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
    _monitor_task = None
    print("[SignalCache] Монитор остановлен")


async def _monitor_loop():
    """Периодически проверяет кэшированные сигналы + фандинг."""
    from risk_manager import check_signal
    from order_executor import open_position
    from listener import _minutes_until, _get_funding_times, WAIT_BEFORE_FUNDING_MIN

    while True:
        await asyncio.sleep(MONITOR_INTERVAL)

        if not _cached_signals:
            continue

        now = time.time()
        stale = []

        for ticker, cached in list(_cached_signals.items()):
            # Проверяем время жизни
            if now - cached.cached_at > CACHE_TTL_SECONDS:
                stale.append(ticker)
                print(f"[SignalCache] {ticker} устарел ({CACHE_TTL_SECONDS // 60} мин) — удаляю")
                continue

            cached.attempts += 1
            if cached.attempts > cached.max_attempts:
                stale.append(ticker)
                print(f"[SignalCache] {ticker} исчерпал попытки ({cached.max_attempts}) — удаляю")
                continue

            # Получаем живые цены + фандинг
            try:
                sig = cached.signal
                s_ex = create_exchange(sig.short_exchange)
                l_ex = create_exchange(sig.long_exchange)
                try:
                    s_price, l_price, s_fund, l_fund = await asyncio.gather(
                        s_ex.get_price(sig.symbol),
                        l_ex.get_price(sig.symbol),
                        s_ex.get_funding_rate(sig.symbol),
                        l_ex.get_funding_rate(sig.symbol),
                        return_exceptions=True,
                    )
                finally:
                    await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)

                if isinstance(s_price, Exception) or isinstance(l_price, Exception):
                    continue

                sp = float(s_price)
                lp = float(l_price)
                if lp <= 0:
                    continue

                current_spread = (sp / lp - 1) * 100
                cached.last_spread = current_spread
                cached.last_check = now

                print(f"[SignalCache] {ticker}: спред {current_spread:.2f}% (попытка {cached.attempts})")

                # Спред должен быть >= 3%
                if current_spread < MIN_SPREAD_PCT:
                    continue

                # Проверяем время до фандинга
                try:
                    from signal_parser import OpenSignal as _OS
                    temp_sig = _OS(
                        ticker=sig.ticker, symbol=sig.symbol,
                        short_exchange=sig.short_exchange, long_exchange=sig.long_exchange,
                        short_price=sp, long_price=lp, spread_pct=current_spread,
                        funding_short=sig.funding_short, funding_long=sig.funding_long,
                        max_size_short=sig.max_size_short, max_size_long=sig.max_size_long,
                        interval_short=sig.interval_short, interval_long=sig.interval_long,
                        raw_text=sig.raw_text,
                    )
                    next_short_ts, next_long_ts = await asyncio.wait_for(
                        _get_funding_times(temp_sig), timeout=10
                    )
                    min_until = min(
                        _minutes_until(next_short_ts),
                        _minutes_until(next_long_ts),
                    )
                except Exception:
                    min_until = 9999  # если не удалось получить — считаем что далеко

                if min_until <= WAIT_BEFORE_FUNDING_MIN:
                    print(f"[SignalCache] {ticker}: до фандинга {min_until:.0f} мин — жду начисления")
                    continue

                print(f"[SignalCache] {ticker}: спред {current_spread:.2f}% >= {MIN_SPREAD_PCT}%, фандинг через {min_until:.0f}м — вхожу!")

                # Удаляем из кэша чтобы не дублировать
                _cached_signals.pop(ticker, None)

                # Обновляем фандинг из бирж
                sf = s_fund.get('rate') if (not isinstance(s_fund, Exception) and s_fund) else sig.funding_short
                lf = l_fund.get('rate') if (not isinstance(l_fund, Exception) and l_fund) else sig.funding_long
                i_s = s_fund.get('interval_hours') if (not isinstance(s_fund, Exception) and s_fund) else sig.interval_short
                i_l = l_fund.get('interval_hours') if (not isinstance(l_fund, Exception) and l_fund) else sig.interval_long

                # Создаём обновлённый сигнал с актуальными ценами
                from signal_parser import OpenSignal as _OS
                updated = _OS(
                    ticker=sig.ticker,
                    symbol=sig.symbol,
                    short_exchange=sig.short_exchange,
                    long_exchange=sig.long_exchange,
                    short_price=sp,
                    long_price=lp,
                    spread_pct=current_spread,
                    funding_short=sf,
                    funding_long=lf,
                    max_size_short=sig.max_size_short,
                    max_size_long=sig.max_size_long,
                    interval_short=i_s,
                    interval_long=i_l,
                    raw_text=f"[cached-signal entry] {sig.ticker}",
                )

                await notify(
                    f"📡 <b>{ticker}: кэшированный сигнал активирован</b>\n"
                    f"📊 Спред: {current_spread:.2f}% (мин {MIN_SPREAD_PCT}%)\n"
                    f"⏱ До фандинга: {min_until:.0f}м\n"
                    f"⚡ Открываю позицию..."
                )

                # Проверяем риск
                s_ex2 = create_exchange(sig.short_exchange)
                l_ex2 = create_exchange(sig.long_exchange)
                try:
                    risk = await asyncio.wait_for(
                        check_signal(updated, s_ex2, l_ex2),
                        timeout=30
                    )
                finally:
                    await asyncio.gather(s_ex2.close(), l_ex2.close(), return_exceptions=True)

                if not risk:
                    print(f"[SignalCache] {ticker}: риск-менеджер отклонил — {risk.reason}")
                    await notify(f"⛔ <b>{ticker}: кэшированный сигнал отклонён</b>\n{risk.reason}")
                    continue

                # Открываем позицию
                try:
                    trade, msg = await asyncio.wait_for(
                        open_position(updated, risk.final_size_usd, signal_received_ms=int(cached.cached_at * 1000)),
                        timeout=60
                    )
                    if trade:
                        print(f"[SignalCache] {ticker}: успешно открыта позиция trade_id={trade.id}")
                    else:
                        print(f"[SignalCache] {ticker}: открытие не удалось")
                except Exception as e:
                    print(f"[SignalCache] {ticker}: ошибка открытия — {e}")
                    await notify(f"❌ <b>{ticker}: ошибка открытия кэшированного сигнала</b>\n{e}")

            except Exception as e:
                print(f"[SignalCache] {ticker}: ошибка проверки — {e}")

        # Удаляем устаревшие
        for ticker in stale:
            _cached_signals.pop(ticker, None)


# ── Мониторинг спреда открытых позиций (авто-закрытие) ─────────

_spread_monitor_task: asyncio.Task | None = None
SPREAD_MONITOR_INTERVAL = 10  # проверка каждые 10 секунд


async def start_spread_monitor():
    """Запускает мониторинг спреда открытых позиций."""
    global _spread_monitor_task
    if _spread_monitor_task and not _spread_monitor_task.done():
        return
    _spread_monitor_task = asyncio.create_task(_spread_monitor_loop())
    print("[SpreadMonitor] Запущен")


async def stop_spread_monitor():
    global _spread_monitor_task
    if _spread_monitor_task and not _spread_monitor_task.done():
        _spread_monitor_task.cancel()
        try:
            await _spread_monitor_task
        except asyncio.CancelledError:
            pass
    _spread_monitor_task = None


async def _spread_monitor_loop():
    """Проверяет спред открытых позиций относительно входа."""
    from db import get_all_open_trades
    from order_executor import close_position
    from signal_parser import CloseSignal

    while True:
        await asyncio.sleep(SPREAD_MONITOR_INTERVAL)

        try:
            trades = get_all_open_trades()
            if not trades:
                continue

            for t in trades:
                try:
                    if not t.short_entry_price or not t.long_entry_price:
                        continue

                    # Спред на момент входа: ((short - long) / short) * 100
                    entry_spread = ((t.short_entry_price - t.long_entry_price) / t.short_entry_price) * 100

                    # Текущий спред с бирж
                    s_ex = create_exchange(t.short_exchange)
                    l_ex = create_exchange(t.long_exchange)
                    try:
                        s_price, l_price = await asyncio.gather(
                            s_ex.get_price(t.symbol),
                            l_ex.get_price(t.symbol),
                            return_exceptions=True,
                        )
                    finally:
                        await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)

                    if isinstance(s_price, Exception) or isinstance(l_price, Exception):
                        continue

                    sp = float(s_price)
                    lp = float(l_price)
                    if sp <= 0:
                        continue

                    current_spread = ((sp - lp) / sp) * 100

                    spread_drop = entry_spread - current_spread

                    print(f"[SpreadMonitor] {t.ticker}: entry_spread={entry_spread:.2f}% "
                          f"current={current_spread:.2f}% drop={spread_drop:.2f}%")

                    # Если спред упал на 1.2% и более от входа — закрываем
                    if spread_drop >= SPREAD_ENTRY_DROP:
                        print(f"[SpreadMonitor] 🚨 {t.ticker}: спред упал на {spread_drop:.2f}% от входа — закрываю!")

                        await notify(
                            f"🚨 <b>Авто-закрытие: {t.ticker}</b>\n"
                            f"📊 Сpread входа: {entry_spread:.2f}%\n"
                            f"📊 Текущий спред: {current_spread:.2f}%\n"
                            f"📉 Падение: {spread_drop:.2f}% (лимит: {SPREAD_ENTRY_DROP}%)\n"
                            f"⚡ Закрываю позицию..."
                        )

                        cs = CloseSignal(ticker=t.ticker, raw_text=f"spread_drop_{spread_drop:.2f}%")
                        _, msg = await close_position(cs)
                        await notify(msg)

                        # После закрытия — кэшируем сигнал для возможного перезахода
                        cached_sig = OpenSignal(
                            ticker=t.ticker,
                            symbol=t.symbol,
                            short_exchange=t.short_exchange,
                            long_exchange=t.long_exchange,
                            short_price=sp,
                            long_price=lp,
                            spread_pct=current_spread,
                            funding_short=t.funding_short,
                            funding_long=t.funding_long,
                            max_size_short=None,
                            max_size_long=None,
                            interval_short=None,
                            interval_long=None,
                            raw_text=f"[spread-drop reentry] {t.ticker}",
                        )
                        add_signal(cached_sig, current_spread)
                        print(f"[SpreadMonitor] {t.ticker}: сигнал кэширован для перезахода")

                except Exception as e:
                    print(f"[SpreadMonitor] ошибка {t.ticker}: {e}")

        except Exception as e:
            print(f"[SpreadMonitor] loop error: {e}")
