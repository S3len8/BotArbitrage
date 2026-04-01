"""
order_executor.py — открытие и закрытие арбитражных позиций.
Замеряет время исполнения каждой ноги от момента получения сигнала.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from exchanges import create_exchange
from db import Trade, save_trade, update_trade, get_open_trade, save_balance_snapshot
from signal_parser import OpenSignal, CloseSignal
from notifier import notify, tradingview_url, edit_notify, exchange_url, notify_order
from settings import LEVERAGE, BALANCE_ALERT_PCT, MARGIN_RISK_PCT


def _now(): return datetime.now(timezone.utc)
def _ms():  return int(time.time() * 1000)


async def open_position(signal: OpenSignal, final_size_usd: float, signal_received_ms: int = None) -> tuple[Optional[Trade], str]:
    """
    signal_received_ms — время получения сигнала в ms (time.time()*1000).
    Если не передан — замер начинается с момента вызова функции.
    """
    t0 = signal_received_ms or _ms()

    short_ex = create_exchange(signal.short_exchange)
    long_ex  = create_exchange(signal.long_exchange)

    trade = Trade(
        ticker=signal.ticker, symbol=signal.symbol,
        short_exchange=signal.short_exchange, long_exchange=signal.long_exchange,
        trade_size_usd=final_size_usd, status='open', leverage=LEVERAGE,
        opened_at=_now(),
        funding_short=signal.funding_short, funding_long=signal.funding_long,
        signal_spread_pct=signal.spread_pct, signal_text=signal.raw_text[:500],
    )

    print(f"[Executor] OPEN {signal.ticker}: SHORT ${final_size_usd:.2f}@{signal.short_exchange} LONG ${final_size_usd:.2f}@{signal.long_exchange} lev={LEVERAGE}×")

    # Открываем ноги ПОСЛЕДОВАТЕЛЬНО: сначала short, потом long.
    # Если short упал — не открываем long совсем.
    # Если long упал — сразу откатываем short.
    short_r: dict | Exception = Exception("not started")
    long_r:  dict | Exception = Exception("not started")

    # ── 1. Short ──
    try:
        short_r = await asyncio.wait_for(
            short_ex.place_market_order(signal.symbol, 'sell', final_size_usd),
            timeout=20
        )
    except asyncio.TimeoutError:
        short_r = Exception(f"timeout 20с")
    except Exception as e:
        short_r = e

    short_ok = not isinstance(short_r, Exception)

    if not short_ok:
        print(f"[Executor] SHORT ошибка {signal.short_exchange}: {short_r}")
        trade.status = 'failed'
        save_trade(trade)
        msg = (f"❌ <b>{signal.ticker}</b>: Short не открылся ({signal.short_exchange.upper()})\n"
               f"Long не открывался — откат не нужен\n"
               f"Причина: {short_r}")
        await _close(short_ex, long_ex)
        await notify(msg)
        return None, msg

    # ── 2. Long ──
    try:
        long_r = await asyncio.wait_for(
            long_ex.place_market_order(signal.symbol, 'buy', final_size_usd),
            timeout=20
        )
    except asyncio.TimeoutError:
        long_r = Exception(f"timeout 20с")
    except Exception as e:
        long_r = e

    long_ok = not isinstance(long_r, Exception)

    t_after = _ms()
    exec_ms = t_after - t0

    if not long_ok:
        print(f"[Executor] LONG ошибка {signal.long_exchange}: {long_r}")
        # Сразу откатываем short
        try:
            await asyncio.wait_for(
                short_ex.close_position(signal.symbol, 'buy', short_r['qty']),
                timeout=15
            )
            rollback_status = "Short откатан ✅"
        except Exception as e:
            rollback_status = f"Short откат ПРОВАЛИЛСЯ ⚠️: {e}"
            print(f"[Executor] rollback short failed: {e}")

        trade.status = 'failed'
        save_trade(trade)
        msg = (f"❌ <b>{signal.ticker}</b>: Long не открылся ({signal.long_exchange.upper()})\n"
               f"{rollback_status}\n"
               f"Причина: {long_r}")
        await _close(short_ex, long_ex)
        await notify(msg)
        return None, msg

    if short_ok and long_ok:
        trade.short_order_id    = short_r['order_id'];  trade.short_entry_price = short_r['price']
        trade.short_qty         = short_r['qty'];        trade.fee_short_usd     = short_r.get('fee', 0)
        trade.long_order_id     = long_r['order_id'];   trade.long_entry_price  = long_r['price']
        trade.long_qty          = long_r['qty'];         trade.fee_long_usd      = long_r.get('fee', 0)
        trade.exec_time_short_ms = exec_ms
        trade.exec_time_long_ms  = exec_ms
        save_trade(trade)

        # Логуємо ордери в канал ордерів
        asyncio.create_task(notify_order(
            signal.short_exchange, 'short', signal.symbol,
            short_r['price'], short_r['qty'], final_size_usd,
            short_r['order_id'], f"↔️ Pair: <b>{signal.ticker}</b> | Trade #{trade.id}"
        ))
        asyncio.create_task(notify_order(
            signal.long_exchange, 'long', signal.symbol,
            long_r['price'], long_r['qty'], final_size_usd,
            long_r['order_id'], f"↔️ Pair: <b>{signal.ticker}</b> | Trade #{trade.id}"
        ))

        # Фактический спред по ценам исполнения
        actual_spread = 0.0
        if trade.long_entry_price and trade.long_entry_price > 0:
            actual_spread = (trade.short_entry_price / trade.long_entry_price - 1) * 100

        tv_url   = tradingview_url(signal.short_exchange, signal.long_exchange, signal.ticker)
        short_url = exchange_url(signal.short_exchange, signal.ticker)
        long_url  = exchange_url(signal.long_exchange, signal.ticker)
        ex_buttons = []
        if short_url:
            ex_buttons.append({"text": f"📉 {signal.short_exchange.upper()}", "url": short_url})
        if long_url:
            ex_buttons.append({"text": f"📈 {signal.long_exchange.upper()}", "url": long_url})

        def _build_msg(current_spread: float = None) -> str:
            spread_line = f"📊 Спред сигнала: {signal.spread_pct:.2f}% | Фактический: {actual_spread:.2f}%"
            if current_spread is not None:
                spread_line += f"\n📡 Текущий спред: {current_spread:.2f}%"
            return (
                f"✅ <b>Открыто: {signal.ticker}</b>\n"
                f"📉 SHORT {signal.short_exchange.upper()}: {trade.short_qty} @ ${trade.short_entry_price:.6f}\n"
                f"📈 LONG  {signal.long_exchange.upper()}: {trade.long_qty} @ ${trade.long_entry_price:.6f}\n"
                f"💵 Размер: ${final_size_usd:.2f} | Плечо: {LEVERAGE}×\n"
                f"{spread_line}\n"
                f"⚡ Исполнение: {exec_ms}мс от сигнала"
            )

        await _close(short_ex, long_ex)
        msg = _build_msg()
        msg_id = await notify(msg, tv_url=tv_url, buttons=ex_buttons)

        # Live-обновление спреда каждые 30 секунд пока позиция открыта
        async def _live_spread_updater():
            if not msg_id:
                return
            from exchanges import create_exchange
            for _ in range(20):  # максимум 20 обновлений = 10 минут
                await asyncio.sleep(30)
                try:
                    from db import get_open_trade
                    open_t = get_open_trade(trade.id)
                    if not open_t or open_t.status != 'open':
                        break  # позиция закрыта — прекращаем
                    s_ex = create_exchange(signal.short_exchange)
                    l_ex = create_exchange(signal.long_exchange)
                    try:
                        sp, lp = await asyncio.gather(
                            s_ex.get_price(signal.symbol),
                            l_ex.get_price(signal.symbol),
                        )
                        cur_spread = (sp / lp - 1) * 100 if lp > 0 else 0
                        updated_msg = _build_msg(cur_spread)
                        await edit_notify(msg_id, updated_msg, tv_url=tv_url, buttons=ex_buttons)
                    finally:
                        await s_ex.close()
                        await l_ex.close()
                except Exception as e:
                    print(f"[Executor] live spread error: {e}")
                    break

        asyncio.create_task(_live_spread_updater())
        return trade, msg

    await _close(short_ex, long_ex)
    return trade, msg


async def close_position(close_signal: CloseSignal) -> tuple[Optional[Trade], str]:
    trade = get_open_trade(close_signal.ticker)
    if not trade:
        return None, f"ℹ️ Нет открытой позиции по <b>{close_signal.ticker}</b>."

    short_ex = create_exchange(trade.short_exchange)
    long_ex  = create_exchange(trade.long_exchange)

    short_c, long_c = await asyncio.gather(
        short_ex.close_position(trade.symbol, 'buy',  trade.short_qty or 0),
        long_ex.close_position( trade.symbol, 'sell', trade.long_qty  or 0),
        return_exceptions=True,
    )
    short_ok = not isinstance(short_c, Exception)
    long_ok  = not isinstance(long_c,  Exception)

    trade.closed_at = _now()
    if short_ok:
        trade.short_close_price = short_c['price']
        trade.short_close_order_id = short_c['order_id']
        trade.fee_short_usd = (trade.fee_short_usd or 0) + short_c.get('fee', 0)
        # Логуємо закриття short
        asyncio.create_task(notify_order(
            trade.short_exchange, 'buy (close short)', trade.symbol,
            short_c['price'], trade.short_qty or 0, trade.trade_size_usd or 0,
            short_c['order_id'], f"🔒 CLOSE | Trade #{trade.id} <b>{trade.ticker}</b>"
        ))
    if long_ok:
        trade.long_close_price = long_c['price']
        trade.long_close_order_id = long_c['order_id']
        trade.fee_long_usd = (trade.fee_long_usd or 0) + long_c.get('fee', 0)
        # Логуємо закриття long
        asyncio.create_task(notify_order(
            trade.long_exchange, 'sell (close long)', trade.symbol,
            long_c['price'], trade.long_qty or 0, trade.trade_size_usd or 0,
            long_c['order_id'], f"🔒 CLOSE | Trade #{trade.id} <b>{trade.ticker}</b>"
        ))

    trade.status = 'closed' if (short_ok and long_ok) else 'partial'

    # 🔧 ИСПРАВЛЕНИЕ: Сразу сохраняем статус в БД чтобы бот видел что позиция закрыта
    update_trade(trade)

    # Получаем реальный PnL с бирж (через 500мс после закрытия чтобы данные появились)
    await asyncio.sleep(0.5)
    real_short_pnl = None
    real_long_pnl  = None
    try:
        s_ex2 = create_exchange(trade.short_exchange)
        l_ex2 = create_exchange(trade.long_exchange)
        s_pnl_r, l_pnl_r = await asyncio.gather(
            s_ex2.get_closed_pnl(trade.symbol),
            l_ex2.get_closed_pnl(trade.symbol),
            return_exceptions=True,
        )
        await asyncio.gather(s_ex2.close(), l_ex2.close(), return_exceptions=True)
        if not isinstance(s_pnl_r, Exception) and s_pnl_r is not None:
            real_short_pnl = s_pnl_r
        if not isinstance(l_pnl_r, Exception) and l_pnl_r is not None:
            real_long_pnl = l_pnl_r
        print(f"[Executor] Real PnL from exchanges: short={real_short_pnl} long={real_long_pnl}")
    except Exception as e:
        print(f"[Executor] get_closed_pnl error: {e}")

    # Если получили реальные данные с бирж — используем их
    fees = (trade.fee_short_usd or 0) + (trade.fee_long_usd or 0)
    if real_short_pnl is not None and real_long_pnl is not None:
        trade.short_pnl_usd = round(real_short_pnl, 6)
        trade.long_pnl_usd  = round(real_long_pnl, 6)
        trade.net_pnl_usd   = round(real_short_pnl + real_long_pnl - fees, 6)
    else:
        # Fallback — считаем через нотиональную стоимость
        _calc_pnl(trade)

    update_trade(trade)

    await _snapshot_balances([trade.short_exchange, trade.long_exchange])

    pnl  = f"${trade.net_pnl_usd:+.4f}" if trade.net_pnl_usd is not None else "—"
    fees = (trade.fee_short_usd or 0) + (trade.fee_long_usd or 0)
    dur_str = ""
    try:
        t1 = datetime.fromisoformat(str(trade.opened_at))
        t2 = datetime.fromisoformat(str(trade.closed_at))
        s  = int((t2 - t1).total_seconds())
        dur_str = f"{s//3600}ч {(s%3600)//60}м {s%60}с" if s >= 3600 else f"{s//60}м {s%60}с"
    except Exception: pass

    # Фактический спред по ценам закрытия
    actual_close_spread = ""
    if trade.short_close_price and trade.long_close_price and trade.long_close_price > 0:
        cs = (trade.short_close_price / trade.long_close_price - 1) * 100
        actual_close_spread = f"\n📊 Спред закрытия: {cs:.2f}%"

    pnl_emoji = "💰" if trade.net_pnl_usd and trade.net_pnl_usd > 0 else "💸"
    msg = (f"{'✅' if trade.status=='closed' else '⚠️'} <b>Закрыто: {trade.ticker}</b>\n"
           f"📉 SHORT {trade.short_exchange.upper()} @ ${trade.short_close_price:.6f if trade.short_close_price else '—'}\n"
           f"📈 LONG  {trade.long_exchange.upper()} @ ${trade.long_close_price:.6f  if trade.long_close_price  else '—'}\n"
           f"📊 Спред входа: {trade.signal_spread_pct:.2f}%{actual_close_spread}\n"
           f"PnL short: {_upnl(trade.short_pnl_usd)} | long: {_upnl(trade.long_pnl_usd)}\n"
           f"Комиссии: ${fees:.4f}\n"
           f"{pnl_emoji} <b>Чистая прибыль: {pnl}</b>\n"
           f"⏱ Удержание: {dur_str}")
    if not (short_ok and long_ok):
        msg += f"\n⚠️ Требуется ручная проверка!"

    # 🔧 ИСПРАВЛЕНИЕ: Очищаем историю фандинга для закрытой позиции
    try:
        from gui_server import _funding_history
        _funding_history.pop(trade.id, None)
    except Exception:
        pass  # gui_server может быть не импортирован в тестах

    await _close(short_ex, long_ex)
    return trade, msg


async def check_balance_alerts(exchanges: list[str], initial_balances: dict[str, float]):
    from exchanges import get_all_balances
    current = await get_all_balances(exchanges)
    for ex, bal in current.items():
        save_balance_snapshot(ex, bal)
        init = initial_balances.get(ex)
        if init and init > 0:
            pct = bal / init * 100
            if pct <= BALANCE_ALERT_PCT:
                await notify(f"🚨 <b>АЛЕРТ: {ex.upper()}</b>\nНачальный: ${init:.2f}\nТекущий: ${bal:.2f}\nОсталось: {pct:.1f}%")


def _calc_pnl(t: Trade):
    """Считает PnL арбитражной позиции.

    Формула через нотиональную стоимость:
    SHORT PnL = size_usd * (entry_short / close_short - 1)
    LONG  PnL = size_usd * (close_long  / entry_long  - 1)

    Это корректно даже если qty на разных биржах отличается
    (например Gate контракты vs Binance монеты).
    """
    size = t.trade_size_usd or 0
    try:
        if t.short_entry_price and t.short_close_price and t.short_entry_price > 0:
            # SHORT: зарабатываем когда цена падает
            short_return = t.short_entry_price / t.short_close_price - 1
            t.short_pnl_usd = round(size * short_return, 6)
        else:
            t.short_pnl_usd = None
    except (TypeError, ZeroDivisionError):
        t.short_pnl_usd = None

    try:
        if t.long_entry_price and t.long_close_price and t.long_entry_price > 0:
            # LONG: зарабатываем когда цена растёт
            long_return = t.long_close_price / t.long_entry_price - 1
            t.long_pnl_usd = round(size * long_return, 6)
        else:
            t.long_pnl_usd = None
    except (TypeError, ZeroDivisionError):
        t.long_pnl_usd = None

    fees = (t.fee_short_usd or 0) + (t.fee_long_usd or 0)
    if t.short_pnl_usd is not None and t.long_pnl_usd is not None:
        t.net_pnl_usd = round(t.short_pnl_usd + t.long_pnl_usd - fees, 6)


async def _snapshot_balances(exchanges: list[str]):
    from exchanges import get_all_balances
    try:
        for ex, bal in (await get_all_balances(exchanges)).items():
            save_balance_snapshot(ex, bal)
    except Exception as e: print(f"[snapshot] {e}")


async def _close(*exs):
    await asyncio.gather(*[e.close() for e in exs], return_exceptions=True)


def _upnl(v): return f"${v:+.4f}" if v is not None else "—"


# ── Фандинг-монитор ───────────────────────────────────────────
# Проверяет каждые 60с: если diff фандинга > спред и идёт в убыток —
# закрывает позицию за 5 минут до начисления.

_funding_monitor_task: asyncio.Task | None = None

# Кэш фандинга: {trade_id: {'sf': rate, 'lf': rate, 'interval': h, 'updated': ts}}
_funding_cache: dict = {}
FUNDING_CACHE_TTL = 20 * 60  # обновляем каждые 20 минут


async def start_funding_monitor():
    """Запускает фоновый мониторинг фандинга для всех открытых позиций."""
    global _funding_monitor_task
    if _funding_monitor_task and not _funding_monitor_task.done():
        return
    _funding_monitor_task = asyncio.create_task(_funding_monitor_loop())
    print("[FundingMonitor] запущен")


async def _funding_monitor_loop():
    """
    Логика:
    1. Каждые 60с проверяем открытые позиции
    2. Каждые 20 минут обновляем фандинг с бирж
    3. Считаем текущий спред (по ценам с бирж)
    4. Считаем net_funding_per_period = сколько % платим/получаем за 1 период
    5. Если фандинг в убыток:
       - Считаем сколько периодов пройдёт пока фандинг "съест" спред
       - Закрываем за 5 минут до того как накопленный фандинг >= текущего спреда
    6. Всегда закрываем за 5 минут до начисления если общий убыток > спреда
    """
    import time as _time
    from db import get_all_open_trades
    from exchanges import create_exchange
    from signal_parser import CloseSignal

    while True:
        await asyncio.sleep(60)
        try:
            trades = get_all_open_trades()
            if not trades:
                continue

            for t in trades:
                try:
                    # Перевірка маржі (пріоритет над фандингом)
                    closed = await _check_margin_risk(t)
                    if not closed:
                        await _check_funding_for_trade(t)
                except Exception as e:
                    print(f"[FundingMonitor] ошибка {t.ticker}: {e}")

        except Exception as e:
            print(f"[FundingMonitor] loop error: {e}")


# Cooldown щоб не спамити попередженнями: {trade_id: timestamp}
_margin_warn_cooldown: dict[int, float] = {}
MARGIN_WARN_COOLDOWN_S = 120  # попередження не частіше ніж раз на 2 хвилини


# В order_executor.py найти функцию _check_margin_risk и заменить её:

async def _check_margin_risk(t) -> bool:
    """Двоступенева перевірка ліквідації та маржинального ризику.

        Ступінь 1 — ПОПЕРЕДЖЕННЯ (відстань до ліквідації ≤ 15%):
            Відправляємо алерт в Telegram, але НЕ закриваємо.
            Cooldown: не частіше ніж раз на 2 хвилини.

        Ступінь 2 — АВАРІЙНЕ ЗАКРИТТЯ (відстань до ліквідації ≤ 10%):
            Негайно закриваємо обидві ноги + сповіщення.

        Також резервна перевірка через unrealized_pnl (якщо get_open_position
        не повертає entry_price):
            Якщо збиток однієї ноги >= MARGIN_RISK_PCT% від маржі → закриваємо.

        Формула відстані до ліквідації (isolated margin, simplified):
            SHORT liq ≈ entry_price × (1 + 1/leverage)
            LONG  liq ≈ entry_price × (1 - 1/leverage)
            distance% = |liq_price - current_price| / current_price × 100
    """
    import time as _time
    from exchanges import create_exchange
    from signal_parser import CloseSignal

    now = _time.time()
    try:
        s_ex = create_exchange(t.short_exchange)
        l_ex = create_exchange(t.long_exchange)

        s_price_r, l_price_r, s_pos, l_pos = await asyncio.gather(
            s_ex.get_price(t.symbol),
            l_ex.get_price(t.symbol),
            s_ex.get_open_position(t.symbol),
            l_ex.get_open_position(t.symbol),
            return_exceptions=True,
        )
        await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)
    except Exception as e:
        print(f"[MarginCheck] {t.ticker}: ошибка данных: {e}")
        return False

    # --- ЛОГИКА САМОЛЕЧЕНИЯ ---
    # Если на обеих биржах позиций нет, а в базе статус open/partial - закрываем в базе
    if (s_pos is None or isinstance(s_pos, Exception)) and \
            (l_pos is None or isinstance(l_pos, Exception)):
        print(f"[MarginCheck] {t.ticker}: Позиции не найдены на биржах. Синхронизирую БД...")
        t.status = 'closed'
        t.closed_at = datetime.now(timezone.utc).isoformat()
        update_trade(t)
        return True
        # --------------------------

    s_price = float(s_price_r) if not isinstance(s_price_r, Exception) else None
    l_price = float(l_price_r) if not isinstance(l_price_r, Exception) else None

    if not s_price or not l_price:
        return False

    # ── Метод 1: відстань до ціни ліквідації ──────────────────
    # Використовуємо entry_price з позиції (точніше) або з бази (fallback)
    s_entry = None
    l_entry = None
    s_pnl   = None
    l_pnl   = None

    if not isinstance(s_pos, Exception) and s_pos:
        s_entry = float(s_pos.get('entry_price') or t.short_entry_price or 0)
        s_pnl   = float(s_pos.get('unrealized_pnl', 0))
    else:
        s_entry = float(t.short_entry_price or 0)

    if not isinstance(l_pos, Exception) and l_pos:
        l_entry = float(l_pos.get('entry_price') or t.long_entry_price or 0)
        l_pnl   = float(l_pos.get('unrealized_pnl', 0))
    else:
        l_entry = float(t.long_entry_price or 0)

    lev = float(t.leverage or LEVERAGE)

    # Розраховуємо теоретичні ціни ліквідації
    # SHORT: ціна росте → збиток → ліквідація зверху
    # LONG:  ціна падає → збиток → ліквідація знизу
    s_dist_pct = None
    l_dist_pct = None

    if s_entry and s_entry > 0 and lev > 0:
        s_liq_price = s_entry * (1 + 1 / lev)   # SHORT: ліквідація вгорі
        s_dist_pct  = (s_liq_price - s_price) / s_price * 100
        # Від'ємне = вже за межею (теоретично), мале позитивне = близько до ліквідації

    if l_entry and l_entry > 0 and lev > 0:
        l_liq_price = l_entry * (1 - 1 / lev)   # LONG: ліквідація внизу
        l_dist_pct  = (l_price - l_liq_price) / l_price * 100

    # Визначаємо найближчу до ліквідації сторону
    dists = []
    if s_dist_pct is not None:
        dists.append(('SHORT', t.short_exchange, s_dist_pct, s_entry, s_price))
    if l_dist_pct is not None:
        dists.append(('LONG',  t.long_exchange,  l_dist_pct, l_entry, l_price))

    if dists:
        min_side, min_ex, min_dist, min_entry, min_price = min(dists, key=lambda x: x[2])

        print(f"[MarginCheck] {t.ticker}: "
              f"short dist={s_dist_pct:.1f}% " if s_dist_pct is not None else "",
              f"long dist={l_dist_pct:.1f}% " if l_dist_pct is not None else "",
              f"(lev={lev:.0f}×)")

        # ── Ступінь 2: АВАРІЙНЕ ЗАКРИТТЯ ──
        if min_dist <= 10:
            print(f"[MarginCheck] 🚨 {t.ticker}: {min_side} в {min_dist:.1f}% від ліквідації! АВАРІЙНЕ ЗАКРИТТЯ.")
            await notify(
                f"🚨 <b>АВАРІЙНЕ ЗАКРИТТЯ: {t.ticker}</b>\n"
                f"⚠️ {min_side} на {min_ex.upper()} в <b>{min_dist:.1f}%</b> від ліквідації\n"
                f"📍 Ціна входу: ${min_entry:.6f} | Поточна: ${min_price:.6f}\n"
                f"📊 SHORT dist: {s_dist_pct:.1f}% | LONG dist: {l_dist_pct:.1f}%\n"
                f"⚡ Закриваю обидві позиції негайно!"
            )
            cs = CloseSignal(ticker=t.ticker, raw_text="emergency_liq")
            _, msg = await close_position(cs)
            await notify(msg)
            _funding_cache.pop(t.id, None)
            return True

        # ── Ступінь 1: ПОПЕРЕДЖЕННЯ ──
        if min_dist <= 15:
            cooldown_key = t.id
            if now - _margin_warn_cooldown.get(cooldown_key, 0) > MARGIN_WARN_COOLDOWN_S:
                _margin_warn_cooldown[cooldown_key] = now
                print(f"[MarginCheck] ⚠️ {t.ticker}: {min_side} в {min_dist:.1f}% від ліквідації! Попередження.")
                await notify(
                    f"⚠️ <b>УВАГА: {t.ticker}</b>\n"
                    f"{min_side} на {min_ex.upper()} в <b>{min_dist:.1f}%</b> від ліквідації\n"
                    f"📍 Ціна входу: ${min_entry:.6f} | Поточна: ${min_price:.6f}\n"
                    f"📊 SHORT dist: {s_dist_pct:.1f}% | LONG dist: {l_dist_pct:.1f}%\n"
                    f"ℹ️ Позиції ще відкриті — стежу далі"
                )
            return False  # не закриваємо — тільки попереджаємо

    # ── Метод 2 (резервний): unrealized_pnl як % від маржі ────
    # Спрацьовує якщо entry_price недоступний або lev=0
    if s_pnl is not None and l_pnl is not None:
        margin_per_side = (t.trade_size_usd or 0) / lev if lev > 0 else 0
        if margin_per_side > 0:
            s_loss_pct = abs(s_pnl) / margin_per_side * 100 if s_pnl < 0 else 0
            l_loss_pct = abs(l_pnl) / margin_per_side * 100 if l_pnl < 0 else 0
            total_pnl  = s_pnl + l_pnl

            print(f"[MarginCheck/PnL] {t.ticker}: margin=${margin_per_side:.2f} "
                  f"short={s_pnl:+.4f} ({s_loss_pct:.1f}%) "
                  f"long={l_pnl:+.4f} ({l_loss_pct:.1f}%)")

            if s_loss_pct >= MARGIN_RISK_PCT or l_loss_pct >= MARGIN_RISK_PCT:
                risky_side = 'SHORT' if s_loss_pct >= l_loss_pct else 'LONG'
                risky_pct  = max(s_loss_pct, l_loss_pct)
                print(f"[MarginCheck/PnL] ⚠️ {t.ticker}: {risky_side} {risky_pct:.1f}% від маржі! Закриваємо.")
                await notify(
                    f"🚨 <b>МАРЖА ВИЧЕРПУЄТЬСЯ: {t.ticker}</b>\n"
                    f"📉 SHORT ({t.short_exchange.upper()}): ${s_pnl:+.4f} ({s_loss_pct:.1f}% маржі)\n"
                    f"📈 LONG  ({t.long_exchange.upper()}): ${l_pnl:+.4f} ({l_loss_pct:.1f}% маржі)\n"
                    f"💰 Загальний PnL: ${total_pnl:+.4f}\n"
                    f"⚡ Закриваю обидві позиції!"
                )
                cs = CloseSignal(ticker=t.ticker, raw_text="margin_pnl_close")
                _, msg = await close_position(cs)
                await notify(msg)
                _funding_cache.pop(t.id, None)
                return True

    return False


async def _check_funding_for_trade(t):
    import time as _time
    from exchanges import create_exchange
    from signal_parser import CloseSignal

    now = int(_time.time())
    cache = _funding_cache.get(t.id, {})

    # Обновляем фандинг каждые 20 минут
    if now - cache.get('updated', 0) > FUNDING_CACHE_TTL:
        s_ex = create_exchange(t.short_exchange)
        l_ex = create_exchange(t.long_exchange)
        s_fund, l_fund, s_price, l_price = await asyncio.gather(
            s_ex.get_funding_rate(t.symbol),
            l_ex.get_funding_rate(t.symbol),
            s_ex.get_price(t.symbol),
            l_ex.get_price(t.symbol),
            return_exceptions=True,
        )
        await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)

        if isinstance(s_fund, Exception) or isinstance(l_fund, Exception):
            return
        if not s_fund or not l_fund:
            return

        # Интервал фандинга (берём наименьший из двух бирж)
        s_interval = s_fund.get('interval_hours', 8)
        l_interval = l_fund.get('interval_hours', 8)
        interval   = min(s_interval, l_interval)

        _funding_cache[t.id] = {
            'sf':          s_fund.get('rate', 0),
            'lf':          l_fund.get('rate', 0),
            'interval':    interval,
            'next_short':  s_fund.get('next_time', 0),
            'next_long':   l_fund.get('next_time', 0),
            'cur_short':   float(s_price) if not isinstance(s_price, Exception) else None,
            'cur_long':    float(l_price) if not isinstance(l_price, Exception) else None,
            'updated':     now,
        }
        cache = _funding_cache[t.id]
        print(f"[FundingMonitor] {t.ticker}: обновлён фандинг "
              f"short={cache['sf']*100:+.3f}% long={cache['lf']*100:+.3f}% "
              f"интервал={interval}ч")

    sf_rate  = cache.get('sf', 0)
    lf_rate  = cache.get('lf', 0)
    interval = cache.get('interval', 8)
    sp       = cache.get('cur_short')
    lp       = cache.get('cur_long')

    # Текущий спред по ценам с бирж
    if sp and lp and lp > 0:
        current_spread_pct = (sp / lp - 1) * 100
    else:
        current_spread_pct = t.signal_spread_pct or 0

    # Спред по ценам входа (реальный потенциал прибыли)
    if t.short_entry_price and t.long_entry_price and t.long_entry_price > 0:
        entry_spread_pct = (t.short_entry_price / t.long_entry_price - 1) * 100
    else:
        entry_spread_pct = t.signal_spread_pct or 0

    # Net фандинг за 1 период:
    # SHORT позиция: если sf_rate > 0 — платим, если < 0 — получаем
    # LONG позиция:  если lf_rate < 0 — платим, если > 0 — получаем
    # Итого за период (в % от позиции):
    net_fund_per_period = -(sf_rate * 100) - (lf_rate * 100)
    # net_fund_per_period > 0 = получаем фандинг (хорошо)
    # net_fund_per_period < 0 = платим фандинг (плохо)

    # Время до следующего начисления
    next_short = cache.get('next_short', 0)
    next_long  = cache.get('next_long', 0)
    next_times = [t for t in [next_short, next_long] if t > 0]
    next_fund  = min(next_times) if next_times else 0
    minutes_until = (next_fund - now) / 60 if next_fund > now else 999

    # Время в позиции
    from datetime import datetime, timezone
    try:
        opened = datetime.fromisoformat(str(t.opened_at).replace('Z', '+00:00'))
        hours_held = (datetime.now(timezone.utc) - opened).total_seconds() / 3600
    except Exception:
        hours_held = 0

    # Сколько периодов уже прошло
    periods_elapsed = hours_held / interval if interval > 0 else 0

    # Накопленный фандинг с момента входа
    accumulated_fund = net_fund_per_period * periods_elapsed

    print(f"[FundingMonitor] {t.ticker}: "
          f"entry_spread={entry_spread_pct:.2f}% cur_spread={current_spread_pct:.2f}% "
          f"net_fund/period={net_fund_per_period:+.3f}% "
          f"accumulated={accumulated_fund:+.3f}% "
          f"held={hours_held:.1f}ч next_fund={minutes_until:.0f}м")

    should_close = False
    close_reason = ""

    if net_fund_per_period < 0:
        # Фандинг идёт в убыток — нужно следить

        # Остаточная прибыль = entry_spread + накопленный фандинг
        remaining_profit = entry_spread_pct + accumulated_fund

        # Через сколько периодов фандинг съест весь спред
        if net_fund_per_period < 0:
            periods_until_loss = entry_spread_pct / abs(net_fund_per_period)
            hours_until_loss   = periods_until_loss * interval
        else:
            hours_until_loss   = 9999

        # Через сколько минут следующее начисление которое убьёт прибыль
        next_period_fund = net_fund_per_period  # следующее начисление
        after_next_profit = remaining_profit + next_period_fund

        print(f"[FundingMonitor] {t.ticker}: "
              f"remaining_profit={remaining_profit:.3f}% "
              f"after_next={after_next_profit:.3f}% "
              f"hours_until_loss={hours_until_loss:.1f}ч")

        # Закрываем за 5 минут до начисления если:
        # 1. После следующего начисления прибыль станет отрицательной
        if after_next_profit < 0 and 0 < minutes_until <= 5:
            should_close = True
            close_reason = (
                f"После следующего фандинга прибыль будет {after_next_profit:.3f}% (убыток)\n"
                f"Entry спред: {entry_spread_pct:.2f}% | Накоплено: {accumulated_fund:+.3f}%\n"
                f"Net фандинг/период: {net_fund_per_period:+.3f}%"
            )

        # 2. Накопленный фандинг уже съел >80% спреда и фандинг ещё идёт в минус
        elif accumulated_fund < -(entry_spread_pct * 0.8):
            should_close = True
            close_reason = (
                f"Фандинг съел {abs(accumulated_fund):.3f}% из {entry_spread_pct:.2f}% спреда\n"
                f"Осталось: {remaining_profit:.3f}%"
            )

        # 3. Логика из примера: фандинг -1%/ч, спред 3% → ждём 2ч и закрываем
        # Если прошло >= расчётного времени удержания (спред / |fund_rate| периодов)
        elif hours_held >= hours_until_loss * 0.9 and 0 < minutes_until <= 5:
            should_close = True
            close_reason = (
                f"Достигнуто расчётное время удержания ({hours_until_loss:.1f}ч)\n"
                f"Фандинг {net_fund_per_period:+.3f}%/{interval}ч | Спред входа {entry_spread_pct:.2f}%"
            )

    if should_close:
        print(f"[FundingMonitor] ⚠️ {t.ticker}: закрываем! {close_reason}")
        await notify(
            f"⚠️ <b>Авто-закрытие {t.ticker}</b>\n"
            f"{close_reason}\n"
            f"⏱ До фандинга: {minutes_until:.0f} мин"
        )
        cs = CloseSignal(ticker=t.ticker, raw_text="auto_close")
        _, msg = await close_position(cs)
        await notify(msg)
        # Убираем из кэша
        _funding_cache.pop(t.id, None)