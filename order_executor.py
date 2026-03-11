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
from notifier import notify
from settings import LEVERAGE, BALANCE_ALERT_PCT


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

    # Замеряем время каждой ноги отдельно
    t_before = _ms()
    short_r, long_r = await asyncio.gather(
        short_ex.place_market_order(signal.symbol, 'sell', final_size_usd),
        long_ex.place_market_order( signal.symbol, 'buy',  final_size_usd),
        return_exceptions=True,
    )
    t_after = _ms()

    # Время от сигнала до исполнения (обе ноги запускаются параллельно,
    # поэтому время одинаковое — это время параллельного gather)
    exec_ms = t_after - t0

    short_ok = not isinstance(short_r, Exception)
    long_ok  = not isinstance(long_r,  Exception)

    if short_ok and long_ok:
        trade.short_order_id    = short_r['order_id'];  trade.short_entry_price = short_r['price']
        trade.short_qty         = short_r['qty'];        trade.fee_short_usd     = short_r.get('fee', 0)
        trade.long_order_id     = long_r['order_id'];   trade.long_entry_price  = long_r['price']
        trade.long_qty          = long_r['qty'];         trade.fee_long_usd      = long_r.get('fee', 0)
        trade.exec_time_short_ms = exec_ms
        trade.exec_time_long_ms  = exec_ms
        save_trade(trade)
        msg = (f"✅ <b>Открыто: {signal.ticker}</b>\n"
               f"📉 SHORT {signal.short_exchange.upper()}: {trade.short_qty:.6f} @ ${trade.short_entry_price:.6f}\n"
               f"📈 LONG  {signal.long_exchange.upper()}: {trade.long_qty:.6f} @ ${trade.long_entry_price:.6f}\n"
               f"💵 Размер: ${final_size_usd:.2f} | Плечо: {LEVERAGE}×\n"
               f"📊 Спред: {signal.spread_pct:.2f}%\n"
               f"⚡ Исполнение: {exec_ms}мс от сигнала")

    elif short_ok:
        try: await short_ex.close_position(signal.symbol, 'buy', short_r['qty'])
        except Exception as e: print(f"[Executor] rollback short failed: {e}")
        trade.status = 'failed'; save_trade(trade)
        msg = f"⚠️ <b>{signal.ticker}</b>: Long не исполнился. Short откатан."

    elif long_ok:
        try: await long_ex.close_position(signal.symbol, 'sell', long_r['qty'])
        except Exception as e: print(f"[Executor] rollback long failed: {e}")
        trade.status = 'failed'; save_trade(trade)
        msg = f"⚠️ <b>{signal.ticker}</b>: Short не исполнился. Long откатан."

    else:
        trade.status = 'failed'; save_trade(trade)
        msg = f"❌ <b>{signal.ticker}</b>: Обе ноги не исполнились.\nShort: {short_r}\nLong: {long_r}"

    await _close(short_ex, long_ex)
    return (trade if trade.status == 'open' else None), msg


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
    if long_ok:
        trade.long_close_price = long_c['price']
        trade.long_close_order_id = long_c['order_id']
        trade.fee_long_usd = (trade.fee_long_usd or 0) + long_c.get('fee', 0)

    trade.status = 'closed' if (short_ok and long_ok) else 'partial'
    _calc_pnl(trade)
    update_trade(trade)

    await _snapshot_balances([trade.short_exchange, trade.long_exchange])

    pnl  = f"${trade.net_pnl_usd:+.4f}" if trade.net_pnl_usd is not None else "—"
    fees = (trade.fee_short_usd or 0) + (trade.fee_long_usd or 0)
    dur  = ""
    try:
        t1 = datetime.fromisoformat(str(trade.opened_at))
        t2 = datetime.fromisoformat(str(trade.closed_at))
        s  = int((t2 - t1).total_seconds())
        dur = f"\n⏱ {s//3600}ч {(s%3600)//60}м"
    except Exception: pass

    msg = (f"{'✅' if trade.status=='closed' else '⚠️'} <b>Закрыто: {trade.ticker}</b>\n"
           f"📉 SHORT @ ${trade.short_close_price:.6f if trade.short_close_price else '—'}\n"
           f"📈 LONG  @ ${trade.long_close_price:.6f  if trade.long_close_price  else '—'}\n"
           f"PnL short: {_upnl(trade.short_pnl_usd)} | long: {_upnl(trade.long_pnl_usd)}\n"
           f"Комиссии: ${fees:.4f}\n"
           f"💰 Чистая прибыль: <b>{pnl}</b>{dur}")
    if not (short_ok and long_ok):
        msg += f"\n⚠️ Требуется ручная проверка!"

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
    try: t.short_pnl_usd = round((t.short_entry_price - t.short_close_price) * t.short_qty, 6)
    except TypeError: t.short_pnl_usd = None
    try: t.long_pnl_usd  = round((t.long_close_price  - t.long_entry_price)  * t.long_qty,  6)
    except TypeError: t.long_pnl_usd = None
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
