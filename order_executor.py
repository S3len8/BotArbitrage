"""
core/order_executor.py — исполнение ордеров с расчётом PnL.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from exchanges.base import BaseExchange, create_exchange
from storage.db import Trade, save_trade, update_trade, get_open_trade, save_balance_snapshot
from telegram.signal_parser import OpenSignal, CloseSignal
from config.settings import LEVERAGE, BALANCE_ALERT_PCT
from notifications.telegram_notify import send_notification


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# Открытие позиции
# ============================================================

async def open_position(signal: OpenSignal, final_size_usd: float) -> tuple[Optional[Trade], str]:
    """
    Открывает арбитраж: SHORT на short_exchange, LONG на long_exchange.
    final_size_usd — уже рассчитан риск-менеджером.
    """
    short_ex = create_exchange(signal.short_exchange)
    long_ex  = create_exchange(signal.long_exchange)

    trade = Trade(
        ticker=signal.ticker,
        symbol=signal.symbol,
        short_exchange=signal.short_exchange,
        long_exchange=signal.long_exchange,
        trade_size_usd=final_size_usd,
        status='open',
        leverage=LEVERAGE,
        opened_at=_now(),
        funding_short=signal.funding_short,
        funding_long=signal.funding_long,
        signal_spread_pct=signal.spread_pct,
        signal_text=signal.raw_text[:500],
    )

    print(
        f"[Executor] OPEN {signal.ticker}: "
        f"SHORT ${final_size_usd:.2f} @ {signal.short_exchange}, "
        f"LONG ${final_size_usd:.2f} @ {signal.long_exchange} | "
        f"leverage={LEVERAGE}x"
    )

    short_res, long_res = await asyncio.gather(
        short_ex.place_market_order(signal.symbol, 'sell', final_size_usd),
        long_ex.place_market_order(signal.symbol,  'buy',  final_size_usd),
        return_exceptions=True,
    )

    short_ok = not isinstance(short_res, Exception)
    long_ok  = not isinstance(long_res,  Exception)

    if short_ok and long_ok:
        trade.short_order_id    = short_res['order_id']
        trade.short_entry_price = short_res['price']
        trade.short_qty         = short_res['qty']
        trade.fee_short_usd     = short_res.get('fee', 0)

        trade.long_order_id     = long_res['order_id']
        trade.long_entry_price  = long_res['price']
        trade.long_qty          = long_res['qty']
        trade.fee_long_usd      = long_res.get('fee', 0)

        trade.status = 'open'
        save_trade(trade)

        msg = (
            f"✅ <b>Позиция открыта: {signal.ticker}</b>\n"
            f"📉 SHORT {signal.short_exchange.upper()}: "
            f"{trade.short_qty:.6f} @ ${trade.short_entry_price:.6f}\n"
            f"📈 LONG  {signal.long_exchange.upper()}: "
            f"{trade.long_qty:.6f} @ ${trade.long_entry_price:.6f}\n"
            f"💵 Размер: ${final_size_usd:.2f} | Плечо: {LEVERAGE}x\n"
            f"📊 Спред сигнала: {signal.spread_pct:.2f}%\n"
            f"🌗 Funding short: {_fmt_pct(signal.funding_short)} | "
            f"long: {_fmt_pct(signal.funding_long)}"
        )

    elif short_ok and not long_ok:
        print(f"[Executor] Long failed ({long_res}), closing short...")
        try:
            await short_ex.close_position(signal.symbol, 'buy', short_res['qty'])
        except Exception as e:
            print(f"[Executor] WARN: failed to close short: {e}")
        trade.status = 'failed'
        save_trade(trade)
        msg = f"⚠️ <b>{signal.ticker}</b>: Long не исполнился ({long_res}). Short откатан."

    elif long_ok and not short_ok:
        print(f"[Executor] Short failed ({short_res}), closing long...")
        try:
            await long_ex.close_position(signal.symbol, 'sell', long_res['qty'])
        except Exception as e:
            print(f"[Executor] WARN: failed to close long: {e}")
        trade.status = 'failed'
        save_trade(trade)
        msg = f"⚠️ <b>{signal.ticker}</b>: Short не исполнился ({short_res}). Long откатан."

    else:
        trade.status = 'failed'
        save_trade(trade)
        msg = (
            f"❌ <b>{signal.ticker}</b>: Обе ноги не исполнились.\n"
            f"Short: {short_res}\nLong: {long_res}"
        )

    await _close_exchanges(short_ex, long_ex)
    return (trade if trade.status == 'open' else None), msg


# ============================================================
# Закрытие позиции
# ============================================================

async def close_position(close_signal: CloseSignal) -> tuple[Optional[Trade], str]:
    trade = get_open_trade(close_signal.ticker)
    if not trade:
        return None, f"ℹ️ Нет открытой позиции по <b>{close_signal.ticker}</b>."

    short_ex = create_exchange(trade.short_exchange)
    long_ex  = create_exchange(trade.long_exchange)

    short_close, long_close = await asyncio.gather(
        short_ex.close_position(trade.symbol, 'buy',  trade.short_qty or 0),
        long_ex.close_position( trade.symbol, 'sell', trade.long_qty  or 0),
        return_exceptions=True,
    )

    short_ok = not isinstance(short_close, Exception)
    long_ok  = not isinstance(long_close,  Exception)

    trade.closed_at = _now()

    if short_ok:
        trade.short_close_price    = short_close['price']
        trade.short_close_order_id = short_close['order_id']
        trade.fee_short_usd        = (trade.fee_short_usd or 0) + short_close.get('fee', 0)

    if long_ok:
        trade.long_close_price    = long_close['price']
        trade.long_close_order_id = long_close['order_id']
        trade.fee_long_usd        = (trade.fee_long_usd or 0) + long_close.get('fee', 0)

    if short_ok and long_ok:
        trade.status = 'closed'
        _calc_pnl(trade)
    else:
        trade.status = 'partial'

    update_trade(trade)

    # Снимаем балансы после закрытия
    await _snapshot_balances([trade.short_exchange, trade.long_exchange])

    pnl_str = f"${trade.net_pnl_usd:+.4f}" if trade.net_pnl_usd is not None else "—"
    status_emoji = "✅" if trade.status == 'closed' else "⚠️"

    short_close_str = f"${trade.short_close_price:.6f}" if trade.short_close_price else "—"
    long_close_str  = f"${trade.long_close_price:.6f}"  if trade.long_close_price  else "—"

    duration = ""
    if trade.opened_at and trade.closed_at:
        try:
            t1 = datetime.fromisoformat(trade.opened_at)
            t2 = datetime.fromisoformat(trade.closed_at)
            secs = int((t2 - t1).total_seconds())
            h, m = divmod(secs // 60, 60)
            duration = f"\n⏱ Время в позиции: {h}ч {m}м"
        except Exception:
            pass

    msg = (
        f"{status_emoji} <b>Позиция закрыта: {trade.ticker}</b>\n"
        f"📉 SHORT закрыт @ {short_close_str}\n"
        f"📈 LONG  закрыт @ {long_close_str}\n"
        f"━━━━━━━━━━━━\n"
        f"PnL short: {_fmt_usd(trade.short_pnl_usd)}\n"
        f"PnL long:  {_fmt_usd(trade.long_pnl_usd)}\n"
        f"Комиссии:  ${((trade.fee_short_usd or 0) + (trade.fee_long_usd or 0)):.4f}\n"
        f"━━━━━━━━━━━━\n"
        f"💰 Чистая прибыль: <b>{pnl_str}</b>"
        f"{duration}"
    )

    if not (short_ok and long_ok):
        msg += f"\n⚠️ Требуется ручная проверка! Short: {'OK' if short_ok else short_close}"

    await _close_exchanges(short_ex, long_ex)
    return trade, msg


# ============================================================
# Мониторинг балансов
# ============================================================

async def check_and_alert_balances(
    exchanges: list[str],
    initial_balances: dict[str, float],
):
    """
    Проверяет текущие балансы и отправляет алерт если баланс
    упал ниже BALANCE_ALERT_PCT% от стартового.
    Вызывается периодически из фонового цикла.
    """
    from exchanges.base import get_all_balances
    from config.settings import BALANCE_ALERT_PCT

    current = await get_all_balances(exchanges)

    for exchange, current_bal in current.items():
        save_balance_snapshot(exchange, current_bal)

        initial = initial_balances.get(exchange)
        if not initial or initial <= 0:
            continue

        pct = current_bal / initial * 100
        if pct <= BALANCE_ALERT_PCT:
            alert = (
                f"🚨 <b>АЛЕРТ БАЛАНСА: {exchange.upper()}</b>\n"
                f"Начальный: ${initial:.2f}\n"
                f"Текущий:   ${current_bal:.2f}\n"
                f"Осталось:  {pct:.1f}% от стартового\n"
                f"Порог алерта: {BALANCE_ALERT_PCT}%"
            )
            print(f"[BalanceAlert] {alert}")
            await send_notification(alert)


# ============================================================
# Helpers
# ============================================================

def _calc_pnl(trade: Trade):
    """
    PnL:
    Short: (entry - close) × qty  → прибыль при падении цены
    Long:  (close - entry) × qty  → прибыль при росте цены
    Net:   short_pnl + long_pnl - комиссии
    """
    try:
        trade.short_pnl_usd = round(
            (trade.short_entry_price - trade.short_close_price) * trade.short_qty, 6
        )
    except TypeError:
        trade.short_pnl_usd = None

    try:
        trade.long_pnl_usd = round(
            (trade.long_close_price - trade.long_entry_price) * trade.long_qty, 6
        )
    except TypeError:
        trade.long_pnl_usd = None

    fees = (trade.fee_short_usd or 0) + (trade.fee_long_usd or 0)
    if trade.short_pnl_usd is not None and trade.long_pnl_usd is not None:
        trade.net_pnl_usd = round(trade.short_pnl_usd + trade.long_pnl_usd - fees, 6)


async def _snapshot_balances(exchanges: list[str]):
    from exchanges.base import get_all_balances
    try:
        balances = await get_all_balances(exchanges)
        for ex, bal in balances.items():
            save_balance_snapshot(ex, bal)
    except Exception as e:
        print(f"[Executor] balance snapshot error: {e}")


async def _close_exchanges(*exchanges):
    await asyncio.gather(*[ex.close() for ex in exchanges], return_exceptions=True)


def _fmt_pct(v: Optional[float]) -> str:
    return f"{v*100:.4f}%" if v is not None else "—"


def _fmt_usd(v: Optional[float]) -> str:
    return f"${v:+.4f}" if v is not None else "—"
