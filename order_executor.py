"""
order_executor.py — открытие и закрытие арбитражных позиций.
Замеряет время исполнения каждой ноги от момента получения сигнала.

Логика фандинг-монитора (обновлено):
  1. За MINUTES_BEFORE_FUNDING минут до начисления — ВСЕГДА закрываем позицию,
     даже если есть небольшой минус по PnL.
  2. После закрытия проверяем текущий спред на биржах:
      - Если спред >= REENTRY_MIN_SPREAD_PCT (4%) → перезаходим (новый сигнал)
      - Если спред < 4% → не перезаходим
   3. Если при закрытии была прибыль (net_pnl > 0) И спред < 4% → не перезаходим.
"""

import asyncio
import math
import time
from datetime import datetime, timezone
from typing import Optional

from exchanges import create_exchange
from db import Trade, save_trade, update_trade, get_open_trade, save_balance_snapshot
from signal_parser import OpenSignal, CloseSignal
from notifier import notify, tradingview_url, edit_notify, exchange_url, notify_order
from settings import LEVERAGE, BALANCE_ALERT_PCT, MARGIN_RISK_PCT
from risk_manager import get_allocated

# ── Настройки перезахода ──────────────────────────────────────
# За сколько минут до фандинга закрывать (всегда, независимо от PnL)
MINUTES_BEFORE_FUNDING: int   = 5
# Минимальный спред для перезахода после закрытия перед фандингом
REENTRY_MIN_SPREAD_PCT: float = 3.5


def _now(): return datetime.now(timezone.utc)
def _ms():  return int(time.time() * 1000)


_pending_opens: set[str] = set()


async def open_position(signal: OpenSignal, budget_short: float, budget_long: float, signal_received_ms: int = None) -> tuple[
    Optional[Trade], str]:
    """
    Покращена функція відкриття:
    1. Налаштування маржі.
    2. Фінальна перевірка спреду (мінімум 3%).
    3. Синхронізація кількості монет/контрактів.
    4. Послідовне виконання ордерів.
    """
    ticker = signal.ticker
    if ticker in _pending_opens:
        print(f"[Executor] {ticker}: вже відкривається іншим потоком — пропускаю")
        return None, f"⏳ {ticker}: позиція вже відкривається"
    _pending_opens.add(ticker)
    t0 = signal_received_ms or _ms()
    from settings import MIN_SPREAD_PCT, AUTO_CLOSE_SPREAD_PCT

    short_ex = create_exchange(signal.short_exchange)
    long_ex = create_exchange(signal.long_exchange)

    try:
        # --- КРОК 1: НАЛАШТУВАННЯ БІРЖ (Маржа, Плечо) ---
        # Це займає 1-2 секунди, за цей час ціна може піти
        await asyncio.gather(
            short_ex._setup_symbol(signal.symbol),
            long_ex._setup_symbol(signal.symbol),
            return_exceptions=True
        )

        # --- КРОК 2: ФІНАЛЬНИЙ ГВАРД СПРЕДУ (ОСТАННЯ МИТЬ) ---
        # Отримуємо ціни ПІСЛЯ налаштувань, прямо перед входом
        s_p_raw, l_p_raw = await asyncio.gather(
            short_ex.get_price(signal.symbol),
            long_ex.get_price(signal.symbol)
        )
        s_p = float(s_p_raw)
        l_p = float(l_p_raw)
        print(f"[Executor] {ticker}: SHORT ({signal.short_exchange}) price={s_p}, LONG ({signal.long_exchange}) price={l_p}")

        current_spread = (s_p / l_p - 1) * 100 if l_p > 0 else 0
        print(f"[Guard] {ticker}: Спред перед входом {current_spread:.2f}% (сигнал: {signal.spread_pct:.2f}%)")

        # --- КРОК 2.5: ПЕРЕВІРКА ЩО SHORT > LONG (НЕГАТИВНИЙ СПРЕД) ---
        if s_p <= l_p:
            await asyncio.gather(short_ex.close(), long_ex.close())
            msg = f"⛔ {ticker}: short_price ({s_p}) <= long_price ({l_p}) — негативний спред"
            _pending_opens.discard(ticker)
            return None, msg

        # --- КРОК 3: РОЗРАХУНОК ОБСЯГУ ---
        # Кожна біржа має свій бюджет: allocated × leverage
        alloc_s = get_allocated(signal.short_exchange)
        alloc_l = get_allocated(signal.long_exchange)
        budget_s = alloc_s * LEVERAGE if alloc_s > 0 else budget_short
        budget_l = alloc_l * LEVERAGE if alloc_l > 0 else budget_long

        print(f"[Executor] {ticker}: alloc_short=${alloc_s:.2f} × {LEVERAGE} = ${budget_s:.2f}, "
              f"alloc_long=${alloc_l:.2f} × {LEVERAGE} = ${budget_l:.2f}")

        err_s = err_l = None
        try:
            inf_s = await short_ex.get_contract_info(signal.symbol)
        except Exception as e:
            err_s = str(e)
            inf_s = {}
        try:
            inf_l = await long_ex.get_contract_info(signal.symbol)
        except Exception as e:
            err_l = str(e)
            inf_l = {}

        if err_s or err_l:
            await asyncio.gather(short_ex.close(), long_ex.close())
            parts = []
            if err_s:
                parts.append(f"📌 {signal.short_exchange}: не вдалося отримати дані — {err_s}")
            if err_l:
                parts.append(f"📌 {signal.long_exchange}: не вдалося отримати дані — {err_l}")
            await notify(
                f"⚠️ <b>{ticker}: помилка отримання даних про контракт</b>\n"
                + "\n".join(parts)
            )
            msg = f"❌ {ticker}: не вдалося отримати contract_info"
            _pending_opens.discard(ticker)
            return None, msg

        mult_s = inf_s.get('multiplier', 1.0) or 1.0
        mult_l = inf_l.get('multiplier', 1.0) or 1.0
        step_s = max(inf_s.get('step', 1.0) or 1.0, 1.0)
        step_l = max(inf_l.get('step', 1.0) or 1.0, 1.0)
        min_s  = max(inf_s.get('min_qty', 1.0) or 1.0, 1.0)
        min_l  = max(inf_l.get('min_qty', 1.0) or 1.0, 1.0)

        min_s_coins = min_s * mult_s
        min_l_coins = min_l

        min_notional_s = inf_s.get('min_notional') or (min_s_coins * s_p)
        min_notional_l = inf_l.get('min_notional') or (min_l_coins * l_p)
        min_cost_s = max(min_s_coins * s_p, min_notional_s)
        min_cost_l = max(min_l_coins * l_p, min_notional_l)

        if budget_s <= 0 or budget_l <= 0:
            await asyncio.gather(short_ex.close(), long_ex.close())
            await notify(
                f"⚠️ <b>{ticker}: бюджет не визначено</b>\n"
                f"📌 {signal.short_exchange}: бюджет ${budget_s:.2f}\n"
                f"📌 {signal.long_exchange}: бюджет ${budget_l:.2f}\n"
                f"💡 Перевірте виділену маржу для бірж"
            )
            msg = f"❌ {ticker}: бюджет не визначено"
            _pending_opens.discard(ticker)
            return None, msg

        if min_cost_s > budget_s or min_cost_l > budget_l:
            await asyncio.gather(short_ex.close(), long_ex.close())
            await notify(
                f"⚠️ <b>{ticker}: мін. лот перевищує бюджет</b>\n"
                f"📌 {signal.short_exchange}: мін. позиція ${min_cost_s:.2f}, бюджет ${budget_s:.2f}\n"
                f"📌 {signal.long_exchange}: мін. позиція ${min_cost_l:.2f}, бюджет ${budget_l:.2f}\n"
                f"💡 Збільште виділену маржу для цієї біржі"
            )
            msg = f"❌ {ticker}: мін. лот перевищує бюджет"
            _pending_opens.discard(ticker)
            return None, msg

        step_s_coins = step_s * mult_s
        step_l_coins = step_l
        step_common = math.gcd(int(step_s_coins), int(step_l_coins))
        if step_common < 1:
            step_common = 1

        qty_s_raw = budget_s / (s_p * mult_s)
        qty_l_raw = budget_l / (l_p * mult_l)
        min_qty_raw = min(qty_s_raw, qty_l_raw)

        qty_common = int(min_qty_raw) // step_common * step_common

        min_lot_coins = max(min_s_coins, min_l_coins)
        if qty_common < min_lot_coins:
            qty_common = int(min_lot_coins) // step_common * step_common
            if qty_common < min_lot_coins:
                qty_common += step_common

        qty_s = qty_common
        qty_l = qty_common

        cost_s = qty_s * s_p * mult_s
        cost_l = qty_l * l_p * mult_l

        print(f"[Executor] {ticker}: qty={qty_common}, cost=${cost_s:.2f}/${cost_l:.2f}, "
              f"budget=${budget_s:.2f}/${budget_l:.2f}")

        from settings import DEPTH_MIN_RATIO
        depth_s = await short_ex.get_order_book_depth(signal.symbol, cost_s, s_p)
        depth_l = await long_ex.get_order_book_depth(signal.symbol, cost_l, l_p)
        if depth_s is not None and depth_s < cost_s / DEPTH_MIN_RATIO:
            await asyncio.gather(short_ex.close(), long_ex.close())
            await notify(
                f"⚠️ <b>{ticker}: недостаточно ликвидности (SH {signal.short_exchange})</b>\n"
                f"📊 Ордер: ${cost_s:.2f}, ликвидность: ${depth_s:.2f} ({DEPTH_MIN_RATIO}x)\n"
                f"💡 Используйте лимитные ордера"
            )
            _pending_opens.discard(ticker)
            return None, f"❌ {ticker}: недостаточно ликвидности (SH)"
        if depth_l is not None and depth_l < cost_l / DEPTH_MIN_RATIO:
            await asyncio.gather(short_ex.close(), long_ex.close())
            await notify(
                f"⚠️ <b>{ticker}: недостаточно ликвидности (LH {signal.long_exchange})</b>\n"
                f"📊 Ордер: ${cost_l:.2f}, ликвидность: ${depth_l:.2f} ({DEPTH_MIN_RATIO}x)\n"
                f"💡 Используйте лимитные ордера"
            )
            _pending_opens.discard(ticker)
            return None, f"❌ {ticker}: недостаточно ликвидности (LH)"

        exec_size_s = cost_s
        exec_size_l = cost_l

        # --- КРОК 4: ВИКОНАННЯ ОРДЕРІВ ---
        trade = Trade(
            ticker=signal.ticker, symbol=signal.symbol,
            short_exchange=signal.short_exchange, long_exchange=signal.long_exchange,
            trade_size_usd=exec_size_s, status='open', leverage=LEVERAGE,
            opened_at=_now(),
            funding_short=signal.funding_short, funding_long=signal.funding_long,
            signal_spread_pct=signal.spread_pct, signal_text=signal.raw_text[:500],
        )

        short_r: dict | Exception = Exception("not started")
        long_r: dict | Exception = Exception("not started")

        # 1. Short
        try:
            short_r = await asyncio.wait_for(
                short_ex.place_market_order(signal.symbol, 'sell', exec_size_s),
                timeout=20
            )
        except Exception as e:
            short_r = e

        if isinstance(short_r, Exception):
            trade.status = 'failed'
            save_trade(trade)
            msg = f"❌ {signal.ticker}: Short помилка ({signal.short_exchange}): {short_r}"
            await asyncio.gather(short_ex.close(), long_ex.close())
            await notify(msg)
            _pending_opens.discard(ticker)
            return None, msg

        # 2. Long
        try:
            long_r = await asyncio.wait_for(
                long_ex.place_market_order(signal.symbol, 'buy', exec_size_l),
                timeout=20
            )
        except Exception as e:
            long_r = e

        t_after = _ms()
        exec_ms = t_after - t0

        # Якщо Long не відкрився - відкат Short
        if isinstance(long_r, Exception):
            try:
                await short_ex.close_position(signal.symbol, 'buy', short_r['qty'])
                rollback = "Short откатан ✅"
            except Exception as re:
                rollback = f"Short відкат ПРОВАЛИВСЯ ⚠️: {re}"

            trade.status = 'failed'
            save_trade(trade)
            msg = f"❌ {signal.ticker}: Long помилка ({signal.long_exchange}): {long_r}\n{rollback}"
            await asyncio.gather(short_ex.close(), long_ex.close())
            await notify(msg)
            _pending_opens.discard(ticker)
            return None, msg

        # --- КРОК 5: ЗБЕРЕЖЕННЯ ТА ПОВІДОМЛЕННЯ ---
        trade.short_order_id = short_r['order_id'];
        trade.short_entry_price = short_r['price']
        trade.short_qty = short_r['qty'];
        trade.fee_short_usd = short_r.get('fee', 0)
        trade.long_order_id = long_r['order_id'];
        trade.long_entry_price = long_r['price']
        trade.long_qty = long_r['qty'];
        trade.fee_long_usd = long_r.get('fee', 0)
        trade.exec_time_short_ms = exec_ms
        trade.exec_time_long_ms = exec_ms

        # ПЕРЕВІРКА: фактичний спред після філів ордерів (толерантність 0.2%)
        actual_spread = (trade.short_entry_price / trade.long_entry_price - 1) * 100
        if actual_spread < AUTO_CLOSE_SPREAD_PCT:
            print(f"[Executor] {signal.ticker}: фактичний спред {actual_spread:.2f}% < {AUTO_CLOSE_SPREAD_PCT}% — негайне закриття!")
            await notify(
                f"⛔ <b>{signal.ticker}: скасовано — фактичний спред {actual_spread:.2f}%</b>\n"
                f"📊 Спред сигналу: {signal.spread_pct:.2f}% | Факт: {actual_spread:.2f}%\n"
                f"⚡ Slippage занадто великий — закриваю"
            )
            # Закриваємо обидві позиції
            try:
                await short_ex.close_position(signal.symbol, 'buy', trade.short_qty)
            except Exception as e:
                print(f"[Executor] Помилка відкату Short: {e}")
            try:
                await long_ex.close_position(signal.symbol, 'sell', trade.long_qty)
            except Exception as e:
                print(f"[Executor] Помилка відкату Long: {e}")
            await asyncio.gather(short_ex.close(), long_ex.close())
            trade.status = 'failed'
            save_trade(trade)
            return None, f"Фактичний спред {actual_spread:.2f}% < {AUTO_CLOSE_SPREAD_PCT}%"

        save_trade(trade)

        # Логування в канал ордерів
        sizes_usd = {'short': exec_size_s, 'long': exec_size_l}
        for side, res, ex_name in [('short', short_r, signal.short_exchange), ('long', long_r, signal.long_exchange)]:
            asyncio.create_task(notify_order(
                ex_name, side, signal.symbol, res['price'], res['qty'],
                sizes_usd[side], res['order_id'], f"↔️ Pair: {signal.ticker} | Trade #{trade.id}"
            ))

        actual_spread = (trade.short_entry_price / trade.long_entry_price - 1) * 100

        tv_url = tradingview_url(signal.short_exchange, signal.long_exchange, signal.ticker)
        short_url = exchange_url(signal.short_exchange, signal.ticker)
        long_url = exchange_url(signal.long_exchange, signal.ticker)
        ex_buttons = [{"text": f"📉 {signal.short_exchange.upper()}", "url": short_url},
                      {"text": f"📈 {signal.long_exchange.upper()}", "url": long_url}]

        def _build_msg(live_sp: float = None) -> str:
            msg = (f"✅ <b>Відкрито: {signal.ticker}</b>\n"
                   f"📉 SHORT {signal.short_exchange.upper()}: {trade.short_qty} @ ${trade.short_entry_price:.6f}\n"
                   f"📈 LONG  {signal.long_exchange.upper()}: {trade.long_qty} @ ${trade.long_entry_price:.6f}\n"
                   f"📊 Спред: {signal.spread_pct:.2f}% | Факт: {actual_spread:.2f}%")
            if live_sp: msg += f"\n📡 Поточний: {live_sp:.2f}%"
            msg += f"\n⚡ Виконання: {exec_ms}мс"
            return msg

        await asyncio.gather(short_ex.close(), long_ex.close())
        msg_id = await notify(_build_msg(), tv_url=tv_url, buttons=ex_buttons)

        # Live оновлення спреду
        async def _live_spread_updater():
            if not msg_id: return
            for _ in range(10):
                await asyncio.sleep(30)
                try:
                    from db import get_trade_by_id
                    t_check = get_trade_by_id(trade.id)
                    if not t_check or t_check.status != 'open': break
                    s_ex = create_exchange(signal.short_exchange)
                    l_ex = create_exchange(signal.long_exchange)
                    sp, lp = await asyncio.gather(s_ex.get_price(signal.symbol), l_ex.get_price(signal.symbol))
                    await asyncio.gather(s_ex.close(), l_ex.close())
                    await edit_notify(msg_id, _build_msg((sp / lp - 1) * 100), tv_url=tv_url, buttons=ex_buttons)
                except:
                    break

        asyncio.create_task(_live_spread_updater())
        return trade, _build_msg()

    except Exception as e:
        print(f"[Executor] Критична помилка: {e}")
        await asyncio.gather(short_ex.close(), long_ex.close(), return_exceptions=True)
        return None, str(e)


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
        trade.short_close_price    = short_c['price']
        trade.short_close_order_id = short_c['order_id']
        trade.fee_short_usd        = (trade.fee_short_usd or 0) + short_c.get('fee', 0)
        asyncio.create_task(notify_order(
            trade.short_exchange, 'buy (close short)', trade.symbol,
            short_c['price'], trade.short_qty or 0, trade.trade_size_usd or 0,
            short_c['order_id'], f"🔒 CLOSE | Trade #{trade.id} <b>{trade.ticker}</b>"
        ))
    if long_ok:
        trade.long_close_price    = long_c['price']
        trade.long_close_order_id = long_c['order_id']
        trade.fee_long_usd        = (trade.fee_long_usd or 0) + long_c.get('fee', 0)
        asyncio.create_task(notify_order(
            trade.long_exchange, 'sell (close long)', trade.symbol,
            long_c['price'], trade.long_qty or 0, trade.trade_size_usd or 0,
            long_c['order_id'], f"🔒 CLOSE | Trade #{trade.id} <b>{trade.ticker}</b>"
        ))

    trade.status = 'closed' if (short_ok and long_ok) else 'partial'

    # Спред на момент закрытия
    if trade.short_close_price and trade.long_close_price and trade.short_close_price > 0:
        trade.exit_spread_pct = round(
            (trade.short_close_price / trade.long_close_price - 1) * 100, 4
        )

    update_trade(trade)

    # Получаем полные данные закрытия с бирж
    await asyncio.sleep(0.5)
    short_data = None
    long_data  = None
    try:
        s_ex2 = create_exchange(trade.short_exchange)
        l_ex2 = create_exchange(trade.long_exchange)
        s_cd, l_cd = await asyncio.gather(
            s_ex2.get_close_data(trade.symbol),
            l_ex2.get_close_data(trade.symbol),
            return_exceptions=True,
        )
        await asyncio.gather(s_ex2.close(), l_ex2.close(), return_exceptions=True)
        if not isinstance(s_cd, Exception) and s_cd:
            short_data = s_cd
        if not isinstance(l_cd, Exception) and l_cd:
            long_data = l_cd
        print(f"[Executor] Close data from exchanges: short={short_data} long={long_data}")
    except Exception as e:
        print(f"[Executor] get_close_data error: {e}")

    # Обновляем trade данными с биржи
    fees = 0.0
    if short_data:
        trade.short_pnl_usd = round(short_data.get('realized_pnl', 0) or 0, 6)
        trade.short_close_time = short_data.get('close_time')
        trade.short_close_price = short_data.get('close_price') or trade.short_close_price
        trade.fee_short_usd = round(short_data.get('fee', 0) or 0, 6)
        fees += trade.fee_short_usd or 0
    if long_data:
        trade.long_pnl_usd = round(long_data.get('realized_pnl', 0) or 0, 6)
        trade.long_close_time = long_data.get('close_time')
        trade.long_close_price = long_data.get('close_price') or trade.long_close_price
        trade.fee_long_usd = round(long_data.get('fee', 0) or 0, 6)
        fees += trade.fee_long_usd or 0

    if short_data and long_data:
        # Exchange PnL уже включает комиссии
        trade.net_pnl_usd = round(
            (trade.short_pnl_usd or 0) + (trade.long_pnl_usd or 0), 6
        )
    elif short_data or long_data:
        _calc_pnl(trade)
        if trade.short_pnl_usd is not None and trade.long_pnl_usd is not None:
            trade.net_pnl_usd = round(trade.short_pnl_usd + trade.long_pnl_usd - fees, 6)
    else:
        _calc_pnl(trade)
        if trade.short_pnl_usd is not None and trade.long_pnl_usd is not None:
            trade.net_pnl_usd = round(trade.short_pnl_usd + trade.long_pnl_usd - fees, 6)

    update_trade(trade)

    await _snapshot_balances([trade.short_exchange, trade.long_exchange])

    pnl  = f"${trade.net_pnl_usd:+.4f}" if trade.net_pnl_usd is not None else "—"
    dur_str = ""
    try:
        t1 = datetime.fromisoformat(str(trade.opened_at))
        t2 = datetime.fromisoformat(str(trade.closed_at))
        s  = int((t2 - t1).total_seconds())
        dur_str = f"{s//3600}ч {(s%3600)//60}м {s%60}с" if s >= 3600 else f"{s//60}м {s%60}с"
    except Exception:
        pass

    actual_close_spread = ""
    if trade.short_close_price and trade.long_close_price and trade.long_close_price > 0:
        cs = (trade.short_close_price / trade.long_close_price - 1) * 100
        actual_close_spread = f"\n📊 Спред закрытия: {cs:.2f}%"

    pnl_emoji = "💰" if trade.net_pnl_usd and trade.net_pnl_usd > 0 else "💸"
    short_close_str = f"${trade.short_close_price:.6f}" if trade.short_close_price else "—"
    long_close_str  = f"${trade.long_close_price:.6f}" if trade.long_close_price else "—"
    msg = (f"{'✅' if trade.status=='closed' else '⚠️'} <b>Закрыто: {trade.ticker}</b>\n"
           f"📉 SHORT {trade.short_exchange.upper()} @ {short_close_str}\n"
           f"📈 LONG  {trade.long_exchange.upper()} @ {long_close_str}\n"
           f"📊 Спред входа: {trade.signal_spread_pct:.2f}%{actual_close_spread}\n"
           f"PnL short: {_upnl(trade.short_pnl_usd)} | long: {_upnl(trade.long_pnl_usd)}\n"
           f"Комиссии: ${fees:.4f}\n"
           f"{pnl_emoji} <b>Чистая прибыль: {pnl}</b>\n"
           f"⏱ Удержание: {dur_str}")
    if not (short_ok and long_ok):
        msg += f"\n⚠️ Требуется ручная проверка!"

    try:
        from gui_server import _funding_history
        _funding_history.pop(trade.id, None)
    except Exception:
        pass

    await _close(short_ex, long_ex)
    _pending_opens.discard(ticker)
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


async def _update_trade_with_close_data(t: Trade):
    """Получает close_data с бирж и сохраняет в БД."""
    from exchanges import create_exchange
    s_ex = create_exchange(t.short_exchange)
    l_ex = create_exchange(t.long_exchange)
    try:
        s_cd, l_cd = await asyncio.gather(
            s_ex.get_close_data(t.symbol),
            l_ex.get_close_data(t.symbol),
            return_exceptions=True,
        )
        await asyncio.gather(s_ex.close(), l_ex.close())
    except Exception as e:
        print(f"[CloseData] {t.ticker}: ошибка получения close_data: {e}")
        s_cd, l_cd = None, None

    fees = 0.0
    if not isinstance(s_cd, Exception) and s_cd:
        pnl = s_cd.get('realized_pnl', 0) or 0
        t.short_pnl_usd = round(pnl, 6)
        t.short_close_price = s_cd.get('close_price') or t.short_close_price
        t.short_close_time = s_cd.get('close_time')
        t.fee_short_usd = round(s_cd.get('fee', 0) or 0, 6)
        fees += t.fee_short_usd or 0
    if not isinstance(l_cd, Exception) and l_cd:
        pnl = l_cd.get('realized_pnl', 0) or 0
        t.long_pnl_usd = round(pnl, 6)
        t.long_close_price = l_cd.get('close_price') or t.long_close_price
        t.long_close_time = l_cd.get('close_time')
        t.fee_long_usd = round(l_cd.get('fee', 0) or 0, 6)
        fees += t.fee_long_usd or 0

    if t.short_pnl_usd is not None and t.long_pnl_usd is not None:
        t.net_pnl_usd = round(t.short_pnl_usd + t.long_pnl_usd, 6)
    elif t.short_pnl_usd is None or t.long_pnl_usd is None:
        _calc_pnl(t)

    update_trade(t)
    print(f"[CloseData] {t.ticker}: сохранено в БД ✅ (short={t.short_pnl_usd} long={t.long_pnl_usd} net={t.net_pnl_usd})")


def _calc_pnl(t: Trade):
    """Считает PnL через нотиональную стоимость."""
    size = t.trade_size_usd or 0
    try:
        if t.short_entry_price and t.short_close_price and t.short_entry_price > 0:
            short_return = t.short_entry_price / t.short_close_price - 1
            t.short_pnl_usd = round(size * short_return, 6)
        else:
            t.short_pnl_usd = None
    except (TypeError, ZeroDivisionError):
        t.short_pnl_usd = None

    try:
        if t.long_entry_price and t.long_close_price and t.long_entry_price > 0:
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
    except Exception as e:
        print(f"[snapshot] {e}")


async def _close(*exs):
    await asyncio.gather(*[e.close() for e in exs], return_exceptions=True)


def _upnl(v): return f"${v:+.4f}" if v is not None else "—"


# ── Перезаход после закрытия перед фандингом ─────────────────

async def _try_reentry(trade: Trade, current_spread_pct: float):
    """
    Пытается перезайти в ту же пару после закрытия перед фандингом.

    Логика:
      - Если спред < REENTRY_MIN_SPREAD_PCT → не заходим
      - Если спред >= REENTRY_MIN_SPREAD_PCT → перезаходим всегда
        (независимо от того, была прибыль или убыток при закрытии)
    """
    if current_spread_pct < REENTRY_MIN_SPREAD_PCT:
        print(f"[Reentry] {trade.ticker}: НЕ перезаходим — спред {current_spread_pct:.2f}% < {REENTRY_MIN_SPREAD_PCT}%")
        await notify(
            f"🔄 <b>Перезаход {trade.ticker}: отменён</b>\n"
            f"📊 Текущий спред: {current_spread_pct:.2f}%\n"
            f"⛔ Минимум для входа: {REENTRY_MIN_SPREAD_PCT}%"
        )
        return

    # Спред достаточен — перезаходим через signal_parser + risk_manager + open_position
    print(f"[Reentry] {trade.ticker}: перезаходим, спред {current_spread_pct:.2f}%")

    try:
        from signal_parser import OpenSignal, normalize_symbol
        from risk_manager import check_signal
        from exchanges import create_exchange as _ce

        # Получаем актуальные цены и фандинг для нового сигнала
        s_ex = _ce(trade.short_exchange)
        l_ex = _ce(trade.long_exchange)
        try:
            s_price, l_price, s_fund, l_fund = await asyncio.gather(
                s_ex.get_price(trade.symbol),
                l_ex.get_price(trade.symbol),
                s_ex.get_funding_rate(trade.symbol),
                l_ex.get_funding_rate(trade.symbol),
                return_exceptions=True,
            )
        finally:
            await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)

        sp = float(s_price) if not isinstance(s_price, Exception) else 0.0
        lp = float(l_price) if not isinstance(l_price, Exception) else 0.0
        sf = s_fund.get('rate') if (not isinstance(s_fund, Exception) and s_fund) else None
        lf = l_fund.get('rate') if (not isinstance(l_fund, Exception) and l_fund) else None
        i_s = s_fund.get('interval_hours') if (not isinstance(s_fund, Exception) and s_fund) else None
        i_l = l_fund.get('interval_hours') if (not isinstance(l_fund, Exception) and l_fund) else None

        reentry_signal = OpenSignal(
            ticker=trade.ticker,
            symbol=trade.symbol,
            short_exchange=trade.short_exchange,
            long_exchange=trade.long_exchange,
            short_price=sp,
            long_price=lp,
            spread_pct=current_spread_pct,
            funding_short=sf,
            funding_long=lf,
            max_size_short=None,
            max_size_long=None,
            interval_short=i_s,
            interval_long=i_l,
            raw_text=f"[auto-reentry after funding close] {trade.ticker}",
        )

        # Проверяем через risk_manager
        s_ex2 = _ce(trade.short_exchange)
        l_ex2 = _ce(trade.long_exchange)
        try:
            risk = await asyncio.wait_for(
                check_signal(reentry_signal, s_ex2, l_ex2),
                timeout=30
            )
        finally:
            await asyncio.gather(s_ex2.close(), l_ex2.close(), return_exceptions=True)

        if not risk:
            print(f"[Reentry] {trade.ticker}: risk_manager отклонил — {risk.reason}")
            if 'черном списке' not in (risk.reason or '').lower():
                await notify(
                    f"🔄 <b>Перезаход {trade.ticker}: отклонён риск-менеджером</b>\n"
                    f"📊 Спред: {current_spread_pct:.2f}%\n"
                    f"⛔ Причина: {risk.reason}"
                )
            return

        await notify(
            f"🔄 <b>Перезаход {trade.ticker}</b>\n"
            f"📊 Текущий спред: {current_spread_pct:.2f}%\n"
            f"💵 SH: ${risk.final_size_short:.2f} / LH: ${risk.final_size_long:.2f}\n"
            f"⚡ Открываю новую позицию..."
        )

        new_trade, open_msg = await open_position(reentry_signal, risk.final_size_short, risk.final_size_long)
        if new_trade:
            print(f"[Reentry] {trade.ticker}: успешно перезашли, trade_id={new_trade.id}")
        else:
            print(f"[Reentry] {trade.ticker}: перезаход не удался")

    except asyncio.TimeoutError:
        print(f"[Reentry] {trade.ticker}: timeout при проверке риска")
        await notify(f"⏱ <b>Перезаход {trade.ticker}: таймаут</b>\nПроверь позицию вручную")
    except Exception as e:
        print(f"[Reentry] {trade.ticker}: ошибка — {e}")
        await notify(f"❌ <b>Ошибка перезахода {trade.ticker}</b>\n{e}")


# ── Фандинг-монитор ───────────────────────────────────────────

_funding_monitor_task: asyncio.Task | None = None

# Кэш фандинга: {trade_id: {'sf': rate, 'lf': rate, 'interval': h, 'updated': ts}}
_funding_cache: dict = {}
FUNDING_CACHE_TTL = 20 * 60  # обновляем каждые 20 минут

# Проверка техработ Binance каждые 15 минут
MAINTENANCE_CHECK_SEC = 15 * 60
_maintenance_check_counter = 0


async def start_funding_monitor():
    """Запускает фоновый мониторинг фандинга для всех открытых позиций."""
    global _funding_monitor_task
    if _funding_monitor_task and not _funding_monitor_task.done():
        return
    _funding_monitor_task = asyncio.create_task(_funding_monitor_loop())
    print("[FundingMonitor] запущен")


async def _funding_monitor_loop():
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
                    closed = await _check_margin_risk(t)
                    if not closed:
                        await _check_funding_for_trade(t)
                except Exception as e:
                    print(f"[FundingMonitor] ошибка {t.ticker}: {e}")

            # Каждые 15 минут проверяем техработы Binance
            global _maintenance_check_counter
            _maintenance_check_counter += 60
            if _maintenance_check_counter >= MAINTENANCE_CHECK_SEC:
                _maintenance_check_counter = 0
                await _check_binance_maintenance(trades)

        except Exception as e:
            print(f"[FundingMonitor] loop error: {e}")


async def _check_binance_maintenance(trades):
    """Проверяет техработы Binance для открытых позиций."""
    import requests
    global _notified_binance_maint

    try:
        r = requests.get(
            'https://api.binance.com/fapi/v1/exchangeInfo',
            params={'symbol': 'BTCUSDT'},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()

        # tradingStatus в symbols: "TRADING" = можно торговать, иначе техработы
        trading_ok = True
        maint_symbols = []

        for sym in data.get('symbols', []):
            if sym.get('symbol', '').endswith('USDT'):
                status = sym.get('status', '')
                contract = sym.get('contractType', '')
                if status != 'TRADING' and contract == 'PERPETUAL':
                    maint_symbols.append(sym['symbol'])

        # Находим позиции с Binance
        binance_trades = [t for t in trades
                         if t.short_exchange == 'binance' or t.long_exchange == 'binance']

        if not binance_trades:
            return

        for t in binance_trades:
            ticker = t.ticker.upper()
            binance_tickers = [f"{ticker}USDT", f"{ticker}PERP"]

            for bt in binance_tickers:
                if bt in maint_symbols:
                    print(f"[BinanceMaintenance] ⚠️ {t.ticker} ({bt}): техработы на Binance!")
                    await notify(
                        f"🚧 <b>Binance: техработы для {t.ticker}</b>\n"
                        f"⏰ Символ: {bt}\n"
                        f"📊 Статус: {'⚠️ ТОРГИ ОСТАНОВЛЕНЫ'}\n"
                        f"💡 Рассмотрите возможность закрыть позицию вручную."
                    )
                    break

    except Exception as e:
        print(f"[BinanceMaintenance] ошибка проверки: {e}")


# Cooldown предупреждений: {trade_id: timestamp}
_margin_warn_cooldown: dict[int, float] = {}
MARGIN_WARN_COOLDOWN_S = 120


async def _check_margin_risk(t) -> bool:
    """Быстрая проверка: позиция есть? orphan leg? ликвидация?"""
    import time as _time
    from exchanges import create_exchange

    try:
        s_ex = create_exchange(t.short_exchange)
        l_ex = create_exchange(t.long_exchange)

        s_pos, l_pos, s_price_r, l_price_r = await asyncio.gather(
            s_ex.get_open_position(t.symbol),
            l_ex.get_open_position(t.symbol),
            s_ex.get_price(t.symbol),
            l_ex.get_price(t.symbol),
            return_exceptions=True,
        )
        await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)
    except Exception as e:
        print(f"[MarginCheck] {t.ticker}: ошибка данных: {e}")
        return False

    s_on = not isinstance(s_pos, Exception) and s_pos and s_pos.get('size', 0) > 0
    l_on = not isinstance(l_pos, Exception) and l_pos and l_pos.get('size', 0) > 0

    # Обе закрыты — самолечение
    if not s_on and not l_on:
        print(f"[MarginCheck] {t.ticker}: обе ноги закрыты ✅")
        t.status = 'closed'
        t.closed_at = datetime.now(timezone.utc).isoformat()
        _update_trade_with_close_data(t)
        return True

    # Orphan leg — сразу закрываем вторую ногу
    if s_on and not l_on:
        print(f"[MarginCheck] 🚨 {t.ticker}: LONG закрыта на бирже, закрываю SHORT!")
        await notify(f"🚨 <b>Orphan leg: {t.ticker}</b>\nLONG закрылась на бирже, закрываю SHORT...")
        s_ex2 = create_exchange(t.short_exchange)
        try:
            await s_ex2.close_position(t.symbol, 'buy', t.short_qty or 0)
        finally:
            await s_ex2.close()
        _update_trade_with_close_data(t)
        return True

    if l_on and not s_on:
        print(f"[MarginCheck] 🚨 {t.ticker}: SHORT закрыта на бирже, закрываю LONG!")
        await notify(f"🚨 <b>Orphan leg: {t.ticker}</b>\nSHORT закрылась на бирже, закрываю LONG...")
        l_ex2 = create_exchange(t.long_exchange)
        try:
            await l_ex2.close_position(t.symbol, 'sell', t.long_qty or 0)
        finally:
            await l_ex2.close()
        _update_trade_with_close_data(t)
        return True

    s_price = float(s_price_r) if not isinstance(s_price_r, Exception) else None
    l_price = float(l_price_r) if not isinstance(l_price_r, Exception) else None
    now = int(_time.time())

    if not s_price or not l_price:
        return False

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

    s_dist_pct = None
    l_dist_pct = None

    if s_entry and s_entry > 0 and lev > 0:
        s_liq_price = s_entry * (1 + 1 / lev)
        s_dist_pct  = (s_liq_price - s_price) / s_price * 100

    if l_entry and l_entry > 0 and lev > 0:
        l_liq_price = l_entry * (1 - 1 / lev)
        l_dist_pct  = (l_price - l_liq_price) / l_price * 100

    dists = []
    if s_dist_pct is not None:
        dists.append(('SHORT', t.short_exchange, s_dist_pct, s_entry, s_price))
    if l_dist_pct is not None:
        dists.append(('LONG',  t.long_exchange,  l_dist_pct, l_entry, l_price))

    if dists:
        min_side, min_ex, min_dist, min_entry, min_price = min(dists, key=lambda x: x[2])

        print(f"[MarginCheck] {t.ticker}: "
              f"short_dist={s_dist_pct:.1f}% long_dist={l_dist_pct:.1f}% (lev={lev:.0f}×)")

        # Ступень 2: аварийное закрытие
        if min_dist <= 5:
            print(f"[MarginCheck] 🚨 {t.ticker}: {min_side} в {min_dist:.1f}% от ликвидации! АВАРИЙНОЕ ЗАКРЫТИЕ.")
            await notify(
                f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ: {t.ticker}</b>\n"
                f"⚠️ {min_side} на {min_ex.upper()} в <b>{min_dist:.1f}%</b> от ликвидации\n"
                f"📍 Цена входа: ${min_entry:.6f} | Текущая: ${min_price:.6f}\n"
                f"📊 SHORT dist: {s_dist_pct:.1f}% | LONG dist: {l_dist_pct:.1f}%\n"
                f"⚡ Закрываю обе позиции немедленно!"
            )
            cs = CloseSignal(ticker=t.ticker, raw_text="emergency_liq")
            _, msg = await close_position(cs)
            await notify(msg)
            _funding_cache.pop(t.id, None)
            return True

        # Ступень 1: предупреждение
        if min_dist <= 10:
            cooldown_key = t.id
            if now - _margin_warn_cooldown.get(cooldown_key, 0) > MARGIN_WARN_COOLDOWN_S:
                _margin_warn_cooldown[cooldown_key] = now
                print(f"[MarginCheck] ⚠️ {t.ticker}: {min_side} в {min_dist:.1f}% от ликвидации! Предупреждение.")
                await notify(
                    f"⚠️ <b>ВНИМАНИЕ: {t.ticker}</b>\n"
                    f"{min_side} на {min_ex.upper()} в <b>{min_dist:.1f}%</b> от ликвидации\n"
                    f"📍 Цена входа: ${min_entry:.6f} | Текущая: ${min_price:.6f}\n"
                    f"📊 SHORT dist: {s_dist_pct:.1f}% | LONG dist: {l_dist_pct:.1f}%\n"
                    f"ℹ️ Позиции ещё открыты — слежу дальше"
                )
            return False

    # Резервный метод: unrealized_pnl как % от маржи
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
                print(f"[MarginCheck/PnL] ⚠️ {t.ticker}: {risky_side} {risky_pct:.1f}% от маржи! Закрываем.")
                await notify(
                    f"🚨 <b>МАРЖА ИСЧЕРПЫВАЕТСЯ: {t.ticker}</b>\n"
                    f"📉 SHORT ({t.short_exchange.upper()}): ${s_pnl:+.4f} ({s_loss_pct:.1f}% маржи)\n"
                    f"📈 LONG  ({t.long_exchange.upper()}): ${l_pnl:+.4f} ({l_loss_pct:.1f}% маржи)\n"
                    f"💰 Общий PnL: ${total_pnl:+.4f}\n"
                    f"⚡ Закрываю обе позиции!"
                )
                cs = CloseSignal(ticker=t.ticker, raw_text="margin_pnl_close")
                _, msg = await close_position(cs)
                await notify(msg)
                _funding_cache.pop(t.id, None)
                return True

    return False


async def _check_funding_for_trade(t):
    """
    Проверяет фандинг для открытой позиции.

    НОВАЯ ЛОГИКА:
    - За MINUTES_BEFORE_FUNDING минут до начисления — ВСЕГДА закрываем,
      даже если есть небольшой минус.
    - После закрытия проверяем текущий спред:
      * спред >= REENTRY_MIN_SPREAD_PCT → перезаход
      * спред < REENTRY_MIN_SPREAD_PCT → не перезаходим
    - Если была прибыль (net_pnl > 0) и спред < порога → не перезаходим.
    """
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

        s_interval = s_fund.get('interval_hours', 8)
        l_interval = l_fund.get('interval_hours', 8)
        interval   = min(s_interval, l_interval)

        _funding_cache[t.id] = {
            'sf':         s_fund.get('rate', 0),
            'lf':         l_fund.get('rate', 0),
            'interval':   interval,
            'next_short': s_fund.get('next_time', 0),
            'next_long':  l_fund.get('next_time', 0),
            'cur_short':  float(s_price) if not isinstance(s_price, Exception) else None,
            'cur_long':   float(l_price) if not isinstance(l_price, Exception) else None,
            'updated':    now,
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

    # Время до следующего начисления
    next_short = cache.get('next_short', 0)
    next_long  = cache.get('next_long', 0)
    next_times = [nt for nt in [next_short, next_long] if nt > 0]
    next_fund  = min(next_times) if next_times else 0
    minutes_until = (next_fund - now) / 60 if next_fund > now else 999

    # Текущий спред по ценам с бирж
    if sp and lp and lp > 0:
        current_spread_pct = (sp / lp - 1) * 100
    else:
        current_spread_pct = t.signal_spread_pct or 0

    # Спред по ценам входа
    if t.short_entry_price and t.long_entry_price and t.long_entry_price > 0:
        entry_spread_pct = (t.short_entry_price / t.long_entry_price - 1) * 100
    else:
        entry_spread_pct = t.signal_spread_pct or 0

    # Net фандинг за 1 период
    net_fund_per_period = -(sf_rate * 100) - (lf_rate * 100)

    # Время в позиции
    try:
        opened = datetime.fromisoformat(str(t.opened_at).replace('Z', '+00:00'))
        hours_held = (datetime.now(timezone.utc) - opened).total_seconds() / 3600
    except Exception:
        hours_held = 0

    periods_elapsed    = hours_held / interval if interval > 0 else 0
    accumulated_fund   = net_fund_per_period * periods_elapsed
    remaining_profit   = entry_spread_pct + accumulated_fund
    after_next_profit  = remaining_profit + net_fund_per_period

    print(f"[FundingMonitor] {t.ticker}: "
          f"entry_spread={entry_spread_pct:.2f}% cur_spread={current_spread_pct:.2f}% "
          f"net_fund/period={net_fund_per_period:+.3f}% "
          f"accumulated={accumulated_fund:+.3f}% "
          f"remaining={remaining_profit:.3f}% "
          f"next_fund={minutes_until:.0f}м")

    # ── НОВАЯ ЛОГИКА: закрываем ВСЕГДА за N минут до фандинга ────
    #
    # Условие: до следующего фандинга осталось <= MINUTES_BEFORE_FUNDING минут
    # Не важно, есть ли убыток или нет — закрываем всегда.
    # После закрытия: перезаход если спред >= REENTRY_MIN_SPREAD_PCT.
    #
    # Дополнительно оставляем старую логику для случаев когда фандинг
    # "съедает" прибыль ещё до ближайшего начисления.
    # ─────────────────────────────────────────────────────────────

    should_close  = False
    close_reason  = ""
    is_pre_fund   = False

    # Определяем: платит ли фандинг на обеих позициях
    # Short получает когда rate > 0, Long получает когда rate < 0
    both_pay_us = (sf_rate > 0) and (lf_rate < 0)

    if both_pay_us:
        # Фандинг платит на обеих позициях — НЕ закрываем перед фандингом
        # Закрываем только если фандинг на одной стороне стал минус и перевешивает плюс другой
        print(f"[FundingMonitor] {t.ticker}: фандинг платит на обеих позициях — пропускаю закрытие перед фандингом")

        # Проверяем: не стал ли фандинг на одной стороне отрицательным настолько,
        # что перевешивает плюс на другой
        net_fund_per_period_raw = (sf_rate * 100) + (lf_rate * 100)
        # Для short: положительный rate = получаем деньги, отрицательный = платим
        # Для long: отрицательный rate = получаем деньги, положительный = платим
        # net_fund_per_period уже считается как -(sf) - (lf)
        # Если net_fund_per_period < 0 — мы получаем чистый плюс
        # Если net_fund_per_period > 0 — мы платим чистый минус
        if net_fund_per_period > 0 and accumulated_fund < -(entry_spread_pct * 0.8):
            should_close = True
            close_reason = (
                f"Фандинг перестал платить на обеих позициях: "
                f"накопленный убыток {abs(accumulated_fund):.3f}% из {entry_spread_pct:.2f}% спреда"
            )
    else:
        # Обычная логика — закрываем перед фандингом
        # ── Случай 1: скоро фандинг → закрываем всегда ──
        if 0 < minutes_until <= MINUTES_BEFORE_FUNDING:
            should_close = True
            is_pre_fund  = True
            close_reason = (
                f"До фандинга {minutes_until:.0f} мин — закрываем позицию\n"
                f"Entry спред: {entry_spread_pct:.2f}% | Текущий: {current_spread_pct:.2f}%\n"
                f"Накоплено: {accumulated_fund:+.3f}% | После фандинга было бы: {after_next_profit:.3f}%\n"
                f"Фандинг: short={sf_rate*100:+.3f}%/период, long={lf_rate*100:+.3f}%/период"
            )

        # ── Случай 2: накопленный фандинг съел >80% спреда ──
        # НО: если entry_spread отрицательный — НЕ закрываем по фандингу
        elif entry_spread_pct > 0 and net_fund_per_period < 0 and accumulated_fund < -(entry_spread_pct * 0.8):
            should_close = True
            close_reason = (
                f"Фандинг съел {abs(accumulated_fund):.3f}% из {entry_spread_pct:.2f}% спреда\n"
                f"Осталось: {remaining_profit:.3f}%"
            )
        elif entry_spread_pct <= 0:
            print(f"[FundingMonitor] {t.ticker}: entry_spread={entry_spread_pct:.2f}% ≤ 0 — пропускаю закрытие по фандингу")

    if not should_close:
        return

    print(f"[FundingMonitor] {'🕐' if is_pre_fund else '⚠️'} {t.ticker}: закрываем! {close_reason}")

    # Уведомление перед закрытием
    emoji = "🕐" if is_pre_fund else "⚠️"
    await notify(
        f"{emoji} <b>Авто-закрытие перед фандингом: {t.ticker}</b>\n"
        f"{close_reason}"
    )

    # Закрываем позицию
    cs = CloseSignal(ticker=t.ticker, raw_text="pre_funding_close")
    _, close_msg = await close_position(cs)
    await notify(close_msg)

    # Убираем из кэша фандинга
    _funding_cache.pop(t.id, None)

    # ── Перезаход (только для случая 1 — перед фандингом) ──
    if is_pre_fund:
        # Получаем актуальный спред для решения о перезаходе
        # (используем текущий спред из кэша, или запрашиваем заново)
        if sp and lp and lp > 0:
            actual_current_spread = (sp / lp - 1) * 100
        else:
            actual_current_spread = current_spread_pct

        # Небольшая пауза чтобы биржи успели обработать закрытие
        await asyncio.sleep(3)

        # Запрашиваем свежие цены для перезахода
        try:
            s_ex = create_exchange(t.short_exchange)
            l_ex = create_exchange(t.long_exchange)
            fresh_sp, fresh_lp = await asyncio.gather(
                s_ex.get_price(t.symbol),
                l_ex.get_price(t.symbol),
                return_exceptions=True,
            )
            await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)
            if (not isinstance(fresh_sp, Exception) and not isinstance(fresh_lp, Exception)
                    and float(fresh_lp) > 0):
                actual_current_spread = (float(fresh_sp) / float(fresh_lp) - 1) * 100
        except Exception as e:
            print(f"[Reentry] {t.ticker}: ошибка получения свежих цен: {e}")

        print(f"[Reentry] {t.ticker}: текущий спред={actual_current_spread:.2f}% "
              f"min_reentry={REENTRY_MIN_SPREAD_PCT}%")

        await _try_reentry(t, actual_current_spread)