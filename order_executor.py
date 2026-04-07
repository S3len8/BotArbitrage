"""
order_executor.py — открытие и закрытие арбитражных позиций.
Замеряет время исполнения каждой ноги от момента получения сигнала.

Логика фандинг-монитора (обновлено):
  1. За MINUTES_BEFORE_FUNDING минут до начисления — ВСЕГДА закрываем позицию,
     даже если есть небольшой минус по PnL.
  2. После закрытия проверяем текущий спред на биржах:
     - Если спред >= REENTRY_MIN_SPREAD_PCT (3%) → перезаходим (новый сигнал)
     - Если спред < 3% → не перезаходим
  3. Если при закрытии была прибыль (net_pnl > 0) И спред < 3% → не перезаходим.
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

# ── Настройки перезахода ──────────────────────────────────────
# За сколько минут до фандинга закрывать (всегда, независимо от PnL)
MINUTES_BEFORE_FUNDING: int   = 5
# Минимальный спред для перезахода после закрытия перед фандингом
REENTRY_MIN_SPREAD_PCT: float = 3.0


def _now(): return datetime.now(timezone.utc)
def _ms():  return int(time.time() * 1000)


async def open_position(signal: OpenSignal, final_size_usd: float, signal_received_ms: int = None) -> tuple[
    Optional[Trade], str]:
    """
    Покращена функція відкриття:
    1. Налаштування маржі.
    2. Фінальна перевірка спреду (мінімум 3%).
    3. Синхронізація кількості монет/контрактів.
    4. Послідовне виконання ордерів.
    """
    t0 = signal_received_ms or _ms()
    from settings import MIN_SPREAD_PCT

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
        s_p, l_p = await asyncio.gather(
            short_ex.get_price(signal.symbol),
            long_ex.get_price(signal.symbol)
        )

        current_spread = (s_p / l_p - 1) * 100 if l_p > 0 else 0
        print(f"[Guard] {signal.ticker}: Спред перед входом {current_spread:.2f}% (Потрібно: {MIN_SPREAD_PCT}%)")

        if current_spread < MIN_SPREAD_PCT:
            await asyncio.gather(short_ex.close(), long_ex.close())
            msg = f"⛔ {signal.ticker} СКАСОВАНО: спред упав до {current_spread:.2f}% (мін {MIN_SPREAD_PCT}%)"
            return None, msg

        # --- КРОК 2.5: ПЕРЕВІРКА ЩО SHORT > LONG (НЕГАТИВНИЙ СПРЕД) ---
        if s_p <= l_p:
            await asyncio.gather(short_ex.close(), long_ex.close())
            msg = f"⛔ {signal.ticker} СКАСОВАНО: short_price ({s_p}) <= long_price ({l_p}) — негативний спред"
            return None, msg

        # --- КРОК 3: РОЗРАХУНОК ОБСЯГУ ДЛЯ КОЖНОЇ БІРЖІ ОКРЕМО ---
        # Кожна біржа має свої одиниці: Gate — контракти, Binance — монети
        # Рахуємо qty окремо, потім обмежуємо по USD

        inf_s, inf_l = await asyncio.gather(
            short_ex.get_contract_info(signal.symbol),
            long_ex.get_contract_info(signal.symbol)
        )

        mult_s = inf_s.get('multiplier', 1.0) or 1.0
        mult_l = inf_l.get('multiplier', 1.0) or 1.0
        step_s = max(inf_s.get('step', 1.0) or 1.0, 1.0)
        step_l = max(inf_l.get('step', 1.0) or 1.0, 1.0)
        min_s  = max(inf_s.get('min_qty', 1.0) or 1.0, 1.0)
        min_l  = max(inf_l.get('min_qty', 1.0) or 1.0, 1.0)

        # Розраховуємо бажану qty для кожної біржі
        qty_s_raw = final_size_usd / (s_p * mult_s) if (s_p * mult_s) > 0 else 0
        qty_l_raw = final_size_usd / (l_p * mult_l) if (l_p * mult_l) > 0 else 0

        # Округлюємо до кроку кожної біржі
        qty_s = (qty_s_raw // step_s) * step_s
        qty_l = (qty_l_raw // step_l) * step_l

        # Перевірка мінімального лота
        if qty_s < min_s:
            cost_s = min_s * s_p * mult_s
            if cost_s <= final_size_usd * 1.2:
                qty_s = min_s
                print(f"[Executor] {signal.ticker}: Short qty збільшено до мін. лота {min_s}")
            else:
                await asyncio.gather(short_ex.close(), long_ex.close())
                msg = f"❌ {signal.ticker}: мін. лот Short ${cost_s:.2f} > ліміту ${final_size_usd}"
                return None, msg

        if qty_l < min_l:
            cost_l = min_l * l_p * mult_l
            if cost_l <= final_size_usd * 1.2:
                qty_l = min_l
                print(f"[Executor] {signal.ticker}: Long qty збільшено до мін. лота {min_l}")
            else:
                await asyncio.gather(short_ex.close(), long_ex.close())
                msg = f"❌ {signal.ticker}: мін. лот Long ${cost_l:.2f} > ліміту ${final_size_usd}"
                return None, msg

        # Фактичний розмір в USD — кожна сторона окремо
        exec_size_s = qty_s * s_p * mult_s
        exec_size_l = qty_l * l_p * mult_l

        # ЗАХИСТ: фактичний розмір не повинен перевищувати ліміт більше ніж на 20%
        max_allowed = final_size_usd * 1.2
        if exec_size_s > max_allowed or exec_size_l > max_allowed:
            print(f"[Executor] {signal.ticker}: ПЕРЕВИЩЕННЯ ОБСЯГУ! "
                  f"short=${exec_size_s:.2f} long=${exec_size_l:.2f} ліміт=${final_size_usd:.2f}")
            await asyncio.gather(short_ex.close(), long_ex.close())
            msg = f"❌ {signal.ticker}: обсяг перевищено — short=${exec_size_s:.2f}, long=${exec_size_l:.2f} (ліміт ${final_size_usd:.2f})"
            return None, msg

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

        # ПЕРЕВІРКА: фактичний спред після філів ордерів
        actual_spread = (trade.short_entry_price / trade.long_entry_price - 1) * 100
        if actual_spread < MIN_SPREAD_PCT:
            print(f"[Executor] {signal.ticker}: фактичний спред {actual_spread:.2f}% < {MIN_SPREAD_PCT}% — негайне закриття!")
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
            return None, f"Фактичний спред {actual_spread:.2f}% < {MIN_SPREAD_PCT}%"

        save_trade(trade)

        # Логування в канал ордерів
        for side, res, ex_name in [('short', short_r, signal.short_exchange), ('long', long_r, signal.long_exchange)]:
            asyncio.create_task(notify_order(
                ex_name, side, signal.symbol, res['price'], res['qty'],
                (res['qty'] * res['price']), res['order_id'], f"↔️ Pair: {signal.ticker} | Trade #{trade.id}"
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
    update_trade(trade)

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

    fees = (trade.fee_short_usd or 0) + (trade.fee_long_usd or 0)
    if real_short_pnl is not None and real_long_pnl is not None:
        # Exchange PnL уже включает комиссии — не вычитаем их повторно
        trade.short_pnl_usd = round(real_short_pnl, 6)
        trade.long_pnl_usd  = round(real_long_pnl, 6)
        trade.net_pnl_usd   = round(real_short_pnl + real_long_pnl, 6)
    else:
        _calc_pnl(trade)
        # Если считаем сами — вычитаем комиссии
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
            await notify(
                f"🔄 <b>Перезаход {trade.ticker}: отклонён риск-менеджером</b>\n"
                f"📊 Спред: {current_spread_pct:.2f}%\n"
                f"⛔ Причина: {risk.reason}"
            )
            return

        await notify(
            f"🔄 <b>Перезаход {trade.ticker}</b>\n"
            f"📊 Текущий спред: {current_spread_pct:.2f}%\n"
            f"💵 Размер: ${risk.final_size_usd:.2f}\n"
            f"⚡ Открываю новую позицию..."
        )

        new_trade, open_msg = await open_position(reentry_signal, risk.final_size_usd)
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

        except Exception as e:
            print(f"[FundingMonitor] loop error: {e}")


# Cooldown предупреждений: {trade_id: timestamp}
_margin_warn_cooldown: dict[int, float] = {}
MARGIN_WARN_COOLDOWN_S = 120


async def _check_margin_risk(t) -> bool:
    """Двухступенчатая проверка ликвидации и маржинального риска."""
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

    # Логика самолечения: позиций нет на обеих биржах → синхронизируем БД
    if (s_pos is None or isinstance(s_pos, Exception)) and \
            (l_pos is None or isinstance(l_pos, Exception)):
        print(f"[MarginCheck] {t.ticker}: Позиции не найдены на биржах. Синхронизирую БД...")
        t.status = 'closed'
        t.closed_at = datetime.now(timezone.utc).isoformat()
        update_trade(t)
        return True

    # Логика orphan leg: одна позиция закрылась, другую нужно закрыть
    s_on_ex = not isinstance(s_pos, Exception) and s_pos is not None
    l_on_ex = not isinstance(l_pos, Exception) and l_pos is not None

    if s_on_ex and not l_on_ex:
        print(f"[MarginCheck] 🚨 {t.ticker}: LONG позиция закрылась на бирже, закрываю SHORT!")
        await notify(f"🚨 <b>Orphan leg: {t.ticker}</b>\nLONG закрылся на бирже, закрываю SHORT...")
        s_ex2 = create_exchange(t.short_exchange)
        try:
            await s_ex2.close_position(t.symbol, 'buy', t.short_qty or 0)
        finally:
            await s_ex2.close()
        t.status = 'closed'
        t.closed_at = datetime.now(timezone.utc).isoformat()
        update_trade(t)
        return True

    if l_on_ex and not s_on_ex:
        print(f"[MarginCheck] 🚨 {t.ticker}: SHORT позиция закрылась на бирже, закрываю LONG!")
        await notify(f"🚨 <b>Orphan leg: {t.ticker}</b>\nSHORT закрылся на бирже, закрываю LONG...")
        l_ex2 = create_exchange(t.long_exchange)
        try:
            await l_ex2.close_position(t.symbol, 'sell', t.long_qty or 0)
        finally:
            await l_ex2.close()
        t.status = 'closed'
        t.closed_at = datetime.now(timezone.utc).isoformat()
        update_trade(t)
        return True

    s_price = float(s_price_r) if not isinstance(s_price_r, Exception) else None
    l_price = float(l_price_r) if not isinstance(l_price_r, Exception) else None

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