"""
listener.py — слушает Telegram-канал и запускает торговлю.

Логика обработки сообщений:
- NewMessage:     всегда обрабатываем (новый сигнал)
- MessageEdited:  обрабатываем ТОЛЬКО если тикер есть в закреплённом сообщении
- Закреплённое:   читается при старте и обновляется при каждом его редактировании
- Дедупликация:   один тикер не открывается дважды за DEDUP_WINDOW_S секунд

Логика ожидания фандинга:
- Если до фандинга на любой из бирж <= WAIT_BEFORE_FUNDING_MIN минут —
  позицию не открываем сразу, а ставим в очередь ожидания.
- После начисления фандинга проверяем спред:
  * спред >= REENTRY_MIN_SPREAD_PCT → открываем позицию
  * спред < REENTRY_MIN_SPREAD_PCT → отменяем
"""

import asyncio
import time
from telethon import TelegramClient, events
from telethon.tl.functions.channels import GetFullChannelRequest

from settings import TG_API_ID, TG_API_HASH, TG_SESSION, SIGNAL_CHANNEL
from signal_parser import parse_message, parse_pinned, OpenSignal, CloseSignal
from risk_manager import check_signal
from order_executor import open_position, close_position, REENTRY_MIN_SPREAD_PCT
from exchanges import create_exchange
from notifier import notify, notify_mexc, ask_funding_confirmation
from signal_cache import add_signal, remove_signal, is_cached, start_signal_monitor, stop_signal_monitor, start_spread_monitor, stop_spread_monitor, CACHE_MIN_SPREAD

DEDUP_WINDOW_S = 120
_seen: dict[str, float] = {}

# Тикеры из закреплённого сообщения
_pinned_tickers: set[str] = set()
_pinned_msg_id: int | None = None
_prev_pinned_tickers: set[str] = set()  # предыдущее состояние для детекта новых

# Дедупликация MEXC уведомлений
_mexc_notified: dict[str, float] = {}
MEXC_NOTIFY_WINDOW_S = 300

# HARDCODED минимум спреда для всех проверок
MIN_SPREAD_PCT = 3.5

# ── Ожидание фандинга ─────────────────────────────────────────
# Минимальное время до фандинга при котором НЕ открываем позицию сразу
WAIT_BEFORE_FUNDING_MIN: int = 15

# Очередь отложенных сигналов: {ticker: PendingSignal}
# Хранит сигналы которые ждут начисления фандинга
_pending_signals: dict[str, "PendingSignal"] = {}

# ── Подтверждения входа вручную ────────────────────────────────
# Когда funding нельзя получить — запрашиваем подтверждение у пользователя
FUNDING_CONFIRM_TIMEOUT_S: int = 900  # 15 минут

# {ticker: {'signal': OpenSignal, 'requested_at': float, 'message_id': int | None}}
_pending_confirmations: dict[str, dict] = {}

# Заблокированные тикеры после отмены/истечения/CLOSE — не мониторим и не добавляем в кэш
# {ticker: timestamp_when_unblock}
_blocked_signals: dict[str, float] = {}
SIGNAL_BLOCK_DURATION_S: int = 1800  # 30 минут блокировки после отмены

def _block_signal(ticker: str) -> None:
    """Блокирует тикер на BLOCK_DURATION_S после отмены/истечения/CLOSE."""
    _blocked_signals[ticker] = time.time() + SIGNAL_BLOCK_DURATION_S
    from signal_cache import remove_signal
    remove_signal(ticker)
    print(f"[Listener] {ticker}: заблокирован на {SIGNAL_BLOCK_DURATION_S // 60} мин")

def _is_blocked(ticker: str) -> bool:
    """Проверяет заблокирован ли тикер. Очищает истёкшие блокировки."""
    now = time.time()
    expired = [t for t, unblock_at in _blocked_signals.items() if now >= unblock_at]
    for t in expired:
        _blocked_signals.pop(t, None)
    return ticker in _blocked_signals


class PendingSignal:
    """Сигнал ожидающий начисления фандинга."""
    def __init__(self, signal: OpenSignal, fund_time: int, received_ms: int):
        self.signal      = signal
        self.fund_time   = fund_time   # unix timestamp ближайшего фандинга
        self.received_ms = received_ms
        self.task: asyncio.Task | None = None


async def _ask_funding_confirmation(signal: OpenSignal, current_spread: float) -> None:
    """Запрашивает подтверждение входа у пользователя через Telegram."""
    ticker = signal.ticker
    _pending_confirmations[ticker] = {
        'signal':        signal,
        'requested_at':  time.time(),
        'message_id':    None,
        'spread':        current_spread,
    }
    msg_id = await ask_funding_confirmation(
        ticker, signal.spread_pct,
        signal.short_exchange, signal.long_exchange,
        current_spread,
    )
    _pending_confirmations[ticker]['message_id'] = msg_id
    print(f"[Listener] {ticker}: запрос подтверждения отправлен (timeout {FUNDING_CONFIRM_TIMEOUT_S // 60} мин)")


async def _confirmation_monitor() -> None:
    """Фоновый мониторинг ожидающих подтверждений — авто-отмена через 15 мин."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        expired = [
            t for t, c in _pending_confirmations.items()
            if now - c['requested_at'] > FUNDING_CONFIRM_TIMEOUT_S
        ]
        for ticker in expired:
            conf = _pending_confirmations.pop(ticker, None)
            if conf:
                await notify(
                    f"⛔ <b>{ticker}: сигнал не активен</b>\n"
                    f"📊 Причина: истёк таймаут подтверждения (15 мин)\n"
                    f"💡 Funding не удалось получить — позиция не открыта"
                )
                print(f"[Listener] {ticker}: подтверждение истекло — сигнал заблокирован на {SIGNAL_BLOCK_DURATION_S // 60} мин")
                _block_signal(ticker)


def _handle_confirmation_response(ticker: str, approved: bool) -> None:
    """Обрабатывает ответ пользователя на запрос подтверждения."""
    conf = _pending_confirmations.pop(ticker, None)
    if not conf:
        return
    if not approved:
        _block_signal(ticker)
    asyncio.create_task(_process_confirmation(conf, approved))


async def _process_confirmation(conf: dict, approved: bool) -> None:
    """Обрабатывает подтверждение — проверяет спред и открывает позицию."""
    signal = conf['signal']
    ticker = signal.ticker
    if not approved:
        await notify(f"ℹ️ <b>{ticker}: вход отменён пользователем</b>")
        print(f"[Listener] {ticker}: вход отменён пользователем")
        return
    try:
        short_ex = create_exchange(signal.short_exchange)
        long_ex  = create_exchange(signal.long_exchange)
        try:
            s_p, l_p = await asyncio.gather(
                short_ex.get_price(signal.symbol),
                long_ex.get_price(signal.symbol),
            )
            current_spread = (s_p / l_p - 1) * 100 if l_p > 0 else 0
            if current_spread < MIN_SPREAD_PCT:
                await notify(
                    f"⛔ <b>{ticker}: сигнал не активен</b>\n"
                    f"📊 Причина: спред упал до {current_spread:.2f}% (мін {MIN_SPREAD_PCT}%)\n"
                    f"💡 Funding не удалось получить — позиция не открыта"
                )
                print(f"[Listener] {ticker}: спред {current_spread:.2f}% < {MIN_SPREAD_PCT}% — вход отменён")
                return
            risk = await check_signal(signal, short_ex, long_ex)
            if not risk:
                await notify(f"⛔ <b>{ticker}: risk-менеджер отклонил: {risk.reason}</b>")
                return
            await open_position(signal, risk.final_size_short, risk.final_size_long)
            await notify(f"✅ <b>{ticker}: позиция открыта после подтверждения</b>")
        finally:
            await asyncio.gather(short_ex.close(), long_ex.close())
    except Exception as e:
        await notify(f"❌ <b>{ticker}: ошибка открытия после подтверждения: {e}</b>")
        print(f"[Listener] {ticker}: ошибка открытия после подтверждения: {e}")


async def _notify_mexc(signal, reason: str = None):
    """Отправляет уведомление о MEXC сигнале без открытия сделки."""
    now = time.time()
    exchanges = tuple(sorted([signal.short_exchange, signal.long_exchange]))
    key = f"{signal.ticker}_{exchanges}"
    if now - _mexc_notified.get(key, 0) < MEXC_NOTIFY_WINDOW_S:
        return
    _mexc_notified[key] = now

    other_ex = signal.long_exchange if signal.short_exchange == 'mexc' else signal.short_exchange
    fs = f"{signal.funding_short*100:+.3f}%" if signal.funding_short is not None else "—"
    fl = f"{signal.funding_long*100:+.3f}%" if signal.funding_long is not None else "—"
    i_s = f"{signal.interval_short}ч" if signal.interval_short else "—"
    i_l = f"{signal.interval_long}ч" if signal.interval_long else "—"

    msg = (f"👁 <b>MEXC сигнал: {signal.ticker}</b>\n"
           f"📊 Спред: {signal.spread_pct:.2f}%\n"
           f"📉 Short: {signal.short_exchange.upper()} @ ${signal.short_price:.6f}\n"
           f"📈 Long:  {signal.long_exchange.upper()} @ ${signal.long_price:.6f}\n"
           f"💸 Фандинг: {fs} / {fl} | Интервалы: {i_s}/{i_l}")
    if reason:
        msg += f"\n⚠️ Причина пропуска: {reason}"
    msg += f"\n\n⚠️ <i>MEXC API не поддерживает торговлю — открой вручную</i>"

    from notifier import tradingview_url, exchange_url
    tv_url    = tradingview_url(signal.short_exchange, signal.long_exchange, signal.ticker)
    short_url = exchange_url(signal.short_exchange, signal.ticker)
    long_url  = exchange_url(signal.long_exchange, signal.ticker)

    buttons = []
    if short_url:
        buttons.append({"text": f"📉 {signal.short_exchange.upper()}", "url": short_url})
    if long_url:
        buttons.append({"text": f"📈 {signal.long_exchange.upper()}", "url": long_url})

    await notify_mexc(msg, tv_url=tv_url, buttons=buttons)
    print(f"[MEXC] Уведомление отправлено: {signal.ticker} {signal.short_exchange}↕{signal.long_exchange}")


def _is_duplicate(ticker: str) -> bool:
    now = time.time()
    if now - _seen.get(ticker, 0) < DEDUP_WINDOW_S:
        return True
    _seen[ticker] = now
    return False


def _funding_ok(signal) -> tuple[bool, str]:
    """Фандинг-фильтр — Стратегия #4."""
    fs = signal.funding_short
    fl = signal.funding_long
    if fs is None or fl is None:
        return True, "фандинг неизвестен — не блокируем"

    diff_pct      = abs(fs - fl) * 100
    both_negative = fs < 0 and fl < 0
    both_pay_us  = (fs > 0) and (fl < 0)
    one_zero     = (fs == 0) or (fl == 0)
    i_s = signal.interval_short
    i_l = signal.interval_long
    info = f"short={fs*100:+.3f}% long={fl*100:+.3f}% diff={diff_pct:.3f}% int={i_s}ч/{i_l}ч"

    if diff_pct < 0.2:
        return True, f"diff<0.2% ✅ {info}"
    if both_negative and diff_pct < 1.0:
        return True, f"оба отриц. + diff<1% ✅ {info}"
    if both_pay_us:
        return True, f"оба платят нам ✅ {info}"
    if one_zero:
        return True, f"одна сторона 0% ✅ {info}"

    return False, f"фандинг не прошёл ❌ {info}"


async def _get_funding_times(signal: OpenSignal) -> tuple[int | None, int | None]:
    """
    Получает время следующего фандинга на обеих биржах.
    Возвращает (next_short_ts, next_long_ts) в unix секундах.
    """
    try:
        s_ex = create_exchange(signal.short_exchange)
        l_ex = create_exchange(signal.long_exchange)
        try:
            s_fund, l_fund = await asyncio.gather(
                s_ex.get_funding_rate(signal.symbol),
                l_ex.get_funding_rate(signal.symbol),
                return_exceptions=True,
            )
        finally:
            await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)

        next_short = s_fund.get('next_time') if (not isinstance(s_fund, Exception) and s_fund) else None
        next_long  = l_fund.get('next_time') if (not isinstance(l_fund, Exception) and l_fund) else None
        return next_short, next_long
    except Exception as e:
        print(f"[Listener] _get_funding_times {signal.ticker}: {e}")
        return None, None


def _minutes_until(ts: int | None) -> float | None:
    """Минут до unix timestamp. Возвращает None если ts=None или <= 0."""
    if not ts or ts <= 0:
        return None
    return max(0.0, (ts - time.time()) / 60)

def _min_until(a: float | None, b: float | None) -> float | None:
    """Минимум двух времён, None = игнорируется (бесконечность)."""
    if a is None: return b
    if b is None: return a
    return min(a, b)


async def _wait_and_open(pending: PendingSignal):
    """
    Ждёт начисления фандинга, потом проверяет спред и открывает позицию.
    Запускается как отдельная asyncio задача.
    """
    signal   = pending.signal
    fund_ts  = pending.fund_time
    ticker   = signal.ticker

    # Ждём до момента фандинга + небольшой буфер (30 сек чтобы биржи успели начислить)
    wait_secs = max(0, fund_ts - time.time()) + 30
    print(f"[Listener] {ticker}: ждём фандинга {wait_secs:.0f}с (до {fund_ts})")

    await notify(
        f"⏳ <b>{ticker}: ожидаем фандинга</b>\n"
        f"📊 Спред сигнала: {signal.spread_pct:.2f}%\n"
        f"📉 SHORT {signal.short_exchange.upper()} | 📈 LONG {signal.long_exchange.upper()}\n"
        f"⏱ Начисление через ~{wait_secs/60:.0f} мин\n"
        f"🔄 После начисления проверим спред (минимум {REENTRY_MIN_SPREAD_PCT}%)"
    )

    await asyncio.sleep(wait_secs)

    # Убираем из очереди
    _pending_signals.pop(ticker, None)

    print(f"[Listener] {ticker}: фандинг начислен, проверяем спред")

    # Получаем актуальные цены
    try:
        s_ex = create_exchange(signal.short_exchange)
        l_ex = create_exchange(signal.long_exchange)
        try:
            s_price, l_price, s_fund, l_fund = await asyncio.gather(
                s_ex.get_price(signal.symbol),
                l_ex.get_price(signal.symbol),
                s_ex.get_funding_rate(signal.symbol),
                l_ex.get_funding_rate(signal.symbol),
                return_exceptions=True,
            )
        finally:
            await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)
    except Exception as e:
        print(f"[Listener] {ticker}: ошибка получения цен после фандинга: {e}")
        await notify(f"❌ <b>{ticker}: ошибка проверки после фандинга</b>\n{e}")
        return

    if isinstance(s_price, Exception) or isinstance(l_price, Exception):
        await notify(f"❌ <b>{ticker}: не удалось получить цены после фандинга</b>")
        return

    sp = float(s_price)
    lp = float(l_price)

    if lp <= 0:
        await notify(f"❌ <b>{ticker}: некорректная цена long после фандинга</b>")
        return

    current_spread = (sp / lp - 1) * 100

    # Обновляем фандинг-ставки в сигнале
    sf = s_fund.get('rate') if (not isinstance(s_fund, Exception) and s_fund) else signal.funding_short
    lf = l_fund.get('rate') if (not isinstance(l_fund, Exception) and l_fund) else signal.funding_long
    i_s = s_fund.get('interval_hours') if (not isinstance(s_fund, Exception) and s_fund) else signal.interval_short
    i_l = l_fund.get('interval_hours') if (not isinstance(l_fund, Exception) and l_fund) else signal.interval_long

    print(f"[Listener] {ticker}: после фандинга спред={current_spread:.2f}% (минимум {REENTRY_MIN_SPREAD_PCT}%)")

    if current_spread < REENTRY_MIN_SPREAD_PCT:
        await notify(
            f"⛔ <b>{ticker}: после фандинга спред упал</b>\n"
            f"📊 Текущий спред: {current_spread:.2f}%\n"
            f"Минимум для входа: {REENTRY_MIN_SPREAD_PCT}%\n"
            f"Позицию не открываем"
        )
        # Сбрасываем дедупликацию чтобы новый сигнал мог пройти
        _seen.pop(ticker, None)
        return

    # Спред достаточен — создаём обновлённый сигнал с актуальными данными
    from signal_parser import OpenSignal as _OS
    updated_signal = _OS(
        ticker=signal.ticker,
        symbol=signal.symbol,
        short_exchange=signal.short_exchange,
        long_exchange=signal.long_exchange,
        short_price=sp,
        long_price=lp,
        spread_pct=current_spread,
        funding_short=sf,
        funding_long=lf,
        max_size_short=signal.max_size_short,
        max_size_long=signal.max_size_long,
        interval_short=i_s,
        interval_long=i_l,
        raw_text=f"[post-funding entry] {signal.ticker}",
    )

    # Проверяем время до фандинга — если <= 15 мин, НЕ заходим
    try:
        next_short_ts, next_long_ts = await asyncio.wait_for(
            _get_funding_times(updated_signal), timeout=10
        )
        min_until = _min_until(
            _minutes_until(next_short_ts),
            _minutes_until(next_long_ts),
        )
    except ValueError:
        # Не удалось получить время фандинга — повторяем запрос через 10 сек
        print(f"[Listener] {ticker}: не удалось получить время фандинга — повторяю через 10с...")
        await asyncio.sleep(10)
        try:
            next_short_ts, next_long_ts = await asyncio.wait_for(
                _get_funding_times(updated_signal), timeout=10
            )
            min_until = _min_until(
                _minutes_until(next_short_ts),
                _minutes_until(next_long_ts),
            )
        except ValueError:
            print(f"[Listener] {ticker}: снова не удалось получить время фандинга — сигнал остаётся в кэше")
            # Кэшируем обратно для мониторинга
            _seen.pop(ticker, None)
            from signal_parser import OpenSignal as _OS
            from signal_cache import add_signal
            cached_sig = _OS(
                ticker=signal.ticker, symbol=signal.symbol,
                short_exchange=signal.short_exchange, long_exchange=signal.long_exchange,
                short_price=sp, long_price=lp, spread_pct=current_spread,
                funding_short=sf, funding_long=lf,
                max_size_short=signal.max_size_short, max_size_long=signal.max_size_long,
                interval_short=i_s, interval_long=i_l,
                raw_text=f"[funding-timeout requeue] {signal.ticker}",
            )
            add_signal(cached_sig, current_spread)
            return
    except (asyncio.TimeoutError, Exception) as e:
        print(f"[Listener] {ticker}: ошибка времени фандинга после начисления — блокируем вход: {e}")
        await notify(f"⛔ <b>{ticker}: не удалось получить время фандинга — позицию не открываем</b>")
        _seen.pop(ticker, None)
        return

    if min_until <= WAIT_BEFORE_FUNDING_MIN:
        await notify(
            f"⛔ <b>{ticker}: до фандинга {min_until:.0f} мин — не перезаходим</b>\n"
            f"📊 Спред: {current_spread:.2f}%\n"
            f"⏱ Ждём ещё {WAIT_BEFORE_FUNDING_MIN - min_until:.0f} мин"
        )
        # Ставим в очередь ожидания
        nearest = min(next_short_ts or 0, next_long_ts or 0) or None
        pending = PendingSignal(updated_signal, nearest, pending.received_ms)
        _pending_signals[signal.ticker] = pending
        pending.task = asyncio.create_task(_wait_and_open(pending))
        return

    # Проверяем фандинг-фильтр с новыми ставками
    fund_ok, fund_reason = _funding_ok(updated_signal)
    if not fund_ok:
        await notify(
            f"⛔ <b>{ticker}: фандинг-фильтр после начисления</b>\n"
            f"📊 Спред: {current_spread:.2f}%\n"
            f"⚠️ {fund_reason}\n"
            f"Позицию не открываем"
        )
        _seen.pop(ticker, None)
        return

    # Проверяем риск-менеджер
    try:
        s_ex2 = create_exchange(signal.short_exchange)
        l_ex2 = create_exchange(signal.long_exchange)
        try:
            risk = await asyncio.wait_for(
                check_signal(updated_signal, s_ex2, l_ex2),
                timeout=30
            )
        finally:
            await asyncio.gather(s_ex2.close(), l_ex2.close(), return_exceptions=True)
    except asyncio.TimeoutError:
        await notify(f"⏱ <b>{ticker}: таймаут риск-проверки после фандинга</b>")
        _seen.pop(ticker, None)
        return
    except Exception as e:
        await notify(f"❌ <b>{ticker}: ошибка риск-проверки после фандинга</b>\n{e}")
        _seen.pop(ticker, None)
        return

    if not risk:
        await notify(
            f"⛔ <b>{ticker}: риск-менеджер отклонил после фандинга</b>\n"
            f"📊 Спред: {current_spread:.2f}%\n"
            f"⚠️ {risk.reason}"
        )
        _seen.pop(ticker, None)
        return

    await notify(
        f"✅ <b>{ticker}: открываем после фандинга</b>\n"
        f"📊 Спред: {current_spread:.2f}%\n"
        f"💵 SH: ${risk.final_size_short:.2f} / LH: ${risk.final_size_long:.2f}"
    )

    try:
        trade, msg = await asyncio.wait_for(
            open_position(updated_signal, risk.final_size_short, risk.final_size_long,
                          signal_received_ms=pending.received_ms),
            timeout=60
        )
        if trade is None:
            _seen.pop(ticker, None)
    except asyncio.TimeoutError:
        await notify(f"⏱ <b>{ticker}: таймаут открытия после фандинга</b>")
        _seen.pop(ticker, None)
    except Exception as e:
        await notify(f"❌ <b>{ticker}: ошибка открытия после фандинга</b>\n{e}")
        _seen.pop(ticker, None)


class SignalListener:

    def __init__(self):
        self.client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
        self._running = False

    async def start(self):
        global _pinned_msg_id
        self._running = True
        await self.client.start()
        me = await self.client.get_me()
        print(f"[Listener] Logged in as {me.username}")
        print(f"[Listener] Watching: {SIGNAL_CHANNEL}")
        await notify(f"🟢 <b>Бот запущен</b>\nКанал: {SIGNAL_CHANNEL}")

        await self._load_pinned()

        # Запускаем мониторы кэшированных сигналов и спреда позиций
        await start_signal_monitor()
        await start_spread_monitor()

        # Запускаем монитор подтверждений (авто-отмена через 15 мин)
        asyncio.create_task(_confirmation_monitor())

        RECENT_MSG_SECONDS = 5 * 60

        @self.client.on(events.CallbackQuery)
        async def on_callback(event):
            if not self._running:
                return
            data = event.data or ""
            if data.startswith("confirm_yes_"):
                ticker = data.replace("confirm_yes_", "")
                print(f"[Listener] Подтверждение получено: {ticker} ✅")
                await event.answer("✅ Вход подтверждён!", alert=True)
                _handle_confirmation_response(ticker, approved=True)
            elif data.startswith("confirm_no_"):
                ticker = data.replace("confirm_no_", "")
                print(f"[Listener] Отмена подтверждения: {ticker} ❌")
                await event.answer("❌ Вход отменён", alert=True)
                _handle_confirmation_response(ticker, approved=False)

        @self.client.on(events.NewMessage(chats=SIGNAL_CHANNEL))
        async def on_new(event):
            if not self._running:
                return
            received_ms = int(time.time() * 1000)
            text = event.message.text or ""
            await self._handle(text, received_ms, is_edit=False)

        @self.client.on(events.MessageEdited(chats=SIGNAL_CHANNEL))
        async def on_edit(event):
            if not self._running:
                return
            received_ms = int(time.time() * 1000)
            msg_id = event.message.id

            if msg_id == _pinned_msg_id:
                print(f"[Listener] Закреплённое сообщение обновлено — перечитываем")
                await self._load_pinned()
                return

            msg_date = event.message.date
            if msg_date:
                import datetime
                age_seconds = (datetime.datetime.now(datetime.timezone.utc) - msg_date).total_seconds()
                is_recent = age_seconds <= RECENT_MSG_SECONDS
            else:
                is_recent = False

            await self._handle(event.message.text or "", received_ms, is_edit=True, is_recent=is_recent)

        await self.client.run_until_disconnected()

    async def stop(self):
        self._running = False
        # Отменяем все задачи ожидания фандинга
        for ticker, pending in list(_pending_signals.items()):
            if pending.task and not pending.task.done():
                pending.task.cancel()
                print(f"[Listener] Отменено ожидание фандинга для {ticker}")
        _pending_signals.clear()
        # Отменяем ожидающие подтверждения
        for ticker in list(_pending_confirmations.keys()):
            print(f"[Listener] Отменено ожидание подтверждения для {ticker}")
        _pending_confirmations.clear()
        # Останавливаем мониторы
        await stop_signal_monitor()
        await stop_spread_monitor()
        await notify("🔴 <b>Бот остановлен</b>")
        await self.client.disconnect()

    async def _load_pinned(self):
        global _pinned_tickers, _pinned_msg_id, _prev_pinned_tickers
        try:
            full = await self.client(GetFullChannelRequest(SIGNAL_CHANNEL))
            pinned_id = full.full_chat.pinned_msg_id
            if not pinned_id:
                print(f"[Listener] Закреплённое сообщение не найдено")
                return
            _pinned_msg_id = pinned_id
            msgs = await self.client.get_messages(SIGNAL_CHANNEL, ids=pinned_id)
            if not msgs:
                return
            text = msgs.text or ""
            signals = parse_pinned(text)
            new_tickers = {s.ticker for s in signals}

            # При самом первом вызове (старт) — обрабатываем все тикеры
            is_first = not _prev_pinned_tickers
            if is_first:
                _prev_pinned_tickers = new_tickers
                _pinned_tickers = new_tickers
                print(f"[Listener] Закреплённое: {len(signals)} спредов → тикеры: {_pinned_tickers}")
                if signals:
                    asyncio.create_task(self._process_pinned_signals(signals, is_startup=True))
            else:
                # При обновлении — только новые тикеры
                added = new_tickers - _prev_pinned_tickers
                _prev_pinned_tickers = new_tickers
                _pinned_tickers = new_tickers
                print(f"[Listener] Закреплённое обновлено: новые={added}")
                if added:
                    new_signals = [s for s in signals if s.ticker in added]
                    asyncio.create_task(self._process_pinned_signals(new_signals, is_startup=False))
        except Exception as e:
            print(f"[Listener] Ошибка чтения закреплённого: {e}")

    async def _process_pinned_signals(self, signals: list, is_startup: bool = False):
        """Обрабатывает сигналы из закреплённого — один проход при запуске."""
        print(f"[Listener] {'Старт' if is_startup else 'Обновление'} pinned: {len(signals)} тикеров")
        for i, signal in enumerate(signals):
            ticker = signal.ticker
            if _is_blocked(ticker):
                print(f"[Listener] [{i+1}/{len(signals)}] {ticker}: заблокирован — пропускаем")
                continue
            from db import get_open_trade
            if get_open_trade(ticker):
                print(f"[Listener] [{i+1}/{len(signals)}] {ticker}: позиция открыта — пропускаем")
                continue

            print(f"[Listener] [{i+1}/{len(signals)}] {ticker}: обрабатываю...")
            received_ms = int(time.time() * 1000)
            await self._handle_pinned(signal, received_ms, is_startup=is_startup)

            if i < len(signals) - 1:
                await asyncio.sleep(3)

        print(f"[Listener] pinned обработан")

    async def _handle_pinned(self, signal: OpenSignal, received_ms: int, is_startup: bool = False):
        """Обрабатывает один сигнал из закреплённого — один проход, спред из сообщения."""
        ticker = signal.ticker
        is_mexc = 'mexc' in (signal.short_exchange.lower(), signal.long_exchange.lower())
        if is_mexc:
            await _notify_mexc(signal)
            return

        print(f"[Listener] [pinned] {ticker}: спред={signal.spread_pct:.2f}% (need {MIN_SPREAD_PCT}%)")

        # Спред ниже минимума — пропускаем (НЕ кэшируем из pinned)
        if signal.spread_pct < MIN_SPREAD_PCT:
            print(f"[Listener] [pinned] {ticker}: спред {signal.spread_pct:.2f}% < {MIN_SPREAD_PCT}% — пропускаем")
            return

        # Спред OK — получаем живые цены и funding
        try:
            short_ex = create_exchange(signal.short_exchange)
            long_ex  = create_exchange(signal.long_exchange)
            s_p, l_p, s_fund, l_fund = await asyncio.gather(
                short_ex.get_price(signal.symbol),
                long_ex.get_price(signal.symbol),
                short_ex.get_funding_rate(signal.symbol),
                long_ex.get_funding_rate(signal.symbol),
            )
            await asyncio.gather(short_ex.close(), long_ex.close())
        except Exception as e:
            print(f"[Listener] [pinned] {ticker}: ошибка получения данных — {e}")
            return

        s_p = float(s_p)
        l_p = float(l_p)
        current_spread = (s_p / l_p - 1) * 100 if l_p > 0 else 0

        # Funding: используем полученные данные
        sf = s_fund.get('rate') if s_fund else None
        lf = l_fund.get('rate') if l_fund else None
        ns_ts = s_fund.get('next_time') if s_fund else None
        nl_ts = l_fund.get('next_time') if l_fund else None

        from signal_parser import OpenSignal as _OS
        updated = _OS(
            ticker=signal.ticker, symbol=signal.symbol,
            short_exchange=signal.short_exchange, long_exchange=signal.long_exchange,
            short_price=s_p, long_price=l_p, spread_pct=current_spread,
            funding_short=sf, funding_long=lf,
            max_size_short=None, max_size_long=None,
            interval_short=None, interval_long=None,
            raw_text=f"[pinned] {signal.ticker}",
        )

        # Проверяем funding
        if sf is not None and lf is not None:
            diff_pct = abs(sf - lf) * 100
            both_negative = sf < 0 and lf < 0
            both_pay_us = (sf > 0) and (lf < 0)
            passes = diff_pct < 0.2 or (both_negative and diff_pct < 1.0) or both_pay_us
            if not passes:
                print(f"[Listener] [pinned] {ticker}: funding не прошёл — пропускаем")
                return

        # Funding близко — пропускаем
        if ns_ts or nl_ts:
            try:
                min_until = _min_until(
                    _minutes_until(ns_ts),
                    _minutes_until(nl_ts),
                )
            except (ValueError, TypeError):
                min_until = None
            if min_until is not None and min_until <= WAIT_BEFORE_FUNDING_MIN:
                print(f"[Listener] [pinned] {ticker}: funding через {min_until:.0f} мин — пропускаем")
                return

        # Все OK — открываем
        try:
            short_ex2 = create_exchange(signal.short_exchange)
            long_ex2  = create_exchange(signal.long_exchange)
            try:
                risk = await asyncio.wait_for(
                    check_signal(updated, short_ex2, long_ex2),
                    timeout=30
                )
            finally:
                await asyncio.gather(short_ex2.close(), long_ex2.close())

            if not risk:
                print(f"[Listener] [pinned] {ticker}: risk отклонил — {risk.reason}")
                await notify(f"⛔ <b>{ticker}: risk отклонил ({risk.reason})")
                return

            await open_position(updated, risk.final_size_short, risk.final_size_long, signal_received_ms=received_ms)
        except Exception as e:
            print(f"[Listener] [pinned] {ticker}: ошибка открытия — {e}")
            await notify(f"❌ <b>{ticker}: ошибка открытия — {e}")

    async def _handle(self, text: str, received_ms: int, is_edit: bool, is_recent: bool = False):
        signal = parse_message(text)
        if signal is None:
            return

        if isinstance(signal, OpenSignal):
            is_mexc = 'mexc' in (signal.short_exchange.lower(), signal.long_exchange.lower())

            if is_mexc:
                print(f"[Listener] Сигнал MEXC {signal.ticker} перехвачен.")
                await _notify_mexc(signal)
                return

            # Пропускаем заблокированные тикеры (после отмены/CLOSE)
            if _is_blocked(signal.ticker):
                remaining = int(_blocked_signals.get(signal.ticker, 0) - time.time())
                print(f"[Listener] {signal.ticker} заблокирован — пропускаем (осталось {remaining // 60} мин)")
                return

            in_pinned = signal.ticker in _pinned_tickers
            if is_edit:
                if is_recent:
                    source = "edit+recent"
                elif in_pinned:
                    source = "edit+pinned"
                else:
                    if is_mexc:
                        await _notify_mexc(signal)
                    print(f"[Listener] edit {signal.ticker} — не в закреплённом и не свежее, пропускаем")
                    return
            else:
                source = "new"

            ok, reason = _funding_ok(signal)
            if not ok:
                if is_mexc:
                    await _notify_mexc(signal, reason=reason)
                print(f"[Listener] [{source}] {signal.ticker} — {reason}, пропускаем")
                return

            if is_mexc:
                await _notify_mexc(signal)
                print(f"[Listener] [{source}] MEXC сигнал {signal.ticker} — уведомление отправлено, торговля пропущена")
                return

            print(f"[Listener] [{source}] OPEN {signal.ticker} {signal.short_exchange}↕{signal.long_exchange} {signal.spread_pct:.2f}% | {reason}")
            # ВСЕ сигналы идут в кэш — вход только когда живой спред >= 3%
            await self._cache_signal(signal, received_ms)

        elif isinstance(signal, CloseSignal):
            # Если тикер в кэше — удаляем и блокируем
            if is_cached(signal.ticker):
                await remove_signal(signal.ticker)
                _block_signal(signal.ticker)
            # Если тикер ожидает подтверждения — отменяем и блокируем
            if signal.ticker in _pending_confirmations:
                conf = _pending_confirmations.pop(signal.ticker, None)
                if conf:
                    await notify(
                        f"⛔ <b>{signal.ticker}: сигнал не активен</b>\n"
                        f"📊 Причина: спред сошёлся (CLOSE сигнал)\n"
                        f"💡 Funding не удалось получить — позиция не открыта"
                    )
                    print(f"[Listener] {signal.ticker}: подтверждение отменено — спред сошёлся, блокирован на {SIGNAL_BLOCK_DURATION_S // 60} мин")
                    _block_signal(signal.ticker)
            print(f"[Listener] CLOSE {signal.ticker}")
            await self._close(signal)

    async def _cache_signal(self, signal: OpenSignal, received_ms: int):
        """Кэширует сигнал и запускает мониторинг — вход только когда живой спред >= 3%."""
        if is_cached(signal.ticker):
            print(f"[Listener] {signal.ticker} уже в кэше — обновляю")
            await remove_signal(signal.ticker)

        # Получаем живые цены
        try:
            s_ex = create_exchange(signal.short_exchange)
            l_ex = create_exchange(signal.long_exchange)
            s_price, l_price = await asyncio.gather(
                s_ex.get_price(signal.symbol),
                l_ex.get_price(signal.symbol),
            )
            await asyncio.gather(s_ex.close(), l_ex.close())
        except Exception as e:
            print(f"[Listener] Ошибка получения цен для кэша {signal.ticker}: {e}")
            return

        if isinstance(s_price, Exception) or isinstance(l_price, Exception):
            print(f"[Listener] Не удалось получить цены для кэша {signal.ticker}")
            return

        sp = float(s_price)
        lp = float(l_price)
        if lp <= 0:
            return

        current_spread = (sp / lp - 1) * 100

        from signal_parser import OpenSignal as _OS
        cached_sig = _OS(
            ticker=signal.ticker,
            symbol=signal.symbol,
            short_exchange=signal.short_exchange,
            long_exchange=signal.long_exchange,
            short_price=sp,
            long_price=lp,
            spread_pct=current_spread,
            funding_short=signal.funding_short,
            funding_long=signal.funding_long,
            max_size_short=signal.max_size_short,
            max_size_long=signal.max_size_long,
            interval_short=signal.interval_short,
            interval_long=signal.interval_long,
            raw_text=signal.raw_text,
        )

        add_signal(cached_sig, current_spread)
        await notify(
            f"💤 <b>{signal.ticker}: сигнал в кэше</b>\n"
            f"📊 Спред: {current_spread:.2f}% (мін {CACHE_MIN_SPREAD}%)\n"
            f"📉 SHORT {signal.short_exchange.upper()} | 📈 LONG {signal.long_exchange.upper()}\n"
            f"⏳ Мониторю каждые 5с, макс 15 мин"
        )
    async def _open(self, signal: OpenSignal, received_ms: int):
        if _is_duplicate(signal.ticker):
            print(f"[Listener] Дубль {signal.ticker} — пропускаем (окно {DEDUP_WINDOW_S}с)")
            return

        try:
            short_ex = create_exchange(signal.short_exchange)
            long_ex  = create_exchange(signal.long_exchange)
        except ValueError as e:
            if 'Unknown exchange' in str(e):
                return
            print(f"[Listener] Ошибка биржи {signal.ticker}: {e}")
            _seen.pop(signal.ticker, None)
            return
        except Exception as e:
            print(f"[Listener] Ошибка биржи {signal.ticker}: {e}")
            _seen.pop(signal.ticker, None)
            return

        # ── Проверка времени до фандинга ─────────────────────
        # Получаем время следующего фандинга на обеих биржах
        try:
            next_short_ts, next_long_ts = await asyncio.wait_for(
                _get_funding_times(signal), timeout=15
            )
        except asyncio.TimeoutError:
            print(f"[Listener] {signal.ticker}: таймаут получения времени фандинга — НЕ открываем")
            await notify(f"⛔ <b>{signal.ticker}</b>: не удалось получить время фандинга — позиция не открыта")
            await short_ex.close()
            await long_ex.close()
            _seen.pop(signal.ticker, None)
            return

        try:
            min_until = _min_until(
                _minutes_until(next_short_ts),
                _minutes_until(next_long_ts),
            )
        except ValueError:
            min_until = None

        if min_until is None:
            current_spread = (signal.short_price / signal.long_price - 1) * 100 if signal.long_price > 0 else 0
            print(f"[Listener] {signal.ticker}: funding неизвестен — запрашиваю подтверждение")
            await short_ex.close()
            await long_ex.close()
            await _ask_funding_confirmation(signal, current_spread)
            _seen.pop(signal.ticker, None)
            return

        nearest_fund_ts = None
        if next_short_ts and next_long_ts:
            nearest_fund_ts = min(next_short_ts, next_long_ts)
        elif next_short_ts:
            nearest_fund_ts = next_short_ts
        elif next_long_ts:
            nearest_fund_ts = next_long_ts

        if min_until <= WAIT_BEFORE_FUNDING_MIN and nearest_fund_ts:
            # До фандинга <= 15 минут — НЕ открываем, ждём начисления
            print(f"[Listener] {signal.ticker}: до фандинга {min_until:.0f} мин "
                  f"(<= {WAIT_BEFORE_FUNDING_MIN}) — ждём начисления")

            await short_ex.close()
            await long_ex.close()

            # Если уже есть ожидающий сигнал по этому тикеру — заменяем
            if signal.ticker in _pending_signals:
                old = _pending_signals[signal.ticker]
                if old.task and not old.task.done():
                    old.task.cancel()
                print(f"[Listener] {signal.ticker}: заменяем существующее ожидание")

            pending = PendingSignal(signal, nearest_fund_ts, received_ms)
            _pending_signals[signal.ticker] = pending
            pending.task = asyncio.create_task(_wait_and_open(pending))
            return
        # ─────────────────────────────────────────────────────

        # Фандинг далеко — открываем как обычно
        try:
            risk = await asyncio.wait_for(
                check_signal(signal, short_ex, long_ex),
                timeout=30
            )
        except asyncio.TimeoutError:
            print(f"[Listener] TIMEOUT check_signal {signal.ticker}")
            await notify(f"⏱ Таймаут проверки {signal.ticker} (30с)")
            await short_ex.close()
            await long_ex.close()
            return
        except Exception as e:
            print(f"[Listener] Ошибка check_signal {signal.ticker}: {e}")
            await short_ex.close()
            await long_ex.close()
            return

        await short_ex.close()
        await long_ex.close()

        if not risk:
            print(f"[Listener] Пропущен {signal.ticker}: {risk.reason}")

            # Если причина — низкий спред, кэшируем сигнал для мониторинга
            if 'спред' in risk.reason.lower() or 'spread' in risk.reason.lower():
                if not is_cached(signal.ticker):
                    # Получаем актуальные цены для кэширования
                    try:
                        s_price, l_price = await asyncio.gather(
                            short_ex.get_price(signal.symbol),
                            long_ex.get_price(signal.symbol),
                            return_exceptions=True,
                        )
                        if not isinstance(s_price, Exception) and not isinstance(l_price, Exception):
                            sp = float(s_price)
                            lp = float(l_price)
                            current_spread = (sp / lp - 1) * 100 if lp > 0 else 0

                            # Обновляем цены в сигнале
                            from signal_parser import OpenSignal as _OS
                            cached_sig = _OS(
                                ticker=signal.ticker,
                                symbol=signal.symbol,
                                short_exchange=signal.short_exchange,
                                long_exchange=signal.long_exchange,
                                short_price=sp,
                                long_price=lp,
                                spread_pct=current_spread,
                                funding_short=signal.funding_short,
                                funding_long=signal.funding_long,
                                max_size_short=signal.max_size_short,
                                max_size_long=signal.max_size_long,
                                interval_short=signal.interval_short,
                                interval_long=signal.interval_long,
                                raw_text=signal.raw_text,
                            )
                            add_signal(cached_sig, current_spread)
                            await notify(
                                f"💤 <b>{signal.ticker}: спред недостаточен — ожидаю</b>\n"
                                f"📊 Текущий спред: {current_spread:.2f}% (мин {MIN_SPREAD_PCT}%)\n"
                                f"📉 SHORT {signal.short_exchange.upper()} | 📈 LONG {signal.long_exchange.upper()}\n"
                                f"⏳ Мониторю каждые {5}с, макс {15} мин"
                            )
                    except Exception as e:
                        print(f"[Listener] Ошибка кэширования {signal.ticker}: {e}")
                else:
                    print(f"[Listener] {signal.ticker} уже в кэше — пропускаю")

            await notify(f"⏭ Пропущен {signal.ticker}\n{risk.reason}")
            return

        print(f"[Listener] Risk OK: {risk.reason}")
        try:
            trade, msg = await asyncio.wait_for(
                open_position(signal, risk.final_size_short, risk.final_size_long, signal_received_ms=received_ms),
                timeout=60
            )
        except asyncio.TimeoutError:
            print(f"[Listener] TIMEOUT open_position {signal.ticker}")
            await notify(f"⏱ Таймаут открытия {signal.ticker} (60с)")
            _seen.pop(signal.ticker, None)
            return
        except Exception as e:
            print(f"[Listener] Ошибка open_position {signal.ticker}: {e}")
            await notify(f"❌ Ошибка при открытии {signal.ticker}: {e}")
            _seen.pop(signal.ticker, None)
            return

        if trade is None:
            _seen.pop(signal.ticker, None)

    async def _close(self, signal: CloseSignal):
        # Если тикер в очереди ожидания — отменяем
        if signal.ticker in _pending_signals:
            pending = _pending_signals.pop(signal.ticker)
            if pending.task and not pending.task.done():
                pending.task.cancel()
            await notify(f"🚫 <b>{signal.ticker}</b>: отменено ожидание фандинга (получен CLOSE)")
            _seen.pop(signal.ticker, None)
            return

        # Если тикер в кэше сигналов — удаляем
        if is_cached(signal.ticker):
            await remove_signal(signal.ticker)

        try:
            _, msg = await close_position(signal)
        except Exception as e:
            await notify(f"❌ Ошибка при закрытии {signal.ticker}: {e}")
            return
        await notify(msg)