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
import time
from dataclasses import dataclass
from typing import Optional

from settings import (
    MAX_OPEN_POSITIONS, LEVERAGE, MIN_VOLUME_USD,
    MAX_FUNDING_DIFF_PCT, MAX_FUNDING_ABS_PCT,
    ATR_VOLATILE_MULTIPLIER, DEPTH_MIN_RATIO, INDEX_DEVIATION_PCT,
)
MIN_SPREAD_PCT = 3.5
from exchanges import BaseExchange


def _calc_atr(candles: list[list]) -> float:
    trs = []
    for i, c in enumerate(candles):
        h, l, c_prev = float(c[2]), float(c[3]), float(c[4])
        if i == 0:
            tr = h - l
        else:
            prev_close = float(candles[i-1][4])
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


async def _check_atr_filter(symbol: str) -> tuple[bool, str]:
    try:
        import requests as _req
        from settings import PROXY
        proxies = {'http': PROXY, 'https': PROXY} if PROXY else None
        url = "https://api.binance.com/api/v3/klines"
        short_raw = _req.get(url, params={"symbol": symbol.upper(), "interval": "1m", "limit": 15}, timeout=10, proxies=proxies)
        long_raw = _req.get(url, params={"symbol": symbol.upper(), "interval": "1m", "limit": 60}, timeout=10, proxies=proxies)
        short_raw.raise_for_status()
        long_raw.raise_for_status()
        short_candles = short_raw.json()
        long_candles = long_raw.json()
        if not short_candles or not long_candles:
            return True, "ATR: insufficient data"
        short_atr = _calc_atr(short_candles)
        last_price = float(short_candles[-1][4])
        long_atr = _calc_atr(long_candles)
        if long_atr > 0:
            ratio = short_atr / long_atr
        else:
            ratio = 0
        pct_short = short_atr / last_price * 100
        pct_long = long_atr / last_price * 100
        if ratio > ATR_VOLATILE_MULTIPLIER:
            return False, (f"ATR-фильтр: волатильность {ratio:.1f}x выше нормы "
                          f"(short={pct_short:.2f}%, long={pct_long:.2f}%)")
        return True, f"ATR ok: {ratio:.2f}x"
    except Exception as e:
        return True, f"ATR: ошибка ({e})"


async def _check_top_gainers_correlation(symbol: str, direction: str) -> tuple[bool, str]:
    """Проверяет корреляцию с BTC/ETH для монет из Top Gainers/Losers.
    direction: 'up' если цена растёт, 'down' если падает.
    Returns (ok, reason).
    """
    try:
        import requests as _req
        from settings import PROXY
        proxies = {'http': PROXY, 'https': PROXY} if PROXY else None

        bin_symbol = symbol.upper().replace('/', '').replace(':USDT', '')
        if not bin_symbol.endswith('USDT'):
            bin_symbol += 'USDT'
        r = _req.get("https://api.binance.com/api/v3/ticker/24hr",
                     params={"symbol": bin_symbol},
                     timeout=10, proxies=proxies)
        if r.status_code == 404:
            return True, "TopGainers: not found"
        r.raise_for_status()
        ticker_data = r.json()
        price_change_pct = float(ticker_data.get('priceChangePercent', 0))

        if abs(price_change_pct) < 3.0:
            return True, f"TopGainers: движение {price_change_pct:.1f}% — безопасно"

        r_btc = _req.get("https://api.binance.com/api/v3/ticker/24hr",
                         params={"symbol": "BTCUSDT"}, timeout=10, proxies=proxies)
        r_eth = _req.get("https://api.binance.com/api/v3/ticker/24hr",
                         params={"symbol": "ETHUSDT"}, timeout=10, proxies=proxies)
        r_btc.raise_for_status()
        r_eth.raise_for_status()
        btc_change = float(r_btc.json().get('priceChangePercent', 0))
        eth_change = float(r_eth.json().get('priceChangePercent', 0))

        btc_confirms = btc_change > 0.5 if direction == 'up' else btc_change < -0.5
        eth_confirms = eth_change > 0.5 if direction == 'up' else eth_change < -0.5

        if not btc_confirms and not eth_confirms:
            return False, (f"⚠️ {symbol}: памповый спред? {price_change_pct:+.1f}% "
                          f"без подтверждения BTC({btc_change:+.1f}%), ETH({eth_change:+.1f}%)")
        return True, f"TopGainers: подтверждено BTC/ETH"
    except Exception as e:
        return True, f"TopGainers: ошибка ({e})"
    """ATR volatility filter. Returns (ok, reason). Rejects if short-term ATR >> long-term ATR."""
    try:
        import requests
        url = "https://api.binance.com/api/v3/klines"
        short_raw = requests.get(url, params={"symbol": symbol.upper(), "interval": "1m", "limit": 60}, timeout=10)
        long_raw = requests.get(url, params={"symbol": symbol.upper(), "interval": "1m", "limit": 60}, timeout=10)
        short_raw.raise_for_status()
        long_raw.raise_for_status()
        short_candles = short_raw.json()
        long_candles = long_raw.json()
        if not short_candles or not long_candles:
            return True, "ATR: insufficient data"
        short_atr = _calc_atr(short_candles[-15:])
        last_price = float(short_candles[-1][4])
        long_atr = _calc_atr(long_candles[-24:])
        if long_atr > 0:
            ratio = short_atr / long_atr
        else:
            ratio = 0
        pct_short = short_atr / last_price * 100
        pct_long = long_atr / last_price * 100
        if ratio > ATR_VOLATILE_MULTIPLIER:
            return False, (f"ATR-фильтр: волатильность {ratio:.1f}x выше нормы "
                          f"(short={pct_short:.2f}%, long={pct_long:.2f}%)")
        return True, f"ATR ok: {ratio:.2f}x"
    except Exception as e:
        return True, f"ATR: ошибка ({e})"
from db import get_open_trade, get_all_open_trades
from signal_parser import OpenSignal

# Глобальный список выключенных бирж — управляется из GUI
_disabled_exchanges: set[str] = set()
_allocated_capital: dict[str, float] = {}
_ignored_tickers: set[str] = set()
_ignored_last_notified: dict[str, float] = {}
_IGNORED_NOTIFY_COOLDOWN: int = 600


def set_ignored_tickers(tickers: list[str]):
    global _ignored_tickers
    _ignored_tickers = {t.upper() for t in tickers}


def is_ticker_ignored(ticker: str) -> bool:
    return ticker.upper() in _ignored_tickers


def _notify_ignored_if_needed(ticker: str, spread_pct: float, short_ex: str, long_ex: str):
    now = time.time()
    last = _ignored_last_notified.get(ticker.upper(), 0)
    if now - last < _IGNORED_NOTIFY_COOLDOWN:
        return
    _ignored_last_notified[ticker.upper()] = now
    try:
        from notifier import notify
        asyncio.create_task(notify(
            f"🔇 <b>{ticker}: сигнал доступен</b>\n"
            f"📊 Спред: {spread_pct:.2f}%\n"
            f"📉 SHORT {short_ex.upper()} | 📈 LONG {long_ex.upper()}\n"
            f"⏳ В чёрном списке — пропускаю"
        ))
    except Exception:
        pass


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
        _notify_ignored_if_needed(signal.ticker, signal.spread_pct,
                                   signal.short_exchange, signal.long_exchange)
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

    # 4b. ATR волатильність
    atr_ok, atr_reason = await _check_atr_filter(signal.symbol)
    print(f"[Risk] {signal.ticker}: {atr_reason}")
    if not atr_ok:
        return RiskResult(ok=False, reason=f"ATR-фильтр: {atr_reason}")

    # 4c. Индекс-ценовой фильтр
    try:
        import requests
        ticker_b = signal.symbol.replace('/', '').replace(':USDT', '')
        r = requests.get('https://fapi.binance.com/fapi/v1/premiumIndex',
                         params={'symbol': ticker_b}, timeout=10, proxies={
            'http': __import__('settings').PROXY, 'https': __import__('settings').PROXY
        } if __import__('settings').PROXY else None)
        r.raise_for_status()
        d = r.json()
        index_price = float(d.get('indexPrice', 0))
        mark_price_s = float(d.get('markPrice', 0))
        if index_price > 0 and mark_price_s > 0:
            dev_s = abs(mark_price_s - index_price) / index_price * 100
            print(f"[Risk] {signal.ticker}: SH index deviation={dev_s:.2f}%")
            if dev_s > INDEX_DEVIATION_PCT:
                print(f"[Risk] {signal.ticker}: ⚠️ HIGH INDEX DEVIATION — рекомендуем лимитные ордера")
    except Exception as e:
        print(f"[Risk] {signal.ticker}: index price check error: {e}")

    # 4d. Top Gainers корреляція з BTC/ETH
    direction = 'up' if short_price > long_price else 'down'
    gainer_ok, gainer_reason = await _check_top_gainers_correlation(signal.symbol, direction)
    print(f"[Risk] {signal.ticker}: {gainer_reason}")
    if not gainer_ok:
        return RiskResult(ok=False, reason=f"TopGainers: {gainer_reason}")

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

    if real_spread >= 5.0:
        conf_mult = 1.0
    elif real_spread >= 3.0:
        conf_mult = 0.5
    else:
        conf_mult = 1.0
    final_size_short *= conf_mult
    final_size_long *= conf_mult

    print(f"[Risk] {signal.ticker}: short={limit_short}=${final_size_short:.2f}, long={limit_long}=${final_size_long:.2f} "
          f"(conf={conf_mult:.0%})")

    vol_info = ""
    if not isinstance(vol_short, Exception) and vol_short:
        vol_info += f" | vol_short=${vol_short:,.0f}"
    if not isinstance(vol_long, Exception) and vol_long:
        vol_info += f" | vol_long=${vol_long:,.0f}"

    return RiskResult(
        ok=True,
        reason=(f"✅ {signal.ticker}: спред {real_spread:.2f}% (conf {conf_mult:.0%}), "
                f"short ${final_size_short:.2f}, long ${final_size_long:.2f}, "
                f"плечо {LEVERAGE}×, позиций {len(open_trades)}/{MAX_OPEN_POSITIONS}{vol_info}"),
        final_size_usd=min(final_size_short, final_size_long),
        final_size_short=final_size_short,
        final_size_long=final_size_long,
    )