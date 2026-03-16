"""
signal_parser.py — парсер сообщений из Telegram.

OPEN сигнал:
  📈📈#BTW | Spread: 10.42%
  🔴Short MEXC   : $0.026500000
  🟢Long  OURBIT : $0.023999000
  ⚖️MEXC Max Size : $172
  ⚖️OURBIT Max Size : $480

CLOSE сигнал:
  CLOSE BTW  /  ❌ CLOSE BTW_USDT  /  🔴 Close #BTW
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class OpenSignal:
    ticker: str
    symbol: str
    short_exchange: str
    long_exchange: str
    short_price: float
    long_price: float
    spread_pct: float
    funding_short: Optional[float]
    funding_long: Optional[float]
    max_size_short: Optional[float]
    max_size_long: Optional[float]
    raw_text: str
    interval_short: Optional[int] = None  # интервал фандинга в часах (4 или 8), None если неизвестен
    interval_long:  Optional[int] = None

    @property
    def trade_size_usd(self) -> Optional[float]:
        if self.max_size_short is None or self.max_size_long is None:
            return None
        return min(self.max_size_short, self.max_size_long)


@dataclass
class CloseSignal:
    ticker: str
    raw_text: str


def normalize_symbol(ticker: str) -> str:
    t = re.sub(r'[_\-]', '', ticker.strip().upper())
    return t if t.endswith('USDT') else t + 'USDT'


def parse_message(text: str) -> Optional[OpenSignal | CloseSignal]:
    if not text:
        return None
    close = _parse_close(text)
    if close:
        return close
    return _parse_open(text)


def _parse_open(text: str) -> Optional[OpenSignal]:
    lines = text.strip().splitlines()
    ticker, spread_pct = None, None
    for line in lines[:3]:
        m = re.search(r'#(\w+)', line)
        if m:
            ticker = m.group(1).upper()
        m2 = re.search(r'Spread:\s*([\d.]+)%', line, re.I)
        if m2:
            spread_pct = float(m2.group(1))
    if not ticker:
        return None

    short_exchange = short_price = long_exchange = long_price = None
    for line in lines:
        m = re.search(r'short\s+(\w+)\s*:?\s*\$?([\d.]+)', line, re.I)
        if m:
            short_exchange = m.group(1).strip().lower()
            short_price = float(m.group(2))
        m = re.search(r'long\s+(\w+)\s*:?\s*\$?([\d.]+)', line, re.I)
        if m:
            long_exchange = m.group(1).strip().lower()
            long_price = float(m.group(2))

    if not (short_exchange and long_exchange):
        return None

    funding_short = funding_long = None
    max_size_short = max_size_long = None
    interval_short = interval_long = None
    for line in lines:
        if re.search(r'funding\s+' + re.escape(short_exchange), line, re.I):
            m = re.search(r'([+-]?[\d.]+)%', line)
            if m: funding_short = float(m.group(1)) / 100
        elif re.search(r'funding\s+' + re.escape(long_exchange), line, re.I):
            m = re.search(r'([+-]?[\d.]+)%', line)
            if m: funding_long = float(m.group(1)) / 100
        if re.search(r'max\s*size', line, re.I):
            m = re.search(r'\$?([\d,]+\.?\d*)', line)
            val = float(m.group(1).replace(',', '')) if m else None
            if re.search(short_exchange, line, re.I):
                max_size_short = val
            elif re.search(long_exchange, line, re.I):
                max_size_long = val
        # F/Interval MEXC : 4H | 12:00 UTC  →  парсим часы
        m_int = re.search(r'f/interval\s+(\w+)\s*:?\s*(\d+)h', line, re.I)
        if m_int:
            ex_name = m_int.group(1).lower()
            hours   = int(m_int.group(2))
            if ex_name == short_exchange:
                interval_short = hours
            elif ex_name == long_exchange:
                interval_long = hours

    return OpenSignal(
        ticker=ticker, symbol=normalize_symbol(ticker),
        short_exchange=short_exchange, long_exchange=long_exchange,
        short_price=short_price or 0.0, long_price=long_price or 0.0,
        spread_pct=spread_pct or 0.0,
        funding_short=funding_short, funding_long=funding_long,
        max_size_short=max_size_short, max_size_long=max_size_long,
        interval_short=interval_short, interval_long=interval_long,
        raw_text=text,
    )


def _parse_close(text: str) -> Optional[CloseSignal]:
    for pat in [r'close\s+#?(\w+)', r'закрыть?\s+#?(\w+)']:
        m = re.search(pat, text.strip(), re.I)
        if m:
            ticker = re.sub(r'_?USDT$', '', m.group(1).upper(), flags=re.I)
            return CloseSignal(ticker=ticker, raw_text=text)
    return None


if __name__ == '__main__':
    sample = """
📈📈#BTW | Spread: 10.42%
📌 BTW_USDT (COPY: BTW)
🔴Short MEXC   : $0.026500000
🟢Long  OURBIT : $0.023999000
🌗Funding MEXC   : -0.02%
🌓Funding OURBIT : -0.02%
⚖️MEXC Max Size     :  $172
⚖️OURBIT Max Size   :  $480
    """
    sig = parse_message(sample)
    print(f"ticker={sig.ticker} short={sig.short_exchange}@{sig.short_price} long={sig.long_exchange}@{sig.long_price}")
    print(f"spread={sig.spread_pct}% size=${sig.trade_size_usd}")
    print(parse_message("CLOSE BTW"))


# ── Парсер закреплённого сообщения ────────────────────────────
# Формат:
#   🏦 Active Spreads 🏦
#   📗 #LYN | 6.14% (0m) 📗
#   copy
#   🔵 GATE   : 0.3365
#   🟠 OURBIT : 0.31702
#   ====...====
#   📗 #LYN | 6.04% (355m) 📗
#   ...

def _strip_markdown(text: str) -> str:
    """Убирает Telegram Markdown: **жирный**, ```код```, [текст](url)."""
    import re as _re
    text = _re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)   # [текст](url) → текст
    text = _re.sub(r'```[^`]*```', lambda m: m.group(0).replace('`',''), text)  # ```блок```
    text = _re.sub(r'`([^`]*)`', r'\1', text)                # `код`
    text = text.replace('**', '').replace('__', '')           # жирный/курсив
    return text


def parse_pinned(text: str) -> list[OpenSignal]:
    """Парсит закреплённое сообщение с несколькими активными спредами."""
    text = _strip_markdown(text)
    signals = []
    blocks = re.split(r'={3,}[-=]*', text)
    for block in blocks:
        sig = _parse_pinned_block(block.strip())
        if sig:
            signals.append(sig)
    return signals


def _parse_pinned_block(block: str) -> Optional[OpenSignal]:
    """Парсит один блок из закреплённого сообщения."""
    if not block:
        return None
    lines = block.splitlines()

    # Ищем тикер и спред: #LYN | 6.14% (0m)
    ticker, spread_pct = None, None
    for line in lines:
        m = re.search(r'#(\w+)\s*\|\s*([\d.]+)%', line)
        if m:
            ticker    = m.group(1).upper()
            spread_pct = float(m.group(2))
            break
    if not ticker:
        return None

    # Парсим строки с биржами и ценами: GATE : 0.3365
    # Первая цена = short (выше), вторая = long (ниже) — по порядку
    exchange_prices = []
    for line in lines:
        # Строка типа: 🔵 GATE   : 0.3365  или  GATE : 0.3365
        m = re.search(r'([A-Z]{2,10})\s*:\s*([\d.]+)', line)
        if m:
            ex    = m.group(1).strip().lower()
            price = float(m.group(2))
            if ex not in ('copy', 'usdt'):
                exchange_prices.append((ex, price))

    if len(exchange_prices) < 2:
        return None

    # В закреплённом: первая биржа = short (дороже), вторая = long (дешевле)
    short_exchange, short_price = exchange_prices[0]
    long_exchange,  long_price  = exchange_prices[1]

    # Проверяем что short > long (иначе меняем местами)
    if short_price < long_price:
        short_exchange, short_price, long_exchange, long_price = \
            long_exchange, long_price, short_exchange, short_price

    return OpenSignal(
        ticker=ticker,
        symbol=normalize_symbol(ticker),
        short_exchange=short_exchange,
        long_exchange=long_exchange,
        short_price=short_price,
        long_price=long_price,
        spread_pct=spread_pct,
        funding_short=None,   # в закреплённом нет фандинга — фильтр пропускается
        funding_long=None,
        max_size_short=None,
        max_size_long=None,
        raw_text=block,
    )