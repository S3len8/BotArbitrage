"""
signal_parser.py — парсер сообщений из Telegram.

Поддерживает два типа сообщений:

1. OPEN-сигнал (пример из канала):
   📈📈#BTW | Spread: 10.42%
   📌 BTW_USDT (COPY: BTW)
   🔴Short MEXC   : $0.026500000
   🟢Long  OURBIT : $0.023999000
   ...
   ⚖️MEXC Max Size     :  $172
   ⚖️OURBIT Max Size   :  $480

2. CLOSE-сигнал:
   CLOSE BTW
   или: ❌ CLOSE BTW_USDT
   или: 🔴 Close #BTW
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class OpenSignal:
    """Структурированный сигнал на открытие позиции."""
    ticker: str           # BTW
    symbol: str           # BTCUSDT (нормализованный)
    short_exchange: str   # mexc
    long_exchange: str    # ourbit
    short_price: float    # 0.026500000
    long_price: float     # 0.023999000
    spread_pct: float     # 10.42
    funding_short: Optional[float]  # -0.02% → -0.0002
    funding_long: Optional[float]
    max_size_short: Optional[float]  # 172.0
    max_size_long: Optional[float]   # 480.0
    raw_text: str         # оригинальное сообщение

    @property
    def trade_size_usd(self) -> Optional[float]:
        """
        Размер сделки = min(max_size_short, max_size_long).
        Обе ноги открываются на одинаковую сумму.
        Возвращает None если max_size не удалось распарсить.
        """
        if self.max_size_short is None or self.max_size_long is None:
            return None
        return min(self.max_size_short, self.max_size_long)


@dataclass
class CloseSignal:
    """Сигнал на закрытие позиции."""
    ticker: str   # BTW
    raw_text: str


def normalize_symbol(ticker: str) -> str:
    """
    BTW → BTWUSDT
    BTW_USDT → BTWUSDT
    Все символы приводим к формату XXXUSDT (без разделителей).
    """
    ticker = ticker.strip().upper()
    ticker = re.sub(r'[_\-]', '', ticker)
    if not ticker.endswith('USDT'):
        ticker += 'USDT'
    return ticker


def normalize_exchange(name: str) -> str:
    """MEXC → mexc, OurBit → ourbit"""
    return name.strip().lower()


def _parse_price(text: str) -> Optional[float]:
    """Извлекает цену из строки типа '$0.026500000'"""
    m = re.search(r'\$?([\d.]+)', text)
    return float(m.group(1)) if m else None


def _parse_funding(text: str) -> Optional[float]:
    """
    Парсит funding из строки типа '-0.02%'.
    Возвращает как десятичную дробь: -0.02% → -0.0002
    """
    m = re.search(r'([+-]?[\d.]+)%', text)
    if m:
        return float(m.group(1)) / 100
    return None


def _parse_max_size(text: str) -> Optional[float]:
    """Парсит '$172' или '172' → 172.0"""
    m = re.search(r'\$?([\d,]+\.?\d*)', text)
    if m:
        return float(m.group(1).replace(',', ''))
    return None


def parse_open_signal(text: str) -> Optional[OpenSignal]:
    """
    Парсит OPEN-сигнал из текста Telegram-сообщения.
    Возвращает OpenSignal или None если сообщение не распознано.

    Алгоритм:
    1. Ищем тикер в первой строке (#BTW)
    2. Находим строки Short/Long для определения бирж и цен
    3. Извлекаем funding и max size
    """
    lines = text.strip().splitlines()

    # --- Тикер из первой строки: "#BTW | Spread: 10.42%" ---
    ticker = None
    spread_pct = None
    for line in lines[:3]:
        m = re.search(r'#(\w+)', line)
        if m:
            ticker = m.group(1).upper()
        m_spread = re.search(r'Spread:\s*([\d.]+)%', line, re.IGNORECASE)
        if m_spread:
            spread_pct = float(m_spread.group(1))

    if not ticker:
        return None

    # --- Биржи и цены из строк Short/Long ---
    # Формат: "🔴Short MEXC   : $0.026500000"
    short_exchange = None
    short_price = None
    long_exchange = None
    long_price = None

    for line in lines:
        # Short строка
        m = re.search(
            r'short\s+(\w+)\s*:?\s*\$?([\d.]+)',
            line, re.IGNORECASE
        )
        if m:
            short_exchange = normalize_exchange(m.group(1))
            short_price = float(m.group(2))
            continue

        # Long строка
        m = re.search(
            r'long\s+(\w+)\s*:?\s*\$?([\d.]+)',
            line, re.IGNORECASE
        )
        if m:
            long_exchange = normalize_exchange(m.group(1))
            long_price = float(m.group(2))

    if not (short_exchange and long_exchange):
        return None

    # --- Funding ---
    # Формат: "🌗Funding MEXC   : -0.02%"
    funding_short = None
    funding_long = None
    for line in lines:
        if re.search(r'funding\s+' + re.escape(short_exchange), line, re.IGNORECASE):
            funding_short = _parse_funding(line)
        elif re.search(r'funding\s+' + re.escape(long_exchange), line, re.IGNORECASE):
            funding_long = _parse_funding(line)

    # --- Max Size ---
    # Формат: "⚖️MEXC Max Size     :  $172"
    max_size_short = None
    max_size_long = None
    for line in lines:
        if re.search(r'max\s*size', line, re.IGNORECASE):
            if re.search(short_exchange, line, re.IGNORECASE):
                max_size_short = _parse_max_size(line)
            elif re.search(long_exchange, line, re.IGNORECASE):
                max_size_long = _parse_max_size(line)

    return OpenSignal(
        ticker=ticker,
        symbol=normalize_symbol(ticker),
        short_exchange=short_exchange,
        long_exchange=long_exchange,
        short_price=short_price or 0.0,
        long_price=long_price or 0.0,
        spread_pct=spread_pct or 0.0,
        funding_short=funding_short,
        funding_long=funding_long,
        max_size_short=max_size_short,
        max_size_long=max_size_long,
        raw_text=text,
    )


# Паттерны для CLOSE-сигнала
# Поддерживаем: "CLOSE BTW", "❌ CLOSE BTW_USDT", "🔴 Close #BTW", "close btw"
_CLOSE_PATTERNS = [
    re.compile(r'close\s+#?(\w+)', re.IGNORECASE),
    re.compile(r'закрыть?\s+#?(\w+)', re.IGNORECASE),
]


def parse_close_signal(text: str) -> Optional[CloseSignal]:
    """
    Парсит CLOSE-сигнал.
    Возвращает CloseSignal или None.
    """
    cleaned = text.strip()
    for pattern in _CLOSE_PATTERNS:
        m = pattern.search(cleaned)
        if m:
            raw_ticker = m.group(1).upper()
            # Убираем _USDT суффикс если есть — храним только тикер
            ticker = re.sub(r'_?USDT$', '', raw_ticker, flags=re.IGNORECASE)
            return CloseSignal(ticker=ticker, raw_text=text)
    return None


def parse_message(text: str) -> Optional[OpenSignal | CloseSignal]:
    """
    Главная точка входа. Пробует распознать любой тип сигнала.
    Возвращает OpenSignal, CloseSignal, или None.
    """
    if not text:
        return None

    # Сначала пробуем CLOSE — он короткий и специфичный
    close = parse_close_signal(text)
    if close:
        return close

    # Потом OPEN — более сложный парсинг
    return parse_open_signal(text)


# ============================================================
# Тест при запуске напрямую
# ============================================================
if __name__ == "__main__":
    sample_open = """
📈📈#BTW | Spread: 10.42%
📌 BTW_USDT (COPY: BTW)
🔴Short MEXC   : $0.026500000
🟢Long  OURBIT : $0.023999000
🌗Funding MEXC   : -0.02%
🌓Funding OURBIT : -0.02%
🔍 Additional Info:
⚖️MEXC Max Size     :  $172
⚖️OURBIT Max Size   :  $480
⏱️F/Interval MEXC   :  4H | 16:00 UTC
⏱️F/Interval OURBIT :  4H | 16:00 UTC
📝 All Exchanges Overview:
🟠OURBIT     : $0.023297
⚪️MEXC       : $0.025403
    """

    sample_close = "CLOSE BTW"

    sig = parse_message(sample_open)
    print("=== OPEN SIGNAL ===")
    print(f"Ticker:         {sig.ticker}")
    print(f"Symbol:         {sig.symbol}")
    print(f"Short on:       {sig.short_exchange} @ {sig.short_price}")
    print(f"Long on:        {sig.long_exchange} @ {sig.long_price}")
    print(f"Spread:         {sig.spread_pct}%")
    print(f"Funding short:  {sig.funding_short}")
    print(f"Funding long:   {sig.funding_long}")
    print(f"Max size short: ${sig.max_size_short}")
    print(f"Max size long:  ${sig.max_size_long}")
    print(f"Trade size:     ${sig.trade_size_usd}")

    print("\n=== CLOSE SIGNAL ===")
    close_sig = parse_message(sample_close)
    print(f"Ticker: {close_sig.ticker}")
