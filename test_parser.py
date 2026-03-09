"""
test_parser.py — быстрая проверка парсера без запуска бота.
Запуск: python test_parser.py
"""
import sys
sys.path.insert(0, '.')

from telegram.signal_parser import parse_message, OpenSignal, CloseSignal

# ============================================================
# Тестовые сообщения
# ============================================================

SAMPLE_OPEN = """
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

CLOSE_VARIANTS = [
    "CLOSE BTW",
    "❌ CLOSE BTW_USDT",
    "🔴 Close #BTW",
    "close btw",
    "Закрыть BTW",
]

NON_SIGNALS = [
    "Привет, как дела?",
    "📊 Рынок сегодня нестабилен",
    "",
]

# ============================================================

def test_open():
    print("=" * 50)
    print("ТЕСТ: OPEN сигнал")
    print("=" * 50)
    sig = parse_message(SAMPLE_OPEN)
    assert isinstance(sig, OpenSignal), f"Ожидал OpenSignal, получил {type(sig)}"
    assert sig.ticker == "BTW"
    assert sig.symbol == "BTWUSDT"
    assert sig.short_exchange == "mexc"
    assert sig.long_exchange == "ourbit"
    assert abs(sig.short_price - 0.0265) < 0.0001
    assert abs(sig.long_price - 0.023999) < 0.0001
    assert sig.spread_pct == 10.42
    assert sig.max_size_short == 172.0
    assert sig.max_size_long == 480.0
    assert sig.trade_size_usd == 172.0  # min(172, 480)

    print(f"  ✅ ticker:         {sig.ticker}")
    print(f"  ✅ symbol:         {sig.symbol}")
    print(f"  ✅ short_exchange: {sig.short_exchange} @ {sig.short_price}")
    print(f"  ✅ long_exchange:  {sig.long_exchange} @ {sig.long_price}")
    print(f"  ✅ spread_pct:     {sig.spread_pct}%")
    print(f"  ✅ max_size_short: ${sig.max_size_short}")
    print(f"  ✅ max_size_long:  ${sig.max_size_long}")
    print(f"  ✅ trade_size:     ${sig.trade_size_usd} (min)")
    print(f"  ✅ funding_short:  {sig.funding_short}")
    print(f"  ✅ funding_long:   {sig.funding_long}")


def test_close():
    print("\n" + "=" * 50)
    print("ТЕСТ: CLOSE сигналы")
    print("=" * 50)
    for text in CLOSE_VARIANTS:
        sig = parse_message(text)
        if isinstance(sig, CloseSignal):
            print(f"  ✅ '{text}' → ticker={sig.ticker}")
        else:
            print(f"  ❌ '{text}' → не распознан (получил {type(sig)})")


def test_non_signals():
    print("\n" + "=" * 50)
    print("ТЕСТ: Не-сигналы (должны вернуть None)")
    print("=" * 50)
    for text in NON_SIGNALS:
        sig = parse_message(text)
        if sig is None:
            print(f"  ✅ '{text[:30]}' → None (правильно)")
        else:
            print(f"  ❌ '{text[:30]}' → {type(sig)} (неправильно!)")


if __name__ == "__main__":
    test_open()
    test_close()
    test_non_signals()
    print("\n✅ Все тесты пройдены!")
