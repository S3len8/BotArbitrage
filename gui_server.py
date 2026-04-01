"""
gui_server.py — FastAPI сервер дашборда.

Новое:
- /api/bot/start  — запускает Telegram-листенер (торговля начинается)
- /api/bot/stop   — останавливает листенер
- /api/bot/status — текущее состояние бота
- /api/db/status  — проверка подключения к PostgreSQL
- Вся торговля управляется кнопкой START в GUI, не автозапуском
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from settings import LEVERAGE, BALANCE_ALERT_PCT, MAX_OPEN_POSITIONS, MIN_VOLUME_USD

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_ws_clients: list[WebSocket] = []
EXCHANGES = ['binance', 'bybit', 'bitget', 'mexc', 'kucoin', 'gate']

# ── Глобальное состояние бота ─────────────────────────────────
_bot_state = {
    "running":    False,
    "started_at": None,
    "error":      None,
}
_listener_task: asyncio.Task | None = None
_listener_instance = None
_db_ok = False  # флаг: PostgreSQL доступен

# Выделенный капитал на каждой бирже (чистые деньги, без плеча)
# 0 = не задан (используется весь баланс биржи)
_allocated: dict[str, float] = {ex: 0.0 for ex in EXCHANGES}

# История фандинга для открытых позиций
# {trade_id: [{'time': iso, 'sf': rate, 'lf': rate, 'interval_s': h, 'interval_l': h,
#              'next_s': ts, 'next_l': ts, 'short_ex': str, 'long_ex': str}, ...]}
_funding_history: dict[int, list] = {}
MAX_FUNDING_HISTORY = 100  # максимум записей на позицию


# ── Startup ───────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global _db_ok
    _db_ok = _try_db_init()
    if _db_ok:
        _load_exchange_settings_from_db()
    asyncio.create_task(_broadcast_loop())
    # В startup() добавьте загрузку списка:
    if _db_ok:
        from db import get_ignored_symbols
        from risk_manager import set_ignored_tickers
        tickers = get_ignored_symbols()
        set_ignored_tickers(tickers)


def _try_db_init() -> bool:
    """Пытается инициализировать БД. Возвращает True если успешно."""
    try:
        from db import db_init
        db_init()
        print("[DB] PostgreSQL connected OK")
        return True
    except Exception as e:
        print(f"[DB] Connection failed: {e}")
        print("[DB] GUI работает без БД. Исправь DATABASE_URL в .env и перезапусти.")
        return False


def _load_exchange_settings_from_db():
    """Загружает настройки бирж из БД и применяет их к risk_manager и _allocated."""
    global _allocated
    try:
        from db import load_exchange_settings
        from risk_manager import set_exchange_enabled, set_exchange_allocated
        settings = load_exchange_settings()
        for ex, cfg in settings.items():
            set_exchange_enabled(ex, cfg['enabled'])
            set_exchange_allocated(ex, cfg['allocated_usd'])
            _allocated[ex] = cfg['allocated_usd']
        if settings:
            print(f"[DB] Загружены настройки бирж: {list(settings.keys())}")
    except Exception as e:
        print(f"[DB] Ошибка загрузки настроек бирж: {e}")


# ── Bot control ───────────────────────────────────────────────

@app.post("/api/bot/start")
async def bot_start():
    global _listener_task, _listener_instance, _bot_state

    if _bot_state["running"]:
        return {"ok": False, "msg": "Бот уже запущен"}

    if not _db_ok:
        return {"ok": False, "msg": "PostgreSQL недоступен. Проверь DATABASE_URL в .env и перезапусти приложение."}

    try:
        from listener import SignalListener
        _listener_instance = SignalListener()
        _listener_task = asyncio.create_task(_run_listener())
        _bot_state["running"]    = True
        _bot_state["started_at"] = datetime.now(timezone.utc).isoformat()
        _bot_state["error"]      = None
        print("[Bot] Started via GUI")
        # Запускаем фандинг-монитор
        from order_executor import start_funding_monitor
        await start_funding_monitor()
        await _broadcast_status()
        return {"ok": True, "msg": "Бот запущен. Слушаю канал..."}
    except Exception as e:
        _bot_state["error"] = str(e)
        return {"ok": False, "msg": f"Ошибка запуска: {e}"}


@app.post("/api/bot/stop")
async def bot_stop():
    global _listener_task, _listener_instance, _bot_state

    if not _bot_state["running"]:
        return {"ok": False, "msg": "Бот не запущен"}

    try:
        if _listener_instance:
            await _listener_instance.stop()
        if _listener_task and not _listener_task.done():
            _listener_task.cancel()
            try:
                await _listener_task
            except (asyncio.CancelledError, Exception):
                pass
        _bot_state["running"]    = False
        _bot_state["started_at"] = None
        _bot_state["error"]      = None
        _listener_task           = None
        _listener_instance       = None
        print("[Bot] Stopped via GUI")
        await _broadcast_status()
        return {"ok": True, "msg": "Бот остановлен"}
    except Exception as e:
        return {"ok": False, "msg": f"Ошибка остановки: {e}"}


async def _run_listener():
    """Задача которая держит листенер живым."""
    global _bot_state
    try:
        await _listener_instance.start()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[Bot] Crashed: {e}")
        _bot_state["running"] = False
        _bot_state["error"]   = str(e)
        await _broadcast_status()


@app.get("/api/bot/status")
async def bot_status():
    return {
        **_bot_state,
        "db_ok": _db_ok,
    }


@app.post("/api/db/reconnect")
async def db_reconnect():
    """Повторная попытка подключения к PostgreSQL без перезапуска приложения."""
    global _db_ok
    _db_ok = _try_db_init()
    return {"ok": _db_ok, "msg": "Подключено" if _db_ok else "Не удалось подключиться. Проверь DATABASE_URL"}


# ── Exchange control ──────────────────────────────────────────

@app.get("/api/exchanges")
async def api_exchanges():
    """Список бирж и их статус (включена/выключена)."""
    from risk_manager import get_disabled_exchanges
    disabled = get_disabled_exchanges()
    return {
        ex: {"enabled": ex not in disabled}
        for ex in EXCHANGES
    }


@app.post("/api/exchanges/toggle")
async def exchange_toggle(exchange: str, enabled: bool):
    """Включить/выключить биржу. ?exchange=mexc&enabled=false"""
    from risk_manager import set_exchange_enabled, get_allocated
    exchange = exchange.lower()
    if exchange not in EXCHANGES:
        return {"ok": False, "msg": f"Неизвестная биржа: {exchange}"}
    set_exchange_enabled(exchange, enabled)
    state = "ENABLED" if enabled else "DISABLED"
    print(f"[GUI] Exchange {exchange.upper()} {state}")
    # Сохраняем в БД
    if _db_ok:
        try:
            from db import save_exchange_setting
            save_exchange_setting(exchange, enabled, get_allocated(exchange))
        except Exception as e:
            print(f"[DB] save_exchange_setting: {e}")
    return {"ok": True, "exchange": exchange, "enabled": enabled}


@app.post("/api/exchanges/allocated")
async def set_allocated(exchange: str, amount: float):
    """Задать выделенный капитал для биржи (чистые деньги без плеча).
    amount=0 означает использовать весь баланс биржи."""
    global _allocated
    exchange = exchange.lower()
    if exchange not in EXCHANGES:
        return {"ok": False, "msg": f"Неизвестная биржа: {exchange}"}
    if amount < 0:
        return {"ok": False, "msg": "Сумма не может быть отрицательной"}
    _allocated[exchange] = amount
    from risk_manager import set_exchange_allocated, is_exchange_enabled
    set_exchange_allocated(exchange, amount)
    print(f"[GUI] {exchange.upper()} allocated=${amount:.2f}")
    # Сохраняем в БД
    if _db_ok:
        try:
            from db import save_exchange_setting
            save_exchange_setting(exchange, is_exchange_enabled(exchange), amount)
        except Exception as e:
            print(f"[DB] save_exchange_setting: {e}")
    return {"ok": True, "exchange": exchange, "allocated": amount}


# Алиасы для совместимости
@app.post("/api/exchanges/{exchange}/enable")
async def exchange_enable(exchange: str):
    from risk_manager import set_exchange_enabled, get_allocated
    exchange = exchange.lower()
    if exchange not in EXCHANGES:
        return {"ok": False, "msg": f"Неизвестная биржа: {exchange}"}
    set_exchange_enabled(exchange, True)
    print(f"[GUI] Exchange {exchange.upper()} ENABLED")
    if _db_ok:
        try:
            from db import save_exchange_setting
            save_exchange_setting(exchange, True, get_allocated(exchange))
        except Exception as e:
            print(f"[DB] save_exchange_setting: {e}")
    return {"ok": True, "exchange": exchange, "enabled": True}


@app.post("/api/exchanges/{exchange}/disable")
async def exchange_disable(exchange: str):
    from risk_manager import set_exchange_enabled, get_allocated
    exchange = exchange.lower()
    if exchange not in EXCHANGES:
        return {"ok": False, "msg": f"Неизвестная биржа: {exchange}"}
    set_exchange_enabled(exchange, False)
    print(f"[GUI] Exchange {exchange.upper()} DISABLED")
    if _db_ok:
        try:
            from db import save_exchange_setting
            save_exchange_setting(exchange, False, get_allocated(exchange))
        except Exception as e:
            print(f"[DB] save_exchange_setting: {e}")
    return {"ok": True, "exchange": exchange, "enabled": False}


# ── Data endpoints ────────────────────────────────────────────

@app.get("/api/balances")
async def api_balances():
    if not _db_ok:
        return {ex: {"live": None, "saved": None, "display": 0} for ex in EXCHANGES}
    from exchanges import get_all_balances
    from db import get_latest_balances
    try:
        live = await get_all_balances(EXCHANGES)
    except Exception:
        live = {}
    saved = get_latest_balances()
    return {
        ex: {
            "live":    round(live.get(ex), 4)  if live.get(ex)  is not None else None,
            "saved":   round(saved.get(ex), 4) if saved.get(ex) is not None else None,
            "display": round(live.get(ex) or saved.get(ex) or 0, 4),
        }
        for ex in EXCHANGES
    }


@app.get("/api/balances/history")
async def api_balance_history(exchange: str, limit: int = 100):
    if not _db_ok:
        return []
    from db import get_balance_history
    return get_balance_history(exchange, limit)


@app.get("/api/trades")
async def api_trades(limit: int = 500):
    if not _db_ok:
        return []
    from db import get_trade_history
    return [_t(t) for t in get_trade_history(limit)]


@app.get("/api/trades/open")
async def api_open_trades():
    if not _db_ok:
        return []
    from db import get_all_open_trades
    return [_t(t) for t in get_all_open_trades()]


@app.get("/api/positions/live")
async def api_positions_live():
    """Открытые позиции с реальными данными с бирж без мерцания."""
    if not _db_ok: return []
    from db import get_all_open_trades, update_trade
    from exchanges import create_exchange
    import asyncio

    trades = get_all_open_trades()
    if not trades: return []

    # Ограничиваем количество одновременных запросов к биржам (защита от бана)
    sem = asyncio.Semaphore(5)

    async def fetch_trade_data(t):
        async with sem:
            item = _t(t)  # Превращаем объект БД в словарь
            try:
                s_ex = create_exchange(t.short_exchange)
                l_ex = create_exchange(t.long_exchange)

                # Запрашиваем всё параллельно
                s_price, l_price, s_pos, l_pos, s_fund, l_fund = await asyncio.gather(
                    s_ex.get_price(t.symbol),
                    l_ex.get_price(t.symbol),
                    s_ex.get_open_position(t.symbol),
                    l_ex.get_open_position(t.symbol),
                    s_ex.get_funding_rate(t.symbol),
                    l_ex.get_funding_rate(t.symbol),
                    return_exceptions=True,
                )
                await asyncio.gather(s_ex.close(), l_ex.close(), return_exceptions=True)

                # Обработка цен и PnL
                sp = float(s_price) if isinstance(s_price, (int, float, str)) else t.short_entry_price
                lp = float(l_price) if isinstance(l_price, (int, float, str)) else t.long_entry_price

                # Берем PnL напрямую из ответа биржи (unrealized_pnl)
                s_pnl = s_pos.get('unrealized_pnl', 0) if (not isinstance(s_pos, Exception) and s_pos) else 0
                l_pnl = l_pos.get('unrealized_pnl', 0) if (not isinstance(l_pos, Exception) and l_pos) else 0

                # Расчет накопленного фандинга (упрощенно)
                fund_earned = item.get('fund_earned', 0)  # Можно добавить логику накопления из истории

                item.update({
                    'cur_short_price': sp,
                    'cur_long_price': lp,
                    'short_unrealized': round(s_pnl, 4),
                    'long_unrealized': round(l_pnl, 4),
                    'unrealized_pnl': round(s_pnl + l_pnl + fund_earned, 4),
                    'short_on_exchange': not isinstance(s_pos, Exception) and s_pos is not None,
                    'long_on_exchange': not isinstance(l_pos, Exception) and l_pos is not None,
                })
            except Exception as e:
                item['live_error'] = str(e)
            return item

    tasks = [fetch_trade_data(t) for t in trades]
    return await asyncio.gather(*tasks)


@app.get("/api/positions/{trade_id}/funding-history")
async def api_funding_history(trade_id: int):
    """История фандинга для конкретной позиции."""
    history = _funding_history.get(trade_id, [])
    return list(reversed(history))  # новые сверху


@app.post("/api/positions/{trade_id}/mark-closed")
async def api_mark_closed(trade_id: int):
    """Принудительно помечает позицию закрытой в БД и останавливает мониторинг рисков."""
    if not _db_ok:
        return {"ok": False, "error": "БД не подключена"}
    try:
        from db import get_trade_by_id, update_trade
        t = get_trade_by_id(trade_id)
        if not t:
            return {"ok": False, "error": f"Сделка #{trade_id} не найдена"}

        # Если уже закрыта - ничего не делаем
        if t.status == 'closed':
            return {"ok": True, "msg": "Уже закрыта"}

        # 1. Меняем статус на закрытый принудительно
        t.status = 'closed'
        t.closed_at = datetime.now(timezone.utc).isoformat()

        # 2. Пытаемся получить последние цены для финального отчета (необязательно)
        try:
            from exchanges import create_exchange
            s_ex = create_exchange(t.short_exchange)
            l_ex = create_exchange(t.long_exchange)
            sp, lp = await asyncio.gather(s_ex.get_price(t.symbol), l_ex.get_price(t.symbol))
            await asyncio.gather(s_ex.close(), l_ex.close())
            t.short_close_price = sp
            t.long_close_price = lp

            # Считаем примерный PnL
            fees = (t.fee_short_usd or 0) + (t.fee_long_usd or 0)
            if t.short_entry_price and t.short_qty:
                t.short_pnl_usd = round((t.short_entry_price - sp) * t.short_qty, 6)
            if t.long_entry_price and t.long_qty:
                t.long_pnl_usd = round((lp - t.long_entry_price) * t.long_qty, 6)
            if t.short_pnl_usd is not None and t.long_pnl_usd is not None:
                t.net_pnl_usd = round(t.short_pnl_usd + t.long_pnl_usd - fees, 6)
        except Exception as e:
            print(f"[GUI] Не удалось обновить цены при ручном закрытии: {e}")

        # 3. Сохраняем изменения в БД
        update_trade(t)

        # 4. ОЧИЩАЕМ КЭШИ (Чтобы алерты сразу прекратились!)
        _funding_history.pop(trade_id, None)
        try:
            from order_executor import _funding_cache, _margin_warn_cooldown
            _funding_cache.pop(trade_id, None)
            _margin_warn_cooldown.pop(trade_id, None)
        except:
            pass

        # Уведомление в Telegram
        try:
            from notifier import notify
            pnl = t.net_pnl_usd
            pnl_str = f"${pnl:+.4f}" if pnl is not None else "—"
            pnl_emoji = "💰" if pnl and pnl > 0 else "💸"
            await notify(
                f"🔒 <b>Закрыто вручную: {t.ticker}</b>\n"
                f"📉 SHORT {t.short_exchange.upper()} @ "
                f"${t.short_close_price:.6f if t.short_close_price else '—'}\n"
                f"📈 LONG  {t.long_exchange.upper()} @ "
                f"${t.long_close_price:.6f if t.long_close_price else '—'}\n"
                f"PnL: short {'+' if (t.short_pnl_usd or 0) >= 0 else ''}${t.short_pnl_usd:.4f if t.short_pnl_usd else 0} | "
                f"long {'+' if (t.long_pnl_usd or 0) >= 0 else ''}${t.long_pnl_usd:.4f if t.long_pnl_usd else 0}\n"
                f"{pnl_emoji} <b>Чистая прибыль: {pnl_str}</b>"
            )
        except Exception as e:
            print(f"[GUI] notify mark-closed: {e}")
        return {"ok": True, "net_pnl": t.net_pnl_usd}

        # return {"ok": True, "msg": "Статус обновлен"}
    except Exception as e:
        print(f"[GUI] Ошибка в mark_closed: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/api/stats")
async def api_stats():
    base = {"leverage": LEVERAGE, "balance_alert_pct": BALANCE_ALERT_PCT,
            "max_open_positions": MAX_OPEN_POSITIONS, "min_volume_usd": MIN_VOLUME_USD}
    if not _db_ok:
        return {**base, "total": 0, "closed": 0, "open_cnt": 0, "failed": 0,
                "wins": 0, "losses": 0, "total_pnl": 0, "avg_pnl": 0, "win_rate": 0,
                "avg_exec_short_ms": None, "avg_exec_long_ms": None}
    from db import get_stats
    return {**get_stats(), **base}


# ── WebSocket ─────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    try:
        await _push(ws)
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in _ws_clients:
            _ws_clients.remove(ws)


async def _broadcast_loop():
    while True:
        await asyncio.sleep(15)
        for ws in list(_ws_clients):
            try:
                await _push(ws)
            except Exception:
                if ws in _ws_clients:
                    _ws_clients.remove(ws)


async def _broadcast_status():
    """Пушит обновлённый статус бота всем WS клиентам."""
    msg = json.dumps({"type": "bot_status", **_bot_state, "db_ok": _db_ok})
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            pass


async def _push(ws: WebSocket):
    balances = {}
    open_trades = []
    stats = {"total": 0, "closed": 0, "open_cnt": 0, "failed": 0,
             "wins": 0, "losses": 0, "total_pnl": 0.0, "avg_pnl": 0.0, "win_rate": 0,
             "avg_exec_short_ms": None, "avg_exec_long_ms": None}

    from risk_manager import get_disabled_exchanges
    disabled = get_disabled_exchanges()

    if _db_ok:
        from exchanges import get_all_balances
        from db import get_latest_balances, get_all_open_trades, get_stats
        try:
            live = await asyncio.wait_for(get_all_balances(EXCHANGES), timeout=10)
        except Exception:
            live = {}
        saved = get_latest_balances()
        balances = {ex: round(live.get(ex) or saved.get(ex) or 0, 4) for ex in EXCHANGES}
        open_trades = [_t(t) for t in get_all_open_trades()]
        stats = get_stats()

    await ws.send_text(json.dumps({
        "type":              "state",
        "balances":          balances,
        "open_trades":       open_trades,
        "stats":             stats,
        "bot_status":        {**_bot_state, "db_ok": _db_ok},
        "disabled_exchanges": disabled,
        "allocated":         _allocated,
        "ts":                datetime.now(timezone.utc).isoformat(),
    }))


# ── Serve HTML ────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    p = Path(__file__).parent / "dashboard.html"
    return HTMLResponse(p.read_text(encoding="utf-8") if p.exists() else "<h1>dashboard.html not found</h1>")


# ── Helper ────────────────────────────────────────────────────

def _t(t) -> dict:
    return {
        "id": t.id, "ticker": t.ticker, "symbol": t.symbol,
        "short_exchange": t.short_exchange, "long_exchange": t.long_exchange,
        "trade_size_usd": t.trade_size_usd,
        "short_entry_price": t.short_entry_price, "long_entry_price": t.long_entry_price,
        "short_close_price": t.short_close_price, "long_close_price": t.long_close_price,
        "short_pnl_usd": t.short_pnl_usd, "long_pnl_usd": t.long_pnl_usd,
        "net_pnl_usd": t.net_pnl_usd,
        "funding_short": t.funding_short, "funding_long": t.funding_long,
        "fee_short_usd": t.fee_short_usd, "fee_long_usd": t.fee_long_usd,
        "opened_at": str(t.opened_at) if t.opened_at else None,
        "closed_at": str(t.closed_at) if t.closed_at else None,
        "status": t.status, "leverage": t.leverage,
        "signal_spread_pct": t.signal_spread_pct,
        "exec_time_short_ms": t.exec_time_short_ms,
        "exec_time_long_ms":  t.exec_time_long_ms,
    }


# Добавьте новые эндпоинты:
@app.get("/api/ignored-symbols")
async def list_ignored():
    from db import get_ignored_symbols
    return get_ignored_symbols()


@app.post("/api/ignored-symbols/add")
async def add_ignored(ticker: str):
    from db import add_ignored_symbol, get_ignored_symbols
    from risk_manager import set_ignored_tickers
    add_ignored_symbol(ticker)
    set_ignored_tickers(get_ignored_symbols())
    return {"ok": True}


@app.post("/api/ignored-symbols/remove")
async def remove_ignored(ticker: str):
    from db import remove_ignored_symbol, get_ignored_symbols
    from risk_manager import set_ignored_tickers
    remove_ignored_symbol(ticker)
    set_ignored_tickers(get_ignored_symbols())
    return {"ok": True}