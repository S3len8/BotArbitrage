"""
gui/server.py — FastAPI сервер для GUI дашборда.

Эндпоинты:
  GET  /api/balances           → текущие балансы по всем биржам
  GET  /api/balances/history   → история балансов для графика
  GET  /api/trades             → все сделки (история)
  GET  /api/trades/open        → открытые позиции
  GET  /api/stats              → сводная статистика
  GET  /api/status             → статус бота (running/stopped)
  POST /api/bot/stop           → остановить бота
  POST /api/bot/start          → запустить бота
  WS   /ws                     → live обновления (balances + trades)
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from storage.db import (
    db_init, get_latest_balances, get_balance_history,
    get_trade_history, get_all_open_trades, get_stats
)
from exchanges.base import get_all_balances
from config.settings import LEVERAGE, BALANCE_ALERT_PCT, MAX_OPEN_POSITIONS

app = FastAPI(title="Arb Bot Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connections
_ws_clients: list[WebSocket] = []

EXCHANGES = ['binance', 'bybit', 'bitget', 'mexc', 'kucoin', 'gate', 'ourbit']


# ============================================================
# REST API
# ============================================================

@app.on_event("startup")
async def startup():
    db_init()
    # Фоновая задача: пушим обновления по WS каждые 15 сек
    asyncio.create_task(_ws_broadcast_loop())


@app.get("/api/balances")
async def api_balances():
    """Live балансы с бирж + последние сохранённые."""
    try:
        live = await get_all_balances(EXCHANGES)
    except Exception:
        live = {}
    saved = get_latest_balances()

    result = {}
    for ex in EXCHANGES:
        live_bal = live.get(ex)
        saved_bal = saved.get(ex)
        result[ex] = {
            "live":    round(live_bal, 4)  if live_bal  is not None else None,
            "saved":   round(saved_bal, 4) if saved_bal is not None else None,
            "display": round(live_bal or saved_bal or 0, 4),
        }
    return result


@app.get("/api/balances/history")
async def api_balance_history(exchange: str, limit: int = 100):
    return get_balance_history(exchange, limit)


@app.get("/api/trades")
async def api_trades(limit: int = 200):
    trades = get_trade_history(limit)
    return [_trade_to_dict(t) for t in trades]


@app.get("/api/trades/open")
async def api_open_trades():
    trades = get_all_open_trades()
    return [_trade_to_dict(t) for t in trades]


@app.get("/api/stats")
async def api_stats():
    stats = get_stats()
    return {
        **stats,
        "leverage": LEVERAGE,
        "balance_alert_pct": BALANCE_ALERT_PCT,
        "max_open_positions": MAX_OPEN_POSITIONS,
    }


@app.get("/api/status")
async def api_status():
    return {
        "status": "running",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# WebSocket — live push
# ============================================================

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    try:
        # Сразу шлём текущее состояние
        await _push_state(ws)
        while True:
            await ws.receive_text()  # keep-alive
    except WebSocketDisconnect:
        pass
    finally:
        if ws in _ws_clients:
            _ws_clients.remove(ws)


async def _ws_broadcast_loop():
    while True:
        await asyncio.sleep(15)
        if _ws_clients:
            for ws in list(_ws_clients):
                try:
                    await _push_state(ws)
                except Exception:
                    if ws in _ws_clients:
                        _ws_clients.remove(ws)


async def _push_state(ws: WebSocket):
    try:
        live_balances = await asyncio.wait_for(get_all_balances(EXCHANGES), timeout=10)
    except Exception:
        live_balances = {}

    saved = get_latest_balances()
    balances = {}
    for ex in EXCHANGES:
        b = live_balances.get(ex) or saved.get(ex) or 0
        balances[ex] = round(b, 4)

    open_trades = [_trade_to_dict(t) for t in get_all_open_trades()]
    stats = get_stats()

    await ws.send_text(json.dumps({
        "type":        "state",
        "balances":    balances,
        "open_trades": open_trades,
        "stats":       stats,
        "ts":          datetime.now(timezone.utc).isoformat(),
    }))


# ============================================================
# HTML — отдаём GUI
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text())
    return HTMLResponse("<h1>Dashboard not found</h1>")


# ============================================================
# Helpers
# ============================================================

def _trade_to_dict(t) -> dict:
    return {
        "id":                  t.id,
        "ticker":              t.ticker,
        "symbol":              t.symbol,
        "short_exchange":      t.short_exchange,
        "long_exchange":       t.long_exchange,
        "trade_size_usd":      t.trade_size_usd,
        "short_entry_price":   t.short_entry_price,
        "long_entry_price":    t.long_entry_price,
        "short_close_price":   t.short_close_price,
        "long_close_price":    t.long_close_price,
        "short_pnl_usd":       t.short_pnl_usd,
        "long_pnl_usd":        t.long_pnl_usd,
        "net_pnl_usd":         t.net_pnl_usd,
        "funding_short":       t.funding_short,
        "funding_long":        t.funding_long,
        "fee_short_usd":       t.fee_short_usd,
        "fee_long_usd":        t.fee_long_usd,
        "opened_at":           t.opened_at,
        "closed_at":           t.closed_at,
        "status":              t.status,
        "leverage":            t.leverage,
        "signal_spread_pct":   t.signal_spread_pct,
    }
