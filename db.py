"""
db.py — PostgreSQL хранилище.
Таблицы: trades, balance_snapshots
"""

import psycopg2
import psycopg2.extras
from decimal import Decimal
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
from settings import DATABASE_URL


@dataclass
class Trade:
    ticker: str
    symbol: str
    short_exchange: str
    long_exchange: str
    trade_size_usd: float

    short_order_id:       Optional[str]   = None
    long_order_id:        Optional[str]   = None
    short_entry_price:    Optional[float] = None
    long_entry_price:     Optional[float] = None
    short_qty:            Optional[float] = None
    long_qty:             Optional[float] = None

    short_close_order_id: Optional[str]   = None
    long_close_order_id:  Optional[str]   = None
    short_close_price:    Optional[float] = None
    long_close_price:     Optional[float] = None

    short_pnl_usd:  Optional[float] = None
    long_pnl_usd:   Optional[float] = None
    net_pnl_usd:    Optional[float] = None

    funding_short:  Optional[float] = None
    funding_long:   Optional[float] = None

    fee_short_usd:  Optional[float] = None
    fee_long_usd:   Optional[float] = None

    opened_at:      Optional[str]   = None
    closed_at:      Optional[str]   = None

    status:            str   = 'open'
    leverage:          int   = 5
    signal_spread_pct: float = 0.0
    signal_text:       str   = ""

    # Время исполнения: секунды от получения сигнала до исполнения ноги
    exec_time_short_ms: Optional[int] = None   # мс от сигнала до short fill
    exec_time_long_ms:  Optional[int] = None   # мс от сигнала до long fill

    id: Optional[int] = None


def db_init():
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id                    BIGSERIAL PRIMARY KEY,
                ticker                VARCHAR(20)    NOT NULL,
                symbol                VARCHAR(20)    NOT NULL,
                short_exchange        VARCHAR(20)    NOT NULL,
                long_exchange         VARCHAR(20)    NOT NULL,
                trade_size_usd        NUMERIC(18,6)  NOT NULL,
                short_order_id        TEXT,
                long_order_id         TEXT,
                short_entry_price     NUMERIC(24,10),
                long_entry_price      NUMERIC(24,10),
                short_qty             NUMERIC(24,10),
                long_qty              NUMERIC(24,10),
                short_close_order_id  TEXT,
                long_close_order_id   TEXT,
                short_close_price     NUMERIC(24,10),
                long_close_price      NUMERIC(24,10),
                short_pnl_usd         NUMERIC(18,6),
                long_pnl_usd          NUMERIC(18,6),
                net_pnl_usd           NUMERIC(18,6),
                funding_short         NUMERIC(12,8),
                funding_long          NUMERIC(12,8),
                fee_short_usd         NUMERIC(18,6),
                fee_long_usd          NUMERIC(18,6),
                opened_at             TIMESTAMPTZ,
                closed_at             TIMESTAMPTZ,
                status                VARCHAR(10)    NOT NULL DEFAULT 'open',
                leverage              SMALLINT       DEFAULT 5,
                signal_spread_pct     NUMERIC(10,4)  DEFAULT 0,
                signal_text           TEXT           DEFAULT '',
                exec_time_short_ms    INTEGER,
                exec_time_long_ms     INTEGER
            )
        """)
        # Добавляем колонки если таблица уже существует (миграция)
        for col, definition in [
            ('exec_time_short_ms', 'INTEGER'),
            ('exec_time_long_ms',  'INTEGER'),
        ]:
            try:
                cur.execute(f"ALTER TABLE trades ADD COLUMN IF NOT EXISTS {col} {definition}")
            except Exception:
                pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS balance_snapshots (
                id           BIGSERIAL PRIMARY KEY,
                exchange     VARCHAR(20)   NOT NULL,
                balance_usd  NUMERIC(18,6) NOT NULL,
                timestamp    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_ticker_status ON trades(ticker, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_balance_exchange_time ON balance_snapshots(exchange, timestamp DESC)")

        # Таблица настроек бирж: выключение и выделенный капитал
        cur.execute("""
            CREATE TABLE IF NOT EXISTS exchange_settings (
                exchange        VARCHAR(20)   PRIMARY KEY,
                enabled         BOOLEAN       NOT NULL DEFAULT TRUE,
                allocated_usd   NUMERIC(18,6) NOT NULL DEFAULT 0,
                updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            )
        """)
        c.commit()
    print(f"[DB] PostgreSQL ready")


def _conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def save_trade(trade: Trade) -> int:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO trades (
                ticker, symbol, short_exchange, long_exchange, trade_size_usd,
                short_order_id, long_order_id,
                short_entry_price, long_entry_price, short_qty, long_qty,
                status, leverage, opened_at,
                funding_short, funding_long, signal_spread_pct, signal_text,
                exec_time_short_ms, exec_time_long_ms
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            trade.ticker, trade.symbol,
            trade.short_exchange, trade.long_exchange, trade.trade_size_usd,
            trade.short_order_id, trade.long_order_id,
            trade.short_entry_price, trade.long_entry_price,
            trade.short_qty, trade.long_qty,
            trade.status, trade.leverage,
            trade.opened_at or datetime.now(timezone.utc),
            trade.funding_short, trade.funding_long,
            trade.signal_spread_pct, trade.signal_text,
            trade.exec_time_short_ms, trade.exec_time_long_ms,
        ))
        trade.id = cur.fetchone()['id']
        c.commit()
    return trade.id


def update_trade(trade: Trade):
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            UPDATE trades SET
                short_close_order_id=%s, long_close_order_id=%s,
                short_close_price=%s,    long_close_price=%s,
                short_pnl_usd=%s,        long_pnl_usd=%s,
                net_pnl_usd=%s,
                fee_short_usd=%s,        fee_long_usd=%s,
                closed_at=%s,            status=%s,
                short_order_id=%s,       long_order_id=%s,
                short_entry_price=%s,    long_entry_price=%s,
                short_qty=%s,            long_qty=%s
            WHERE id=%s
        """, (
            trade.short_close_order_id, trade.long_close_order_id,
            trade.short_close_price,    trade.long_close_price,
            trade.short_pnl_usd,        trade.long_pnl_usd,
            trade.net_pnl_usd,
            trade.fee_short_usd,        trade.fee_long_usd,
            trade.closed_at,            trade.status,
            trade.short_order_id,       trade.long_order_id,
            trade.short_entry_price,    trade.long_entry_price,
            trade.short_qty,            trade.long_qty,
            trade.id,
        ))
        c.commit()


def get_open_trade(ticker: str) -> Optional[Trade]:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT * FROM trades
            WHERE ticker=%s AND status IN ('open','partial')
            ORDER BY id DESC LIMIT 1
        """, (ticker.upper(),))
        row = cur.fetchone()
    return _to_trade(row) if row else None


def get_all_open_trades() -> list[Trade]:
    with _conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM trades WHERE status IN ('open','partial') ORDER BY id DESC")
        rows = cur.fetchall()
    return [_to_trade(r) for r in rows]


def get_trade_history(limit: int = 200) -> list[Trade]:
    with _conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM trades ORDER BY id DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [_to_trade(r) for r in rows]


def get_stats() -> dict:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*)                                        AS total,
                SUM(CASE WHEN status='closed' THEN 1 END)      AS closed,
                SUM(CASE WHEN status='open'   THEN 1 END)      AS open_cnt,
                SUM(CASE WHEN status='failed' THEN 1 END)      AS failed,
                SUM(CASE WHEN net_pnl_usd > 0 THEN 1 END)      AS wins,
                SUM(CASE WHEN net_pnl_usd < 0 THEN 1 END)      AS losses,
                SUM(net_pnl_usd)                                AS total_pnl,
                AVG(CASE WHEN status='closed' THEN net_pnl_usd END) AS avg_pnl,
                AVG(CASE WHEN exec_time_short_ms IS NOT NULL THEN exec_time_short_ms END) AS avg_exec_short_ms,
                AVG(CASE WHEN exec_time_long_ms  IS NOT NULL THEN exec_time_long_ms  END) AS avg_exec_long_ms
            FROM trades
        """)
        row = cur.fetchone()
    d = dict(row)
    for k in ('total_pnl', 'avg_pnl', 'avg_exec_short_ms', 'avg_exec_long_ms'):
        if d.get(k) is not None:
            d[k] = float(d[k])
    total_decided = (d['wins'] or 0) + (d['losses'] or 0)
    d['win_rate'] = round((d['wins'] or 0) / total_decided * 100, 1) if total_decided else 0
    return d


def save_balance_snapshot(exchange: str, balance_usd: float):
    with _conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO balance_snapshots (exchange, balance_usd) VALUES (%s, %s)",
            (exchange, balance_usd)
        )
        c.commit()


def get_latest_balances() -> dict[str, float]:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (exchange) exchange, balance_usd
            FROM balance_snapshots
            ORDER BY exchange, timestamp DESC
        """)
        rows = cur.fetchall()
    return {r['exchange']: float(r['balance_usd']) for r in rows}


def get_balance_history(exchange: str, limit: int = 100) -> list[dict]:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT balance_usd, timestamp FROM balance_snapshots
            WHERE exchange=%s ORDER BY timestamp DESC LIMIT %s
        """, (exchange, limit))
        rows = cur.fetchall()
    return [{'balance': float(r['balance_usd']), 'ts': r['timestamp'].isoformat()} for r in reversed(rows)]


def _to_trade(row: dict) -> Trade:
    d = {}
    for k, v in row.items():
        if k not in Trade.__dataclass_fields__:
            continue
        if isinstance(v, Decimal):
            v = float(v)
        elif isinstance(v, datetime):
            v = v.isoformat()
        d[k] = v
    return Trade(**d)


# ── exchange_settings ─────────────────────────────────────────

def save_exchange_setting(exchange: str, enabled: bool, allocated_usd: float):
    """Upsert настроек биржи (enabled + allocated_usd)."""
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO exchange_settings (exchange, enabled, allocated_usd, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (exchange) DO UPDATE SET
                enabled       = EXCLUDED.enabled,
                allocated_usd = EXCLUDED.allocated_usd,
                updated_at    = NOW()
        """, (exchange.lower(), enabled, allocated_usd))
        c.commit()


def load_exchange_settings() -> dict[str, dict]:
    """Загружает все настройки бирж. Возвращает {exchange: {enabled, allocated_usd}}."""
    try:
        with _conn() as c, c.cursor() as cur:
            cur.execute("SELECT exchange, enabled, allocated_usd FROM exchange_settings")
            rows = cur.fetchall()
        return {
            r['exchange']: {
                'enabled':       bool(r['enabled']),
                'allocated_usd': float(r['allocated_usd']),
            }
            for r in rows
        }
    except Exception as e:
        print(f"[DB] load_exchange_settings error: {e}")
        return {}