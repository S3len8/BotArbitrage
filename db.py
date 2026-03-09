"""
storage/db.py — PostgreSQL хранилище для арбитражного бота.

Таблицы:
- trades:            все сделки с полным набором данных
- balance_snapshots: история балансов для GUI и алертов

Строка подключения задаётся через DATABASE_URL в .env:
  DATABASE_URL=postgresql://user:password@localhost:5432/arb_bot
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Строка подключения — единственный источник конфига БД
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/arb_bot")


# ============================================================
# Модели
# ============================================================

@dataclass
class Trade:
    """Полная запись арбитражной сделки. Все финансовые поля в USD."""
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

    status:         str   = 'open'
    leverage:       int   = 5
    signal_spread_pct: float = 0.0
    signal_text:    str   = ""

    id: Optional[int] = None


@dataclass
class BalanceSnapshot:
    exchange:    str
    balance_usd: float
    timestamp:   str
    id: Optional[int] = None


# ============================================================
# Инициализация
# ============================================================

def db_init(dsn: str = DATABASE_URL):
    """Создаёт таблицы и индексы если не существуют."""
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
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
                    signal_text           TEXT           DEFAULT ''
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS balance_snapshots (
                    id           BIGSERIAL PRIMARY KEY,
                    exchange     VARCHAR(20)   NOT NULL,
                    balance_usd  NUMERIC(18,6) NOT NULL,
                    timestamp    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_ticker_status
                    ON trades(ticker, status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_balance_exchange_time
                    ON balance_snapshots(exchange, timestamp DESC)
            """)
        conn.commit()
    print(f"[DB] PostgreSQL ready: {_safe_dsn(dsn)}")


def _connect(dsn: str = DATABASE_URL):
    """
    Возвращает psycopg2 соединение.
    RealDictCursor — строки возвращаются как dict, как sqlite3.Row.
    """
    conn = psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def _safe_dsn(dsn: str) -> str:
    """Скрывает пароль для логов: postgresql://user:***@host/db"""
    import re
    return re.sub(r':([^@/]+)@', ':***@', dsn)


# ============================================================
# Trades
# ============================================================

def save_trade(trade: Trade, dsn: str = DATABASE_URL) -> int:
    """Сохраняет новую сделку. Возвращает id из PostgreSQL (RETURNING id)."""
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (
                    ticker, symbol, short_exchange, long_exchange, trade_size_usd,
                    short_order_id, long_order_id,
                    short_entry_price, long_entry_price, short_qty, long_qty,
                    status, leverage, opened_at,
                    funding_short, funding_long,
                    signal_spread_pct, signal_text
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                trade.ticker, trade.symbol,
                trade.short_exchange, trade.long_exchange, trade.trade_size_usd,
                trade.short_order_id, trade.long_order_id,
                trade.short_entry_price, trade.long_entry_price,
                trade.short_qty, trade.long_qty,
                trade.status, trade.leverage,
                trade.opened_at or _now(),
                trade.funding_short, trade.funding_long,
                trade.signal_spread_pct, trade.signal_text,
            ))
            trade.id = cur.fetchone()['id']
        conn.commit()
    return trade.id


def update_trade(trade: Trade, dsn: str = DATABASE_URL):
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trades SET
                    short_close_order_id = %s,  long_close_order_id = %s,
                    short_close_price    = %s,  long_close_price    = %s,
                    short_pnl_usd        = %s,  long_pnl_usd        = %s,
                    net_pnl_usd          = %s,
                    fee_short_usd        = %s,  fee_long_usd        = %s,
                    closed_at            = %s,  status              = %s,
                    short_order_id       = %s,  long_order_id       = %s,
                    short_entry_price    = %s,  long_entry_price    = %s,
                    short_qty            = %s,  long_qty            = %s
                WHERE id = %s
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
        conn.commit()


def get_open_trade(ticker: str, dsn: str = DATABASE_URL) -> Optional[Trade]:
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trades
                WHERE ticker = %s AND status IN ('open','partial')
                ORDER BY id DESC LIMIT 1
            """, (ticker.upper(),))
            row = cur.fetchone()
    return _row_to_trade(row) if row else None


def get_all_open_trades(dsn: str = DATABASE_URL) -> list[Trade]:
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM trades WHERE status IN ('open','partial') ORDER BY id DESC"
            )
            rows = cur.fetchall()
    return [_row_to_trade(r) for r in rows]


def get_trade_history(limit: int = 200, dsn: str = DATABASE_URL) -> list[Trade]:
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM trades ORDER BY id DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
    return [_row_to_trade(r) for r in rows]


def get_stats(dsn: str = DATABASE_URL) -> dict:
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                        AS total,
                    SUM(CASE WHEN status='closed' THEN 1 END)      AS closed,
                    SUM(CASE WHEN status='open'   THEN 1 END)      AS open_cnt,
                    SUM(CASE WHEN status='failed' THEN 1 END)      AS failed,
                    SUM(CASE WHEN net_pnl_usd > 0 THEN 1 END)      AS wins,
                    SUM(CASE WHEN net_pnl_usd < 0 THEN 1 END)      AS losses,
                    SUM(net_pnl_usd)                                AS total_pnl,
                    AVG(CASE WHEN status='closed' THEN net_pnl_usd END) AS avg_pnl
                FROM trades
            """)
            row = cur.fetchone()
    d = dict(row)
    # Конвертируем Decimal → float для JSON-сериализации
    for k in ('total_pnl', 'avg_pnl'):
        if d.get(k) is not None:
            d[k] = float(d[k])
    total_decided = (d['wins'] or 0) + (d['losses'] or 0)
    d['win_rate'] = round((d['wins'] or 0) / total_decided * 100, 1) if total_decided else 0
    return d


# ============================================================
# Balances
# ============================================================

def save_balance_snapshot(exchange: str, balance_usd: float, dsn: str = DATABASE_URL):
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO balance_snapshots (exchange, balance_usd) VALUES (%s, %s)",
                (exchange, balance_usd)
            )
        conn.commit()


def get_latest_balances(dsn: str = DATABASE_URL) -> dict[str, float]:
    """Последний сохранённый баланс по каждой бирже."""
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (exchange)
                    exchange, balance_usd
                FROM balance_snapshots
                ORDER BY exchange, timestamp DESC
            """)
            rows = cur.fetchall()
    return {r['exchange']: float(r['balance_usd']) for r in rows}


def get_balance_history(exchange: str, limit: int = 100, dsn: str = DATABASE_URL) -> list[dict]:
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT balance_usd, timestamp
                FROM balance_snapshots
                WHERE exchange = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """, (exchange, limit))
            rows = cur.fetchall()
    # Возвращаем от старых к новым для графика
    return [
        {'balance': float(r['balance_usd']), 'ts': r['timestamp'].isoformat()}
        for r in reversed(rows)
    ]


# ============================================================
# Helpers
# ============================================================

def _row_to_trade(row: dict) -> Trade:
    """
    psycopg2 RealDictCursor возвращает dict.
    Decimal → float, datetime → ISO string для dataclass.
    """
    from decimal import Decimal
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


def _now() -> datetime:
    return datetime.now(timezone.utc)
