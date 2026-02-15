"""Unified SQLite database for trades, forwarded messages, and settings."""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH: Path = None  # Set by init_db()

TRADE_COLUMNS = {
    "status", "filled_price", "qty", "exit_price", "result",
    "pnl_pct", "pnl_usdt", "tp1_hit", "sl_moved", "filled_at", "closed_at",
    "channel_name", "exchange_order_id", "source", "exchange_name",
    # OpenClaw integration columns
    "tp4", "sl_order_id", "tp_order_id", "market_type", "leverage",
    "remaining_qty", "sl_initial",
}


def init_db(data_dir: Path):
    global DB_PATH
    DB_PATH = data_dir / "tgforwarder.db"

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                entry_price REAL,
                filled_price REAL,
                qty REAL,
                amount_usdt REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                sl REAL,
                exit_price REAL,
                result TEXT,
                pnl_pct REAL,
                pnl_usdt REAL,
                tp1_hit INTEGER DEFAULT 0,
                sl_moved INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now', 'localtime')),
                filled_at TEXT,
                closed_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_formats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                channel_name TEXT DEFAULT '',
                template TEXT NOT NULL,
                default_side TEXT DEFAULT 'LONG',
                trade_amount REAL DEFAULT 0,
                exchange TEXT DEFAULT 'binance',
                enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT DEFAULT (datetime('now', 'localtime'))
            )
        """)
        # Migration: add trade_amount if missing
        try:
            conn.execute("ALTER TABLE channel_formats ADD COLUMN trade_amount REAL DEFAULT 0")
        except Exception:
            pass
        # Migration: add exchange column if missing
        try:
            conn.execute("ALTER TABLE channel_formats ADD COLUMN exchange TEXT DEFAULT 'binance'")
        except Exception:
            pass
        # Migration: add noise_filter to channel_formats
        try:
            conn.execute("ALTER TABLE channel_formats ADD COLUMN noise_filter TEXT DEFAULT ''")
        except Exception:
            pass
        # Migration: add channel_name to trades
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN channel_name TEXT DEFAULT ''")
        except Exception:
            pass
        # Migration: add exchange_order_id to trades (for dedup with exchange)
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN exchange_order_id TEXT DEFAULT ''")
        except Exception:
            pass
        # Migration: add source to trades ('bot' or 'exchange')
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN source TEXT DEFAULT 'bot'")
        except Exception:
            pass
        # Migration: add exchange_name to trades ('binance' or 'okx')
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN exchange_name TEXT DEFAULT ''")
        except Exception:
            pass
        # Migration: OpenClaw integration columns
        for col_sql in [
            "ALTER TABLE trades ADD COLUMN tp4 REAL",
            "ALTER TABLE trades ADD COLUMN sl_order_id TEXT DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN tp_order_id TEXT DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN market_type TEXT DEFAULT 'spot'",
            "ALTER TABLE trades ADD COLUMN leverage INTEGER DEFAULT 1",
            "ALTER TABLE trades ADD COLUMN remaining_qty REAL",
            "ALTER TABLE trades ADD COLUMN sl_initial REAL",
        ]:
            try:
                conn.execute(col_sql)
            except Exception:
                pass
        # Sync state for exchange trade sync
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS forwarded_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT,
                target_name TEXT,
                preview TEXT,
                status TEXT DEFAULT 'success',
                created_at TEXT DEFAULT (datetime('now', 'localtime'))
            )
        """)


# ── Trades ───────────────────────────────────────────────

def db_insert_trade(ticker, side, entry_price, qty, amount_usdt, tp1, tp2, tp3, sl, channel_name=''):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """INSERT INTO trades (ticker, side, status, entry_price, qty, amount_usdt, tp1, tp2, tp3, sl, channel_name)
               VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, side, entry_price, qty, amount_usdt, tp1, tp2, tp3, sl, channel_name),
        )
        return cur.lastrowid


def db_update_trade(trade_id, **kwargs):
    if not trade_id or not kwargs:
        return
    kwargs = {k: v for k, v in kwargs.items() if k in TRADE_COLUMNS}
    if not kwargs:
        return
    cols = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [trade_id]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"UPDATE trades SET {cols} WHERE id = ?", vals)


def db_get_trades(limit=100, status=None, channel=None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        where = []
        params = []
        if status and status != "all":
            where.append("status = ?")
            params.append(status)
        if channel and channel != "all":
            where.append("channel_name = ?")
            params.append(channel)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM trades {clause} ORDER BY id DESC LIMIT ?", params,
        ).fetchall()
        return [dict(r) for r in rows]


def db_get_stats(channel=None):
    with sqlite3.connect(DB_PATH) as conn:
        ch_filter = ""
        ch_params = ()
        if channel and channel != "all":
            ch_filter = " AND channel_name = ?"
            ch_params = (channel,)

        total = conn.execute(
            f"SELECT COUNT(*) FROM trades WHERE 1=1{ch_filter}", ch_params
        ).fetchone()[0]
        closed = conn.execute(
            f"SELECT COUNT(*) FROM trades WHERE status = 'closed'{ch_filter}", ch_params
        ).fetchone()[0]
        wins = conn.execute(
            f"SELECT COUNT(*) FROM trades WHERE status = 'closed' AND pnl_usdt > 0{ch_filter}", ch_params
        ).fetchone()[0]
        total_pnl = conn.execute(
            f"SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status = 'closed'{ch_filter}", ch_params
        ).fetchone()[0]

        today = datetime.now().strftime("%Y-%m-%d")
        today_pnl = conn.execute(
            f"SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status = 'closed' AND closed_at LIKE ?{ch_filter}",
            (f"{today}%",) + ch_params,
        ).fetchone()[0]
        today_count = conn.execute(
            f"SELECT COUNT(*) FROM trades WHERE created_at LIKE ?{ch_filter}",
            (f"{today}%",) + ch_params,
        ).fetchone()[0]
        open_count = conn.execute(
            f"SELECT COUNT(*) FROM trades WHERE status IN ('pending', 'open'){ch_filter}", ch_params
        ).fetchone()[0]

        return {
            "total_trades": total,
            "closed_trades": closed,
            "wins": wins,
            "win_rate": round(wins / closed * 100, 1) if closed > 0 else 0,
            "total_pnl": round(total_pnl, 2),
            "today_pnl": round(today_pnl, 2),
            "today_count": today_count,
            "open_count": open_count,
        }


def db_get_trade_channels():
    """Return distinct channel names from trades."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT DISTINCT channel_name FROM trades WHERE channel_name != '' ORDER BY channel_name"
        ).fetchall()
        return [r[0] for r in rows]


def db_get_today_pnl():
    today = datetime.now().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        result = conn.execute(
            "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status = 'closed' AND closed_at LIKE ?",
            (f"{today}%",),
        ).fetchone()[0]
        return result


def _period_cutoff(period):
    """Return ISO date string for the start of the given period, or None for lifetime."""
    now = datetime.now()
    if period == 'today':
        return now.strftime('%Y-%m-%d')
    elif period == '7d':
        return (now - timedelta(days=6)).strftime('%Y-%m-%d')
    elif period == '30d':
        return (now - timedelta(days=29)).strftime('%Y-%m-%d')
    return None


def _format_perf_row(r):
    """Format a raw aggregate row dict into a performance metrics dict."""
    ct = r['closed_trades']
    wins = r['wins']
    return {
        'closed_trades': ct,
        'wins': wins,
        'losses': r['losses'],
        'win_rate': round(wins / ct * 100, 1) if ct > 0 else 0,
        'total_pnl': round(r['total_pnl'], 2),
        'gross_profit': round(r['gross_profit'], 2),
        'gross_loss': round(r['gross_loss'], 2),
        'avg_pnl': round(r['avg_pnl'], 2),
        'avg_pnl_pct': round(r['avg_pnl_pct'], 2),
        'cumulative_return_pct': round(r['cumulative_return_pct'], 2),
        'avg_win': round(r['avg_win'], 2),
        'avg_loss': round(r['avg_loss'], 2),
        'tp1_hit_rate': round(r['tp1_hits'] / ct * 100, 1) if ct > 0 else 0,
    }


_PERF_SELECT = """
    COUNT(*) as closed_trades,
    COUNT(CASE WHEN pnl_usdt > 0 THEN 1 END) as wins,
    COUNT(CASE WHEN pnl_usdt < 0 THEN 1 END) as losses,
    COALESCE(SUM(pnl_usdt), 0) as total_pnl,
    COALESCE(SUM(CASE WHEN pnl_usdt > 0 THEN pnl_usdt ELSE 0 END), 0) as gross_profit,
    COALESCE(SUM(CASE WHEN pnl_usdt < 0 THEN pnl_usdt ELSE 0 END), 0) as gross_loss,
    COALESCE(AVG(pnl_usdt), 0) as avg_pnl,
    COALESCE(AVG(pnl_pct), 0) as avg_pnl_pct,
    COALESCE(SUM(pnl_pct), 0) as cumulative_return_pct,
    COALESCE(AVG(CASE WHEN pnl_usdt > 0 THEN pnl_usdt END), 0) as avg_win,
    COALESCE(AVG(CASE WHEN pnl_usdt < 0 THEN pnl_usdt END), 0) as avg_loss,
    COUNT(CASE WHEN tp1_hit = 1 THEN 1 END) as tp1_hits
"""


def db_get_performance_stats(period='lifetime', channel=None):
    """Return comprehensive performance metrics filtered by time period."""
    cutoff = _period_cutoff(period)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        where_parts = ["status = 'closed'"]
        params = []
        if cutoff:
            where_parts.append("closed_at >= ?")
            params.append(cutoff)
        if channel and channel != 'all':
            where_parts.append("channel_name = ?")
            params.append(channel)
        where = " AND ".join(where_parts)

        row = conn.execute(
            f"SELECT {_PERF_SELECT} FROM trades WHERE {where}", params
        ).fetchone()

        result = _format_perf_row(dict(row))
        result['period'] = period
        return result


def db_get_performance_table(period='lifetime'):
    """Return performance metrics per channel + total, for table display."""
    cutoff = _period_cutoff(period)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        time_filter = ""
        time_params = []
        if cutoff:
            time_filter = " AND closed_at >= ?"
            time_params = [cutoff]

        # Per-channel stats
        rows = conn.execute(f"""
            SELECT channel_name, {_PERF_SELECT}
            FROM trades
            WHERE status = 'closed'{time_filter}
            GROUP BY channel_name
            ORDER BY channel_name
        """, time_params).fetchall()

        channels = []
        for row in rows:
            r = dict(row)
            name = r.pop('channel_name', '') or '(unknown)'
            entry = _format_perf_row(r)
            entry['channel'] = name
            channels.append(entry)

        # Total stats
        total_row = conn.execute(f"""
            SELECT {_PERF_SELECT}
            FROM trades
            WHERE status = 'closed'{time_filter}
        """, time_params).fetchone()

        total = _format_perf_row(dict(total_row))
        total['channel'] = 'Total'

        return {
            'period': period,
            'rows': channels,
            'total': total,
        }


# ── Settings ─────────────────────────────────────────────

def db_load_settings():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {k: v for k, v in rows}


def db_save_settings(settings_dict):
    with sqlite3.connect(DB_PATH) as conn:
        for key, value in settings_dict.items():
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value)),
            )


# ── Forwarded Messages ───────────────────────────────────

def db_insert_forwarded_message(source_name, target_name, preview, status="success"):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO forwarded_messages (source_name, target_name, preview, status) VALUES (?, ?, ?, ?)",
            (source_name, target_name, preview, status),
        )


def db_get_forwarded_messages(limit=50):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM forwarded_messages ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Channel Formats ─────────────────────────────────────

def db_get_channel_formats():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM channel_formats ORDER BY id").fetchall()
        return [dict(r) for r in rows]


def db_add_channel_format(channel_id, channel_name, template, default_side='LONG', trade_amount=0, exchange='binance', noise_filter=''):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO channel_formats (channel_id, channel_name, template, default_side, trade_amount, exchange, noise_filter) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (channel_id, channel_name, template, default_side, trade_amount, exchange, noise_filter),
        )
        return cur.lastrowid


def db_update_channel_format(fmt_id, **kwargs):
    allowed = {'channel_id', 'channel_name', 'template', 'default_side', 'trade_amount', 'exchange', 'enabled', 'noise_filter'}
    kwargs = {k: v for k, v in kwargs.items() if k in allowed}
    if not kwargs:
        return
    kwargs['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cols = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [fmt_id]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"UPDATE channel_formats SET {cols} WHERE id = ?", vals)


def db_delete_channel_format(fmt_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM channel_formats WHERE id = ?", (fmt_id,))


def db_get_forwarded_count():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM forwarded_messages").fetchone()[0]


# ── Sync State ─────────────────────────────────────────

def db_get_sync_state(key):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None


def db_set_sync_state(key, value):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)", (key, str(value)))


# ── Exchange Trade Sync ────────────────────────────────

def db_delete_trade(trade_id, source_only="exchange"):
    """Delete a trade by ID. If source_only is set, only deletes if the trade has that source."""
    with sqlite3.connect(DB_PATH) as conn:
        if source_only:
            conn.execute("DELETE FROM trades WHERE id = ? AND source = ?", (trade_id, source_only))
        else:
            conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))


def db_get_known_exchange_order_ids(exchange_name):
    """Return set of exchange_order_ids already in DB for a given exchange."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT exchange_order_id FROM trades WHERE exchange_name = ? AND exchange_order_id != ''",
            (exchange_name,),
        ).fetchall()
        return {r[0] for r in rows}


# ── OpenClaw Trade Functions ──────────────────────────

def db_insert_openclaw_trade(ticker, side, entry_price, qty, amount_usdt,
                              tp1, tp2, tp3, tp4, sl, sl_initial,
                              market_type='spot', leverage=1,
                              exchange_name='binance', signal_text=None):
    """Insert a trade from openclaw_trader (source='openclaw')."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (ticker, side, status, entry_price, qty, amount_usdt,
                tp1, tp2, tp3, tp4, sl, sl_initial,
                market_type, leverage, remaining_qty,
                source, exchange_name, channel_name)
               VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'openclaw', ?, '')""",
            (ticker, side, entry_price, qty, amount_usdt,
             tp1, tp2, tp3, tp4, sl, sl_initial,
             market_type, leverage, qty,
             exchange_name),
        )
        return cur.lastrowid


def db_get_active_openclaw_trades():
    """Get all active/pending trades with source='openclaw'."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM trades WHERE source='openclaw' AND status IN ('pending', 'open') ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]


def db_get_active_trades_by_symbol(ticker, source=None):
    """Get active trades for a specific ticker, optionally filtered by source."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if source:
            rows = conn.execute(
                "SELECT * FROM trades WHERE ticker=? AND source=? AND status IN ('pending', 'open')",
                (ticker, source),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades WHERE ticker=? AND status IN ('pending', 'open')",
                (ticker,),
            ).fetchall()
        return [dict(r) for r in rows]


def db_get_trade(trade_id):
    """Get a single trade by ID."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        return dict(row) if row else None


def db_insert_synced_trade(ticker, side, status, filled_price, qty, amount_usdt,
                           exit_price, pnl_pct, pnl_usdt, exchange_order_id,
                           exchange_name, created_at, closed_at=None, result=None):
    """Insert a trade discovered from exchange sync (source='exchange')."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (ticker, side, status, entry_price, filled_price, qty, amount_usdt,
                exit_price, result, pnl_pct, pnl_usdt,
                created_at, closed_at,
                channel_name, exchange_order_id, source, exchange_name)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, 'exchange', ?)""",
            (ticker, side, status, filled_price, filled_price, qty, amount_usdt,
             exit_price, result, pnl_pct, pnl_usdt,
             created_at, closed_at,
             exchange_order_id, exchange_name),
        )
        return cur.lastrowid
