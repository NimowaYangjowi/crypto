"""Unified SQLite database for trades, forwarded messages, and settings."""

import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH: Path = None  # Set by init_db()

TRADE_COLUMNS = {
    "status", "filled_price", "qty", "exit_price", "result",
    "pnl_pct", "pnl_usdt", "tp1_hit", "sl_moved", "filled_at", "closed_at",
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
                enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT DEFAULT (datetime('now', 'localtime'))
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

def db_insert_trade(ticker, side, entry_price, qty, amount_usdt, tp1, tp2, tp3, sl):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """INSERT INTO trades (ticker, side, status, entry_price, qty, amount_usdt, tp1, tp2, tp3, sl)
               VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, side, entry_price, qty, amount_usdt, tp1, tp2, tp3, sl),
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


def db_get_trades(limit=100, status=None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if status and status != "all":
            rows = conn.execute(
                "SELECT * FROM trades WHERE status = ? ORDER BY id DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]


def db_get_stats():
    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        closed = conn.execute("SELECT COUNT(*) FROM trades WHERE status = 'closed'").fetchone()[0]
        wins = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE status = 'closed' AND pnl_usdt > 0"
        ).fetchone()[0]
        total_pnl = conn.execute(
            "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status = 'closed'"
        ).fetchone()[0]

        today = datetime.now().strftime("%Y-%m-%d")
        today_pnl = conn.execute(
            "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status = 'closed' AND closed_at LIKE ?",
            (f"{today}%",),
        ).fetchone()[0]
        today_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE created_at LIKE ?", (f"{today}%",)
        ).fetchone()[0]
        open_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE status IN ('pending', 'open')"
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


def db_get_today_pnl():
    today = datetime.now().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        result = conn.execute(
            "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status = 'closed' AND closed_at LIKE ?",
            (f"{today}%",),
        ).fetchone()[0]
        return result


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


def db_add_channel_format(channel_id, channel_name, template, default_side='LONG'):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO channel_formats (channel_id, channel_name, template, default_side) VALUES (?, ?, ?, ?)",
            (channel_id, channel_name, template, default_side),
        )
        return cur.lastrowid


def db_update_channel_format(fmt_id, **kwargs):
    allowed = {'channel_id', 'channel_name', 'template', 'default_side', 'enabled'}
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
