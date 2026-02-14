#!/usr/bin/env python3
"""
Trade database module. SQLite-based position, order, and PnL tracking.

All trade state lives here. watcher.py and trade.py both read/write through this module.
"""

import os
import sqlite3
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db")


def _utcnow():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _today_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class TradeDB:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                market_type TEXT NOT NULL DEFAULT 'spot',
                entry_price REAL NOT NULL,
                filled_price REAL,
                quantity REAL NOT NULL,
                remaining_qty REAL,
                amount_usdt REAL,
                sl_price REAL NOT NULL,
                sl_initial REAL NOT NULL,
                sl_order_id TEXT,
                sl_moved_to_breakeven INTEGER DEFAULT 0,
                tp1_price REAL,
                tp2_price REAL,
                tp3_price REAL,
                tp4_price REAL,
                tp_order_id TEXT,
                leverage INTEGER DEFAULT 1,
                status TEXT DEFAULT 'pending',
                close_reason TEXT,
                close_price REAL,
                realized_pnl REAL DEFAULT 0,
                signal_text TEXT,
                created_at TEXT,
                updated_at TEXT,
                closed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER,
                exchange_order_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price REAL,
                stop_price REAL,
                quantity REAL NOT NULL,
                filled_qty REAL DEFAULT 0,
                status TEXT DEFAULT 'pending',
                purpose TEXT,
                created_at TEXT,
                updated_at TEXT,
                FOREIGN KEY (position_id) REFERENCES positions(id)
            );

            CREATE TABLE IF NOT EXISTS daily_pnl (
                date TEXT PRIMARY KEY,
                realized_pnl REAL DEFAULT 0,
                trade_count INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                loss_limit_hit INTEGER DEFAULT 0,
                loss_limit_hit_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
            CREATE INDEX IF NOT EXISTS idx_orders_position ON orders(position_id);
        """)
        self.conn.commit()

    # --- Positions ---

    def create_position(self, **kwargs):
        now = _utcnow()
        kwargs.setdefault("created_at", now)
        kwargs.setdefault("updated_at", now)
        kwargs.setdefault("remaining_qty", kwargs.get("quantity"))
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" for _ in kwargs)
        cur = self.conn.execute(
            f"INSERT INTO positions ({cols}) VALUES ({placeholders})",
            list(kwargs.values()),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_position(self, position_id, updates):
        updates["updated_at"] = _utcnow()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [position_id]
        self.conn.execute(
            f"UPDATE positions SET {set_clause} WHERE id = ?", values
        )
        self.conn.commit()

    def close_position(self, position_id, reason, pnl, close_price):
        now = _utcnow()
        self.conn.execute(
            """UPDATE positions
               SET status='closed', close_reason=?, realized_pnl=?,
                   close_price=?, closed_at=?, updated_at=?
               WHERE id=?""",
            (reason, pnl, close_price, now, now, position_id),
        )
        self.conn.commit()

    def get_position(self, position_id):
        row = self.conn.execute(
            "SELECT * FROM positions WHERE id=?", (position_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_active_positions(self):
        rows = self.conn.execute(
            "SELECT * FROM positions WHERE status IN ('pending','active') ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_positions_by_symbol(self, symbol):
        rows = self.conn.execute(
            "SELECT * FROM positions WHERE symbol=? AND status IN ('pending','active')",
            (symbol,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_position_history(self, limit=20):
        rows = self.conn.execute(
            "SELECT * FROM positions ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Orders ---

    def create_order(self, **kwargs):
        now = _utcnow()
        kwargs.setdefault("created_at", now)
        kwargs.setdefault("updated_at", now)
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" for _ in kwargs)
        cur = self.conn.execute(
            f"INSERT INTO orders ({cols}) VALUES ({placeholders})",
            list(kwargs.values()),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_order(self, order_id, updates):
        updates["updated_at"] = _utcnow()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [order_id]
        self.conn.execute(
            f"UPDATE orders SET {set_clause} WHERE id=?", values
        )
        self.conn.commit()

    # --- Daily PnL ---

    def get_daily_pnl(self, date=None):
        date = date or _today_utc()
        row = self.conn.execute(
            "SELECT * FROM daily_pnl WHERE date=?", (date,)
        ).fetchone()
        return dict(row) if row else None

    def update_daily_pnl(self, pnl_change, is_win=None):
        date = _today_utc()
        existing = self.get_daily_pnl(date)
        if existing:
            new_pnl = existing["realized_pnl"] + pnl_change
            tc = existing["trade_count"] + 1
            w = existing["wins"] + (1 if is_win is True else 0)
            l = existing["losses"] + (1 if is_win is False else 0)
            self.conn.execute(
                "UPDATE daily_pnl SET realized_pnl=?, trade_count=?, wins=?, losses=? WHERE date=?",
                (new_pnl, tc, w, l, date),
            )
        else:
            w = 1 if is_win is True else 0
            l = 1 if is_win is False else 0
            self.conn.execute(
                "INSERT INTO daily_pnl (date, realized_pnl, trade_count, wins, losses) VALUES (?,?,1,?,?)",
                (date, pnl_change, w, l),
            )
        self.conn.commit()

    def is_loss_limit_hit(self):
        daily = self.get_daily_pnl()
        if not daily:
            return False
        if daily.get("loss_limit_hit", 0) == 1:
            return True
        return daily["realized_pnl"] <= -500

    def set_loss_limit_hit(self):
        date = _today_utc()
        now = _utcnow()
        existing = self.get_daily_pnl(date)
        if existing:
            self.conn.execute(
                "UPDATE daily_pnl SET loss_limit_hit=1, loss_limit_hit_at=? WHERE date=?",
                (now, date),
            )
        else:
            self.conn.execute(
                "INSERT INTO daily_pnl (date, realized_pnl, loss_limit_hit, loss_limit_hit_at) VALUES (?,0,1,?)",
                (date, now),
            )
        self.conn.commit()

    # --- Queries ---

    def get_total_exposure(self):
        row = self.conn.execute(
            "SELECT COALESCE(SUM(amount_usdt),0) as total FROM positions WHERE status IN ('pending','active')"
        ).fetchone()
        return row["total"] if row else 0

    def close(self):
        self.conn.close()
