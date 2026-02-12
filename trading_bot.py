#!/usr/bin/env python3
"""
Standalone Binance Trading Bot
- Monitors Telegram channels via Telethon (user session)
- Parses trading signals with regex
- Executes trades on Binance (Spot LONG / Futures SHORT)
- Reports results via Telegram Bot API
- Monitors positions with 3-stage trailing stop:
  TP1 → SL to breakeven, TP2 → SL to TP1, TP3 → SL to TP2 (target TP4)
"""

import asyncio
import logging
import os
import re
import sys
import time
import json
import sqlite3
import httpx
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient, events
import ccxt
from aiohttp import web

# ── Setup ─────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trading_bot.log"),
    ],
)
logger = logging.getLogger("trading_bot")

# ── Config ────────────────────────────────────────────────

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
SOURCE_CHANNELS = [c.strip() for c in os.getenv("SOURCE_CHANNELS", "").split(",") if c.strip()]
MY_CHAT_ID = int(os.getenv("MY_CHAT_ID", "0"))
TRADE_AMOUNT = float(os.getenv("TRADE_AMOUNT", "100"))
SELL_BLOCKED = {s.strip().upper() for s in os.getenv("SELL_BLOCKED", "").split(",") if s.strip()}
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "500"))
ENTRY_TIMEOUT = int(os.getenv("ENTRY_TIMEOUT", "600"))  # seconds
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8081"))

# ── Signal Parser ─────────────────────────────────────────

SIGNAL_PATTERN = re.compile(
    r"#(\w+)\s*[–—-]\s*(LONG|SHORT)\s*"
    r".*?진입\s*포인트[:\s]*([\d.]+)\s*"
    r".*?목표\s*수익[:\s]*([\d.,\s]+)\s*"
    r".*?손절가[:\s]*([\d.]+)",
    re.DOTALL | re.IGNORECASE,
)


def parse_signal(text):
    """Parse a trading signal message. Returns dict or None."""
    match = SIGNAL_PATTERN.search(text)
    if not match:
        return None

    ticker = match.group(1).upper()
    side = match.group(2).upper()
    entry = float(match.group(3))
    targets = [float(t.strip()) for t in match.group(4).split(",") if t.strip()]
    sl = float(match.group(5))

    if len(targets) < 3:
        return None

    return {
        "ticker": ticker,
        "side": side,
        "entry": entry,
        "tp1": targets[0],
        "tp2": targets[1],
        "tp3": targets[2],
        "tp4": targets[3] if len(targets) > 3 else targets[2],
        "sl": sl,
    }


# ── Database ──────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "trades.db"

TRADE_COLUMNS = {
    "status", "filled_price", "qty", "exit_price", "result",
    "pnl_pct", "pnl_usdt", "tp1_hit", "tp2_hit", "tp3_hit", "sl_moved",
    "filled_at", "closed_at",
}


def init_db():
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
                tp4 REAL,
                sl REAL,
                exit_price REAL,
                result TEXT,
                pnl_pct REAL,
                pnl_usdt REAL,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                tp3_hit INTEGER DEFAULT 0,
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
        # Migrate: add new columns if missing
        existing = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        for col, typ, default in [
            ("tp4", "REAL", None),
            ("tp2_hit", "INTEGER", "0"),
            ("tp3_hit", "INTEGER", "0"),
        ]:
            if col not in existing:
                dflt = f" DEFAULT {default}" if default is not None else ""
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}{dflt}")


def db_insert_trade(ticker, side, entry_price, qty, amount_usdt, tp1, tp2, tp3, tp4, sl):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """INSERT INTO trades (ticker, side, status, entry_price, qty, amount_usdt, tp1, tp2, tp3, tp4, sl)
               VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, side, entry_price, qty, amount_usdt, tp1, tp2, tp3, tp4, sl),
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


def apply_settings_from_db():
    """Load settings from DB and override in-memory globals."""
    global TRADE_AMOUNT, SELL_BLOCKED, MAX_CONCURRENT, DAILY_LOSS_LIMIT, ENTRY_TIMEOUT
    saved = db_load_settings()
    if not saved:
        return
    if "TRADE_AMOUNT" in saved:
        TRADE_AMOUNT = float(saved["TRADE_AMOUNT"])
    if "SELL_BLOCKED" in saved:
        SELL_BLOCKED = {s.strip().upper() for s in saved["SELL_BLOCKED"].split(",") if s.strip()}
    if "MAX_CONCURRENT" in saved:
        MAX_CONCURRENT = int(saved["MAX_CONCURRENT"])
    if "DAILY_LOSS_LIMIT" in saved:
        DAILY_LOSS_LIMIT = float(saved["DAILY_LOSS_LIMIT"])
    if "ENTRY_TIMEOUT" in saved:
        ENTRY_TIMEOUT = int(saved["ENTRY_TIMEOUT"])
    logger.info(f"Settings loaded: TRADE_AMOUNT={TRADE_AMOUNT}, SELL_BLOCKED={SELL_BLOCKED}, "
                f"MAX_CONCURRENT={MAX_CONCURRENT}, DAILY_LOSS_LIMIT={DAILY_LOSS_LIMIT}, "
                f"ENTRY_TIMEOUT={ENTRY_TIMEOUT}")


# ── Binance Exchange ──────────────────────────────────────

def create_exchange(futures=False):
    config = {
        "apiKey": BINANCE_API_KEY,
        "secret": BINANCE_SECRET_KEY,
        "enableRateLimit": True,
    }
    if futures:
        config["options"] = {"defaultType": "future"}
    exchange = ccxt.binance(config)
    exchange.load_markets()
    return exchange


# ── Trade Execution ───────────────────────────────────────

async def execute_spot_long(signal, bot_client):
    """Execute LONG on Binance Spot. Runs in background thread."""
    ticker = signal["ticker"]
    symbol = f"{ticker}/USDT"
    entry = signal["entry"]
    tp1, tp2, tp3, tp4 = signal["tp1"], signal["tp2"], signal["tp3"], signal["tp4"]
    sl = signal["sl"]
    trade_id = None

    try:
        exchange = create_exchange(futures=False)
        market = exchange.market(symbol)
        qty = round(TRADE_AMOUNT / entry, market["precision"]["amount"])

        trade_id = db_insert_trade(
            ticker, "LONG", entry, qty, TRADE_AMOUNT,
            tp1, tp2, tp3, tp4, sl,
        )

        # Place limit buy at entry
        order = exchange.create_limit_buy_order(symbol, qty, entry)
        order_id = order["id"]
        logger.info(f"[SPOT LONG] {symbol} entry order: {order_id} qty={qty} @ {entry}")

        await notify(bot_client, (
            f"✅ {ticker} LONG 주문 접수\n"
            f"진입: {entry} | SL: {sl}\n"
            f"TP1: {tp1} | TP2: {tp2} | TP3: {tp3} | TP4: {tp4}\n"
            f"수량: {qty} | 투입: ~{TRADE_AMOUNT} USDT"
        ))

        # Wait for entry fill (timeout: ENTRY_TIMEOUT)
        wait_start = time.time()
        while True:
            if time.time() - wait_start > ENTRY_TIMEOUT:
                try:
                    exchange.cancel_order(order_id, symbol)
                except Exception:
                    pass
                logger.info(f"[SPOT LONG] {symbol} entry TIMEOUT ({ENTRY_TIMEOUT}s)")
                db_update_trade(trade_id, status="timeout", result="timeout",
                                closed_at=datetime.now().isoformat())
                await notify(bot_client, f"⏰ {ticker} LONG 진입 미체결 ({ENTRY_TIMEOUT // 60}분). 주문 취소.")
                return
            o = exchange.fetch_order(order_id, symbol)
            if o["status"] == "closed":
                filled_qty = o["filled"]
                avg_price = o["average"] or entry
                logger.info(f"[SPOT LONG] {symbol} FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat())
                await notify(bot_client, f"📥 {ticker} 진입 체결: {filled_qty} @ {avg_price}")
                break
            if o["status"] == "canceled":
                logger.info(f"[SPOT LONG] {symbol} entry CANCELED")
                db_update_trade(trade_id, status="cancelled", result="cancelled",
                                closed_at=datetime.now().isoformat())
                await notify(bot_client, f"❌ {ticker} 진입 주문 취소됨")
                return
            await asyncio.sleep(5)

        # Place SL and TP orders (TP at tp4 = final target)
        sl_order = exchange.create_order(symbol, "stop_loss_limit", "sell", filled_qty, sl, {"stopPrice": sl})
        sl_order_id = sl_order["id"]
        tp_order = exchange.create_limit_sell_order(symbol, filled_qty, tp4)
        tp_order_id = tp_order["id"]
        logger.info(f"[SPOT LONG] {symbol} SL: {sl_order_id} @ {sl}, TP4: {tp_order_id} @ {tp4}")

        # Monitor: 3-stage trailing stop
        trail_stage = 0  # 0=initial, 1=TP1 hit, 2=TP2 hit, 3=TP3 hit
        stage_labels = {0: "sl_hit", 1: "sl_after_tp1", 2: "sl_after_tp2", 3: "sl_after_tp3"}

        while True:
            try:
                ticker_data = exchange.fetch_ticker(symbol)
                price = ticker_data["last"]

                # Stage 1: TP1 → SL to breakeven (entry price)
                if trail_stage == 0 and price >= tp1:
                    logger.info(f"[SPOT LONG] {symbol} TP1 reached ({price}). SL → {avg_price}")
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                        sl_order = exchange.create_order(
                            symbol, "stop_loss_limit", "sell", filled_qty, avg_price,
                            {"stopPrice": avg_price}
                        )
                        sl_order_id = sl_order["id"]
                        trail_stage = 1
                        db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                        await notify(bot_client, f"🔄 {ticker} TP1 도달! SL → 진입점({avg_price})")
                    except Exception as e:
                        logger.error(f"Failed to move SL: {e}")

                # Stage 2: TP2 → SL to TP1
                elif trail_stage == 1 and price >= tp2:
                    logger.info(f"[SPOT LONG] {symbol} TP2 reached ({price}). SL → TP1({tp1})")
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                        sl_order = exchange.create_order(
                            symbol, "stop_loss_limit", "sell", filled_qty, tp1,
                            {"stopPrice": tp1}
                        )
                        sl_order_id = sl_order["id"]
                        trail_stage = 2
                        db_update_trade(trade_id, tp2_hit=1)
                        await notify(bot_client, f"🔄 {ticker} TP2 도달! SL → TP1({tp1})")
                    except Exception as e:
                        logger.error(f"Failed to move SL: {e}")

                # Stage 3: TP3 → SL to TP2, ride to TP4
                elif trail_stage == 2 and price >= tp3:
                    logger.info(f"[SPOT LONG] {symbol} TP3 reached ({price}). SL → TP2({tp2})")
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                        sl_order = exchange.create_order(
                            symbol, "stop_loss_limit", "sell", filled_qty, tp2,
                            {"stopPrice": tp2}
                        )
                        sl_order_id = sl_order["id"]
                        trail_stage = 3
                        db_update_trade(trade_id, tp3_hit=1)
                        await notify(bot_client, f"🔄 {ticker} TP3 도달! SL → TP2({tp2}) | TP4({tp4}) 노림")
                    except Exception as e:
                        logger.error(f"Failed to move SL: {e}")

                # Check TP4 (final target)
                tp_status = exchange.fetch_order(tp_order_id, symbol)
                if tp_status["status"] == "closed":
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                    except Exception:
                        pass
                    pnl = round((tp4 - avg_price) / avg_price * 100, 2)
                    pnl_usdt = round((tp4 - avg_price) * filled_qty, 2)
                    record_pnl((tp4 - avg_price) * filled_qty)
                    db_update_trade(trade_id, status="closed", result="tp4_hit",
                                    exit_price=tp4, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                    closed_at=datetime.now().isoformat())
                    logger.info(f"[SPOT LONG] {symbol} TP4 HIT! PnL: {pnl}%")
                    await notify(bot_client, f"🎯 {ticker} LONG TP4 도달!\n수익률: {pnl}% | {pnl_usdt} USDT")
                    return

                # Check SL
                sl_status = exchange.fetch_order(sl_order_id, symbol)
                if sl_status["status"] == "closed":
                    try:
                        exchange.cancel_order(tp_order_id, symbol)
                    except Exception:
                        pass
                    sl_fill = sl_status["average"] or sl
                    pnl = round((sl_fill - avg_price) / avg_price * 100, 2)
                    pnl_usdt = round((sl_fill - avg_price) * filled_qty, 2)
                    record_pnl((sl_fill - avg_price) * filled_qty)
                    result = stage_labels[trail_stage]
                    db_update_trade(trade_id, status="closed", result=result,
                                    exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                    closed_at=datetime.now().isoformat())
                    logger.info(f"[SPOT LONG] {symbol} SL HIT @ {sl_fill} (stage {trail_stage}). PnL: {pnl}%")
                    await notify(bot_client, f"📊 {ticker} LONG SL 도달 @ {sl_fill}\n단계: {trail_stage} | 수익률: {pnl}% | {pnl_usdt} USDT")
                    return

                # Check spot balance (position closed externally?)
                balance = exchange.fetch_balance()
                token_total = float(balance.get(ticker, {}).get("total", 0))
                if token_total < filled_qty * 0.95:
                    for oid in [sl_order_id, tp_order_id]:
                        try:
                            exchange.cancel_order(oid, symbol)
                        except Exception:
                            pass
                    db_update_trade(trade_id, status="closed", result="external",
                                    closed_at=datetime.now().isoformat())
                    logger.info(f"[SPOT LONG] {symbol} position closed externally")
                    await notify(bot_client, f"📊 {ticker} LONG 포지션 외부에서 종료됨")
                    return

            except ccxt.NetworkError as e:
                logger.warning(f"Network error: {e}")

            await asyncio.sleep(10)

    except Exception as e:
        if trade_id:
            db_update_trade(trade_id, status="error", result=str(e)[:200],
                            closed_at=datetime.now().isoformat())
        logger.error(f"[SPOT LONG] {symbol} error: {e}")
        await notify(bot_client, f"⚠️ {ticker} LONG 에러: {e}")


async def execute_futures_short(signal, bot_client):
    """Execute SHORT on Binance Futures (1x). Runs in background thread."""
    ticker = signal["ticker"]
    symbol = f"{ticker}/USDT"
    entry = signal["entry"]
    tp1, tp2, tp3, tp4 = signal["tp1"], signal["tp2"], signal["tp3"], signal["tp4"]
    sl = signal["sl"]
    trade_id = None

    try:
        exchange = create_exchange(futures=True)
        market = exchange.market(symbol)
        qty = round(TRADE_AMOUNT / entry, market["precision"]["amount"])

        trade_id = db_insert_trade(
            ticker, "SHORT", entry, qty, TRADE_AMOUNT,
            tp1, tp2, tp3, tp4, sl,
        )

        # Set 1x leverage + isolated margin
        try:
            exchange.set_leverage(1, symbol)
            exchange.set_margin_mode("isolated", symbol)
        except Exception:
            pass

        # Place limit sell (short) at entry
        order = exchange.create_limit_sell_order(symbol, qty, entry)
        order_id = order["id"]
        logger.info(f"[FUTURES SHORT] {symbol} entry order: {order_id} qty={qty} @ {entry}")

        await notify(bot_client, (
            f"✅ {ticker} SHORT 주문 접수\n"
            f"진입: {entry} | SL: {sl}\n"
            f"TP1: {tp1} | TP2: {tp2} | TP3: {tp3} | TP4: {tp4}\n"
            f"수량: {qty} | 투입: ~{TRADE_AMOUNT} USDT | 1x"
        ))

        # Wait for entry fill (timeout: ENTRY_TIMEOUT)
        wait_start = time.time()
        while True:
            if time.time() - wait_start > ENTRY_TIMEOUT:
                try:
                    exchange.cancel_order(order_id, symbol)
                except Exception:
                    pass
                logger.info(f"[FUTURES SHORT] {symbol} entry TIMEOUT ({ENTRY_TIMEOUT}s)")
                db_update_trade(trade_id, status="timeout", result="timeout",
                                closed_at=datetime.now().isoformat())
                await notify(bot_client, f"⏰ {ticker} SHORT 진입 미체결 ({ENTRY_TIMEOUT // 60}분). 주문 취소.")
                return
            o = exchange.fetch_order(order_id, symbol)
            if o["status"] == "closed":
                filled_qty = o["filled"]
                avg_price = o["average"] or entry
                logger.info(f"[FUTURES SHORT] {symbol} FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat())
                await notify(bot_client, f"📥 {ticker} 숏 진입 체결: {filled_qty} @ {avg_price}")
                break
            if o["status"] == "canceled":
                logger.info(f"[FUTURES SHORT] {symbol} entry CANCELED")
                db_update_trade(trade_id, status="cancelled", result="cancelled",
                                closed_at=datetime.now().isoformat())
                await notify(bot_client, f"❌ {ticker} 진입 주문 취소됨")
                return
            await asyncio.sleep(5)

        # Place SL (STOP_MARKET) and TP (TAKE_PROFIT_MARKET at tp4 = final target)
        sl_order = exchange.create_order(
            symbol, "stop_market", "buy", filled_qty, None,
            {"stopPrice": sl, "reduceOnly": True}
        )
        sl_order_id = sl_order["id"]
        tp_order = exchange.create_order(
            symbol, "take_profit_market", "buy", filled_qty, None,
            {"stopPrice": tp4, "reduceOnly": True}
        )
        tp_order_id = tp_order["id"]
        logger.info(f"[FUTURES SHORT] {symbol} SL: {sl_order_id} @ {sl}, TP4: {tp_order_id} @ {tp4}")

        # Monitor: 3-stage trailing stop
        trail_stage = 0  # 0=initial, 1=TP1 hit, 2=TP2 hit, 3=TP3 hit
        stage_labels = {0: "sl_hit", 1: "sl_after_tp1", 2: "sl_after_tp2", 3: "sl_after_tp3"}

        while True:
            try:
                ticker_data = exchange.fetch_ticker(symbol)
                price = ticker_data["last"]

                # Stage 1: TP1 → SL to breakeven (SHORT: price drops to tp1)
                if trail_stage == 0 and price <= tp1:
                    logger.info(f"[FUTURES SHORT] {symbol} TP1 reached ({price}). SL → {avg_price}")
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                        sl_order = exchange.create_order(
                            symbol, "stop_market", "buy", filled_qty, None,
                            {"stopPrice": avg_price, "reduceOnly": True}
                        )
                        sl_order_id = sl_order["id"]
                        trail_stage = 1
                        db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                        await notify(bot_client, f"🔄 {ticker} TP1 도달! SL → 진입점({avg_price})")
                    except Exception as e:
                        logger.error(f"Failed to move SL: {e}")

                # Stage 2: TP2 → SL to TP1
                elif trail_stage == 1 and price <= tp2:
                    logger.info(f"[FUTURES SHORT] {symbol} TP2 reached ({price}). SL → TP1({tp1})")
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                        sl_order = exchange.create_order(
                            symbol, "stop_market", "buy", filled_qty, None,
                            {"stopPrice": tp1, "reduceOnly": True}
                        )
                        sl_order_id = sl_order["id"]
                        trail_stage = 2
                        db_update_trade(trade_id, tp2_hit=1)
                        await notify(bot_client, f"🔄 {ticker} TP2 도달! SL → TP1({tp1})")
                    except Exception as e:
                        logger.error(f"Failed to move SL: {e}")

                # Stage 3: TP3 → SL to TP2, ride to TP4
                elif trail_stage == 2 and price <= tp3:
                    logger.info(f"[FUTURES SHORT] {symbol} TP3 reached ({price}). SL → TP2({tp2})")
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                        sl_order = exchange.create_order(
                            symbol, "stop_market", "buy", filled_qty, None,
                            {"stopPrice": tp2, "reduceOnly": True}
                        )
                        sl_order_id = sl_order["id"]
                        trail_stage = 3
                        db_update_trade(trade_id, tp3_hit=1)
                        await notify(bot_client, f"🔄 {ticker} TP3 도달! SL → TP2({tp2}) | TP4({tp4}) 노림")
                    except Exception as e:
                        logger.error(f"Failed to move SL: {e}")

                # Check TP4 (final target)
                tp_status = exchange.fetch_order(tp_order_id, symbol)
                if tp_status["status"] == "closed":
                    try:
                        exchange.cancel_order(sl_order_id, symbol)
                    except Exception:
                        pass
                    pnl = round((avg_price - tp4) / avg_price * 100, 2)
                    pnl_usdt = round((avg_price - tp4) * filled_qty, 2)
                    record_pnl((avg_price - tp4) * filled_qty)
                    db_update_trade(trade_id, status="closed", result="tp4_hit",
                                    exit_price=tp4, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                    closed_at=datetime.now().isoformat())
                    logger.info(f"[FUTURES SHORT] {symbol} TP4 HIT! PnL: {pnl}%")
                    await notify(bot_client, f"🎯 {ticker} SHORT TP4 도달!\n수익률: {pnl}% | {pnl_usdt} USDT")
                    return

                # Check SL
                sl_status = exchange.fetch_order(sl_order_id, symbol)
                if sl_status["status"] == "closed":
                    try:
                        exchange.cancel_order(tp_order_id, symbol)
                    except Exception:
                        pass
                    sl_fill = sl_status["average"] or sl
                    pnl = round((avg_price - sl_fill) / avg_price * 100, 2)
                    pnl_usdt = round((avg_price - sl_fill) * filled_qty, 2)
                    record_pnl((avg_price - sl_fill) * filled_qty)
                    result = stage_labels[trail_stage]
                    db_update_trade(trade_id, status="closed", result=result,
                                    exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                    closed_at=datetime.now().isoformat())
                    logger.info(f"[FUTURES SHORT] {symbol} SL HIT @ {sl_fill} (stage {trail_stage}). PnL: {pnl}%")
                    await notify(bot_client, f"📊 {ticker} SHORT SL 도달 @ {sl_fill}\n단계: {trail_stage} | 수익률: {pnl}% | {pnl_usdt} USDT")
                    return

                # Check position exists
                positions = exchange.fetch_positions([symbol])
                active = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
                if not active:
                    for oid in [sl_order_id, tp_order_id]:
                        try:
                            exchange.cancel_order(oid, symbol)
                        except Exception:
                            pass
                    db_update_trade(trade_id, status="closed", result="external",
                                    closed_at=datetime.now().isoformat())
                    await notify(bot_client, f"📊 {ticker} SHORT 포지션 외부에서 종료됨")
                    return

            except ccxt.NetworkError as e:
                logger.warning(f"Network error: {e}")

            await asyncio.sleep(10)

    except Exception as e:
        if trade_id:
            db_update_trade(trade_id, status="error", result=str(e)[:200],
                            closed_at=datetime.now().isoformat())
        logger.error(f"[FUTURES SHORT] {symbol} error: {e}")
        await notify(bot_client, f"⚠️ {ticker} SHORT 에러: {e}")


# ── Telegram Notification (Bot API HTTP) ──────────────────

_http_client = httpx.AsyncClient(timeout=10)


async def notify(_, message):
    """Send notification via Bot API HTTP (no entity resolution needed)."""
    if not BOT_TOKEN or not MY_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        resp = await _http_client.post(url, json={"chat_id": MY_CHAT_ID, "text": message})
        if not resp.json().get("ok"):
            logger.error(f"Notify failed: {resp.text}")
    except Exception as e:
        logger.error(f"Failed to notify: {e}")


# ── Active Trades Tracking ────────────────────────────────

active_trades = {}
daily_realized_pnl = 0.0
_daily_reset_date = datetime.now().date()


def _check_daily_reset():
    global daily_realized_pnl, _daily_reset_date
    today = datetime.now().date()
    if today != _daily_reset_date:
        daily_realized_pnl = 0.0
        _daily_reset_date = today
        logger.info("Daily PnL reset")


def record_pnl(pnl_usdt):
    global daily_realized_pnl
    daily_realized_pnl += pnl_usdt
    logger.info(f"Daily realized PnL: {daily_realized_pnl:.2f} USDT")


# ── Dashboard Web Server ─────────────────────────────────

async def _serve_trades_dashboard(request):
    html_path = Path(__file__).parent / "templates" / "trades.html"
    return web.FileResponse(html_path)


async def _api_trades(request):
    status_filter = request.query.get("status")
    limit = min(int(request.query.get("limit", "100")), 500)
    trades = db_get_trades(limit=limit, status=status_filter)
    return web.json_response({"trades": trades})


async def _api_stats(request):
    stats = db_get_stats()
    stats["active_trades"] = list(active_trades.keys())
    stats["daily_realized_pnl"] = round(daily_realized_pnl, 2)
    return web.json_response(stats)


async def _api_get_settings(request):
    return web.json_response({
        "TRADE_AMOUNT": TRADE_AMOUNT,
        "SELL_BLOCKED": ",".join(sorted(SELL_BLOCKED)),
        "MAX_CONCURRENT": MAX_CONCURRENT,
        "DAILY_LOSS_LIMIT": DAILY_LOSS_LIMIT,
        "ENTRY_TIMEOUT": ENTRY_TIMEOUT,
    })


async def _api_post_settings(request):
    global TRADE_AMOUNT, SELL_BLOCKED, MAX_CONCURRENT, DAILY_LOSS_LIMIT, ENTRY_TIMEOUT
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    updates = {}
    if "TRADE_AMOUNT" in data:
        val = float(data["TRADE_AMOUNT"])
        if val <= 0:
            return web.json_response({"error": "TRADE_AMOUNT must be > 0"}, status=400)
        TRADE_AMOUNT = val
        updates["TRADE_AMOUNT"] = val
    if "SELL_BLOCKED" in data:
        raw = str(data["SELL_BLOCKED"]).strip()
        SELL_BLOCKED = {s.strip().upper() for s in raw.split(",") if s.strip()}
        updates["SELL_BLOCKED"] = raw.upper()
    if "MAX_CONCURRENT" in data:
        val = int(data["MAX_CONCURRENT"])
        if val < 1:
            return web.json_response({"error": "MAX_CONCURRENT must be >= 1"}, status=400)
        MAX_CONCURRENT = val
        updates["MAX_CONCURRENT"] = val
    if "DAILY_LOSS_LIMIT" in data:
        val = float(data["DAILY_LOSS_LIMIT"])
        if val <= 0:
            return web.json_response({"error": "DAILY_LOSS_LIMIT must be > 0"}, status=400)
        DAILY_LOSS_LIMIT = val
        updates["DAILY_LOSS_LIMIT"] = val
    if "ENTRY_TIMEOUT" in data:
        val = int(data["ENTRY_TIMEOUT"])
        if val < 10:
            return web.json_response({"error": "ENTRY_TIMEOUT must be >= 10"}, status=400)
        ENTRY_TIMEOUT = val
        updates["ENTRY_TIMEOUT"] = val

    if updates:
        db_save_settings(updates)
        logger.info(f"Settings updated via dashboard: {updates}")

    return web.json_response({
        "ok": True,
        "TRADE_AMOUNT": TRADE_AMOUNT,
        "SELL_BLOCKED": ",".join(sorted(SELL_BLOCKED)),
        "MAX_CONCURRENT": MAX_CONCURRENT,
        "DAILY_LOSS_LIMIT": DAILY_LOSS_LIMIT,
        "ENTRY_TIMEOUT": ENTRY_TIMEOUT,
    })


async def start_dashboard():
    app = web.Application()
    app.router.add_get("/", _serve_trades_dashboard)
    app.router.add_get("/api/trades", _api_trades)
    app.router.add_get("/api/stats", _api_stats)
    app.router.add_get("/api/settings", _api_get_settings)
    app.router.add_post("/api/settings", _api_post_settings)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", DASHBOARD_PORT)
    await site.start()
    logger.info(f"Trading dashboard at http://localhost:{DASHBOARD_PORT}")


# ── Main Bot ──────────────────────────────────────────────

async def main():
    global daily_realized_pnl

    if not all([API_ID, API_HASH, BINANCE_API_KEY, BINANCE_SECRET_KEY, SOURCE_CHANNELS]):
        logger.error("Missing required config. Check .env file.")
        sys.exit(1)

    # Initialize database and load saved settings
    init_db()
    apply_settings_from_db()
    daily_realized_pnl = db_get_today_pnl()
    logger.info(f"DB initialized. Today's realized PnL: {daily_realized_pnl:.2f} USDT")

    # Start dashboard
    await start_dashboard()

    # User client for monitoring channels
    user_client = TelegramClient("user_session", API_ID, API_HASH)
    await user_client.start()
    logger.info("User client connected")

    # Send startup notification via Bot API HTTP
    await notify(None, "🟢 트레이딩 봇 시작됨\n" + f"모니터링: {', '.join(SOURCE_CHANNELS)}\n투입: {TRADE_AMOUNT} USDT/신호")

    # Resolve channel entities
    source_entities = []
    for ch in SOURCE_CHANNELS:
        try:
            entity = await user_client.get_entity(ch)
            source_entities.append(entity)
            name = getattr(entity, "title", ch)
            logger.info(f"Monitoring: {name} (@{ch})")
        except Exception as e:
            logger.error(f"Cannot resolve channel '{ch}': {e}")

    if not source_entities:
        logger.error("No valid source channels. Exiting.")
        sys.exit(1)

    # Message handler
    @user_client.on(events.NewMessage(chats=source_entities))
    async def signal_handler(event):
        text = event.message.message
        if not text:
            return

        signal = parse_signal(text)
        if not signal:
            return  # 시그널 형식이 아닌 메시지 → 무시

        ticker = signal["ticker"]
        side = signal["side"]

        logger.info(f"Signal detected: #{ticker} – {side}")

        # 매도 금지 체크
        if ticker in SELL_BLOCKED and side == "SHORT":
            logger.info(f"BLOCKED: {ticker} SHORT is prohibited")
            await notify(None, f"⛔ {ticker} 매도 금지 종목. SHORT 시그널 무시.")
            return

        # 일일 손실 한도 체크
        _check_daily_reset()
        if daily_realized_pnl <= -DAILY_LOSS_LIMIT:
            logger.info(f"Daily loss limit reached: {daily_realized_pnl:.2f} USDT")
            await notify(None, f"⛔ 일일 손실 한도 도달 ({daily_realized_pnl:.2f}/{-DAILY_LOSS_LIMIT} USDT). 신호 무시.")
            return

        # 동시 포지션 제한
        if len(active_trades) >= MAX_CONCURRENT:
            logger.info(f"Max concurrent positions reached: {len(active_trades)}")
            await notify(None, f"⛔ 동시 포지션 한도 도달 ({len(active_trades)}/{MAX_CONCURRENT}개). 신호 무시.")
            return

        # 중복 거래 방지
        trade_key = f"{ticker}_{side}"
        if trade_key in active_trades:
            logger.info(f"Already trading {trade_key}, skipping")
            await notify(None, f"⏭️ {ticker} {side} 이미 진행 중. 스킵.")
            return

        active_trades[trade_key] = signal

        # 비동기로 거래 실행
        async def run_trade():
            try:
                if side == "LONG":
                    await execute_spot_long(signal, None)
                else:
                    await execute_futures_short(signal, None)
            finally:
                active_trades.pop(trade_key, None)

        asyncio.create_task(run_trade())

    logger.info(f"Trading bot running. Monitoring {len(source_entities)} channel(s). Ctrl+C to stop.")

    try:
        await user_client.run_until_disconnected()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await notify(None, "🔴 트레이딩 봇 종료됨")
        await _http_client.aclose()
        await user_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
