"""
Shared initialization for OpenClaw CLI scripts.

Resolves symlinks to find the TGForwarder project root, initializes the shared
database, loads dashboard settings, and provides exchange factory functions.

Usage in trade.py / watcher.py / monitor.py:
    from shared_settings import init_openclaw, create_exchange
    settings, config = init_openclaw()
"""

import os
import sys
from pathlib import Path

# Resolve symlinks to find project root (openclaw_trader/ -> parent is project root)
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from core.database import (
    init_db, db_load_settings, db_get_today_pnl,
    db_get_active_openclaw_trades, db_get_active_trades_by_symbol,
    db_insert_openclaw_trade, db_update_trade, db_get_trade,
)
from core.config import load_config, AppConfig

DATA_DIR = Path.home() / ".tgforwarder"


def init_openclaw():
    """Initialize shared infrastructure. Returns (settings_dict, AppConfig).

    Steps:
    1. Ensure ~/.tgforwarder exists
    2. init_db(DATA_DIR)
    3. Load .env + AppConfig
    4. Load settings from DB (overrides config defaults)
    5. Return (settings, config)
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db(DATA_DIR)
    config = load_config(DATA_DIR)

    db_settings = db_load_settings()

    settings = {
        "TRADE_AMOUNT": float(db_settings.get("TRADE_AMOUNT", str(config.trade_amount))),
        "SELL_BLOCKED": _parse_set(db_settings.get("SELL_BLOCKED", "")),
        "TRADE_BLOCKED": _parse_set(db_settings.get("TRADE_BLOCKED", "")),
        "DAILY_LOSS_LIMIT": float(db_settings.get("DAILY_LOSS_LIMIT", str(config.daily_loss_limit))),
        "ENTRY_TIMEOUT": int(db_settings.get("ENTRY_TIMEOUT", str(config.entry_timeout))),
        "MAX_LEVERAGE": int(db_settings.get("MAX_LEVERAGE", str(config.max_leverage))),
        "MAX_CONCURRENT": int(db_settings.get("MAX_CONCURRENT", str(config.max_concurrent))),
    }
    return settings, config


def _parse_set(raw):
    return {s.strip().upper() for s in raw.split(",") if s.strip()}


def is_daily_limit_hit(settings):
    """Check if daily loss limit has been reached (across all sources)."""
    today_pnl = db_get_today_pnl()
    return today_pnl <= -settings["DAILY_LOSS_LIMIT"]


def make_symbol(ticker, futures=False, exchange_name="binance"):
    """Format ccxt symbol from ticker."""
    if exchange_name == "okx" and futures:
        return f"{ticker}/USDT:USDT"
    return f"{ticker}/USDT"


def create_exchange(config, exchange_name="binance", futures=False):
    """Create a sync ccxt exchange instance. For trade.py and monitor.py."""
    import ccxt

    if exchange_name == "okx":
        exc_config = {
            "apiKey": config.okx_api_key,
            "secret": config.okx_secret_key,
            "password": config.okx_passphrase,
            "enableRateLimit": True,
            "hostname": "www.okx.cab",
        }
        if futures:
            exc_config["options"] = {"defaultType": "swap"}
        exc = ccxt.okx(exc_config)
        exc.load_markets()
        if futures:
            try:
                exc.set_position_mode(False)
            except Exception:
                pass
    else:
        exc_config = {
            "apiKey": config.binance_api_key,
            "secret": config.binance_secret_key,
            "enableRateLimit": True,
        }
        if futures:
            exc_config["options"] = {"defaultType": "future"}
        exc = ccxt.binance(exc_config)
        exc.load_markets()
    return exc


async def create_async_exchange(config, exchange_name="binance", futures=False):
    """Create an async ccxt exchange instance. For watcher.py."""
    import ccxt.async_support as ccxt_async

    if exchange_name == "okx":
        exc_config = {
            "apiKey": config.okx_api_key,
            "secret": config.okx_secret_key,
            "password": config.okx_passphrase,
            "enableRateLimit": True,
            "hostname": "www.okx.cab",
        }
        if futures:
            exc_config["options"] = {"defaultType": "swap"}
        exc = ccxt_async.okx(exc_config)
        await exc.load_markets()
        if futures:
            try:
                await exc.set_position_mode(False)
            except Exception:
                pass
    else:
        exc_config = {
            "apiKey": config.binance_api_key,
            "secret": config.binance_secret_key,
            "enableRateLimit": True,
        }
        if futures:
            exc_config["options"] = {"defaultType": "future"}
        exc = ccxt_async.binance(exc_config)
        await exc.load_markets()
    return exc


# ── OKX Order Helpers (sync, for trade.py) ──────────────

def create_sl_order(exchange, exchange_name, symbol, side, qty, price, futures=False):
    """Create a stop-loss order appropriate to the exchange."""
    close_side = "sell" if side == "LONG" else "buy"
    if exchange_name == "okx":
        params = {"triggerPrice": str(price), "triggerType": "last"}
        return exchange.create_order(symbol, "trigger", close_side, qty, price, params)
    else:
        if futures:
            return exchange.create_order(symbol, "stop_market", close_side, qty, None,
                                         {"stopPrice": price, "reduceOnly": True})
        elif side == "LONG":
            return exchange.create_order(symbol, "stop_loss_limit", close_side, qty, price,
                                         {"stopPrice": price})
        else:
            return exchange.create_order(symbol, "stop_market", close_side, qty, None,
                                         {"stopPrice": price, "reduceOnly": True})


def create_tp_order(exchange, exchange_name, symbol, side, qty, price, futures=False):
    """Create a take-profit order appropriate to the exchange."""
    close_side = "sell" if side == "LONG" else "buy"
    if exchange_name == "okx":
        params = {"triggerPrice": str(price), "triggerType": "last"}
        return exchange.create_order(symbol, "trigger", close_side, qty, price, params)
    else:
        if futures:
            return exchange.create_order(symbol, "take_profit_market", close_side, qty, None,
                                         {"stopPrice": price, "reduceOnly": True})
        elif side == "LONG":
            return exchange.create_limit_sell_order(symbol, qty, price)
        else:
            return exchange.create_order(symbol, "take_profit_market", close_side, qty, None,
                                         {"stopPrice": price, "reduceOnly": True})


def fetch_exit_order(exchange, exchange_name, order_id, symbol):
    """Fetch SL/TP order status. OKX algo orders need params={'stop': True}."""
    if exchange_name == "okx":
        return exchange.fetch_order(order_id, symbol, params={"stop": True})
    return exchange.fetch_order(order_id, symbol)


def cancel_exit_order(exchange, exchange_name, order_id, symbol):
    """Cancel SL/TP order. OKX algo orders need params={'stop': True}."""
    if exchange_name == "okx":
        exchange.cancel_order(order_id, symbol, params={"stop": True})
    else:
        exchange.cancel_order(order_id, symbol)


def cancel_exit_orders_safe(exchange, exchange_name, symbol, order_ids):
    """Cancel a list of SL/TP orders, ignoring errors."""
    for oid in order_ids:
        if not oid:
            continue
        try:
            cancel_exit_order(exchange, exchange_name, oid, symbol)
        except Exception:
            pass
