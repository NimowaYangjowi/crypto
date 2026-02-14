#!/usr/bin/env python3
"""
Binance Trade Executor

Places entry orders with SL/TP on Binance, records to database.
Position monitoring is handled separately by watcher.py.

Usage:
    python3 trade.py --ticker BTCUSDT --side LONG --entry 66400 \
        --tp1 68000 --tp2 70000 --tp3 72000 --tp4 74000 \
        --sl 63000 --amount 100
"""

import argparse
import json
import os
import sys
import time

# Ensure sibling imports work regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ccxt
from db import TradeDB

ENTRY_TIMEOUT = 1800  # 30 minutes max wait for entry fill
SELL_BLOCKED = {"BTC", "XRP"}


def get_exchange(futures=False):
    api_key = os.environ.get("BINANCE_API_KEY")
    secret = os.environ.get("BINANCE_SECRET_KEY")
    if not api_key or not secret:
        print("ERROR: BINANCE_API_KEY and BINANCE_SECRET_KEY must be set")
        sys.exit(1)
    config = {
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
    }
    if futures:
        config["options"] = {"defaultType": "future"}
    exchange = ccxt.binance(config)
    exchange.load_markets()
    return exchange


def get_tick_info(exchange, symbol):
    market = exchange.market(symbol)
    return {
        "price_precision": market["precision"]["price"],
        "amount_precision": market["precision"]["amount"],
        "min_amount": market.get("limits", {}).get("amount", {}).get("min", 0),
        "min_cost": market.get("limits", {}).get("cost", {}).get("min", 0),
    }


def _wait_for_fill(exchange, order_id, symbol, timeout=ENTRY_TIMEOUT):
    """Wait for order fill with timeout. Returns (status, filled_qty, avg_price)."""
    start = time.time()
    while time.time() - start < timeout:
        o = exchange.fetch_order(order_id, symbol)
        if o["status"] == "closed":
            return "filled", o["filled"], o["average"] or o["price"]
        if o["status"] == "canceled":
            return "canceled", 0, 0
        time.sleep(5)
    return "timeout", 0, 0


def execute_long(exchange, db, symbol, entry, tp1, tp2, tp3, tp4, sl, amount_usdt, signal_text=None):
    """Execute LONG trade on Spot market. Returns result dict."""
    info = get_tick_info(exchange, symbol)
    qty = round(amount_usdt / entry, info["amount_precision"])

    print(f"[SPOT LONG] {symbol}")
    print(f"  Entry: {entry} | Qty: {qty} | Cost: ~{amount_usdt} USDT")
    print(f"  TP1: {tp1} | TP3: {tp3} | SL: {sl}")

    # Record position in DB
    pos_id = db.create_position(
        symbol=symbol, side="long", market_type="spot",
        entry_price=entry, quantity=qty, amount_usdt=amount_usdt,
        sl_price=sl, sl_initial=sl,
        tp1_price=tp1, tp2_price=tp2, tp3_price=tp3, tp4_price=tp4,
        leverage=1, status="pending", signal_text=signal_text,
    )

    # Place limit buy at entry price
    order = exchange.create_limit_buy_order(symbol, qty, entry)
    order_id = order["id"]
    print(f"  Entry order: {order_id}")
    db.create_order(
        position_id=pos_id, exchange_order_id=order_id,
        symbol=symbol, side="buy", order_type="limit",
        price=entry, quantity=qty, purpose="entry",
    )

    # Wait for fill
    print("  Waiting for entry fill...")
    status, filled_qty, avg_price = _wait_for_fill(exchange, order_id, symbol)

    if status == "canceled":
        print("  Entry CANCELED.")
        db.update_position(pos_id, {"status": "closed", "close_reason": "entry_canceled"})
        return {"status": "canceled", "reason": "entry_canceled"}

    if status == "timeout":
        print(f"  Entry timeout ({ENTRY_TIMEOUT}s). Canceling.")
        try:
            exchange.cancel_order(order_id, symbol)
        except Exception:
            pass
        db.update_position(pos_id, {"status": "closed", "close_reason": "entry_timeout"})
        return {"status": "timeout", "reason": "entry_timeout"}

    print(f"  FILLED: {filled_qty} @ {avg_price}")
    db.update_position(pos_id, {
        "status": "active", "filled_price": avg_price,
        "quantity": filled_qty, "remaining_qty": filled_qty,
    })

    # Place SL order
    try:
        sl_order = exchange.create_order(
            symbol, "stop_loss_limit", "sell", filled_qty, sl,
            {"stopPrice": sl},
        )
        sl_oid = sl_order["id"]
        print(f"  SL order: {sl_oid} @ {sl}")
        db.update_position(pos_id, {"sl_order_id": sl_oid})
        db.create_order(
            position_id=pos_id, exchange_order_id=sl_oid,
            symbol=symbol, side="sell", order_type="stop_loss_limit",
            price=sl, stop_price=sl, quantity=filled_qty, purpose="sl",
        )
    except Exception as e:
        print(f"  WARNING: SL order failed: {e}")

    # Place TP order at TP3
    try:
        tp_order = exchange.create_limit_sell_order(symbol, filled_qty, tp3)
        tp_oid = tp_order["id"]
        print(f"  TP3 order: {tp_oid} @ {tp3}")
        db.update_position(pos_id, {"tp_order_id": tp_oid})
        db.create_order(
            position_id=pos_id, exchange_order_id=tp_oid,
            symbol=symbol, side="sell", order_type="limit",
            price=tp3, quantity=filled_qty, purpose="tp",
        )
    except Exception as e:
        print(f"  WARNING: TP order failed: {e}")

    print("  Position active. watcher.py handles ongoing management.")
    return {
        "status": "active", "position_id": pos_id,
        "filled_qty": filled_qty, "avg_price": avg_price,
        "sl": sl, "tp3": tp3,
    }


def execute_short(exchange, db, symbol, entry, tp1, tp2, tp3, tp4, sl, amount_usdt, signal_text=None):
    """Execute SHORT trade on Futures (1x leverage). Returns result dict."""
    info = get_tick_info(exchange, symbol)
    qty = round(amount_usdt / entry, info["amount_precision"])

    print(f"[FUTURES SHORT] {symbol}")
    print(f"  Entry: {entry} | Qty: {qty} | Cost: ~{amount_usdt} USDT")
    print(f"  TP1: {tp1} | TP3: {tp3} | SL: {sl}")

    # Set leverage 1x, isolated margin
    try:
        exchange.set_leverage(1, symbol)
    except Exception as e:
        print(f"  Leverage note: {e}")
    try:
        exchange.set_margin_mode("isolated", symbol)
    except Exception:
        pass

    # Record position in DB
    pos_id = db.create_position(
        symbol=symbol, side="short", market_type="futures",
        entry_price=entry, quantity=qty, amount_usdt=amount_usdt,
        sl_price=sl, sl_initial=sl,
        tp1_price=tp1, tp2_price=tp2, tp3_price=tp3, tp4_price=tp4,
        leverage=1, status="pending", signal_text=signal_text,
    )

    # Place limit sell (short) at entry
    order = exchange.create_limit_sell_order(symbol, qty, entry)
    order_id = order["id"]
    print(f"  Short entry order: {order_id}")
    db.create_order(
        position_id=pos_id, exchange_order_id=order_id,
        symbol=symbol, side="sell", order_type="limit",
        price=entry, quantity=qty, purpose="entry",
    )

    # Wait for fill
    print("  Waiting for entry fill...")
    status, filled_qty, avg_price = _wait_for_fill(exchange, order_id, symbol)

    if status == "canceled":
        print("  Entry CANCELED.")
        db.update_position(pos_id, {"status": "closed", "close_reason": "entry_canceled"})
        return {"status": "canceled", "reason": "entry_canceled"}

    if status == "timeout":
        print(f"  Entry timeout ({ENTRY_TIMEOUT}s). Canceling.")
        try:
            exchange.cancel_order(order_id, symbol)
        except Exception:
            pass
        db.update_position(pos_id, {"status": "closed", "close_reason": "entry_timeout"})
        return {"status": "timeout", "reason": "entry_timeout"}

    print(f"  FILLED: {filled_qty} @ {avg_price}")
    db.update_position(pos_id, {
        "status": "active", "filled_price": avg_price,
        "quantity": filled_qty, "remaining_qty": filled_qty,
    })

    # Place SL (STOP_MARKET)
    try:
        sl_order = exchange.create_order(
            symbol, "stop_market", "buy", filled_qty, None,
            {"stopPrice": sl, "reduceOnly": True},
        )
        sl_oid = sl_order["id"]
        print(f"  SL order: {sl_oid} @ {sl}")
        db.update_position(pos_id, {"sl_order_id": sl_oid})
        db.create_order(
            position_id=pos_id, exchange_order_id=sl_oid,
            symbol=symbol, side="buy", order_type="stop_market",
            stop_price=sl, quantity=filled_qty, purpose="sl",
        )
    except Exception as e:
        print(f"  WARNING: SL order failed: {e}")

    # Place TP at TP3 (TAKE_PROFIT_MARKET)
    try:
        tp_order = exchange.create_order(
            symbol, "take_profit_market", "buy", filled_qty, None,
            {"stopPrice": tp3, "reduceOnly": True},
        )
        tp_oid = tp_order["id"]
        print(f"  TP3 order: {tp_oid} @ {tp3}")
        db.update_position(pos_id, {"tp_order_id": tp_oid})
        db.create_order(
            position_id=pos_id, exchange_order_id=tp_oid,
            symbol=symbol, side="buy", order_type="take_profit_market",
            stop_price=tp3, quantity=filled_qty, purpose="tp",
        )
    except Exception as e:
        print(f"  WARNING: TP order failed: {e}")

    print("  Position active. watcher.py handles ongoing management.")
    return {
        "status": "active", "position_id": pos_id,
        "filled_qty": filled_qty, "avg_price": avg_price,
        "sl": sl, "tp3": tp3,
    }


def main():
    parser = argparse.ArgumentParser(description="Binance Trade Executor")
    parser.add_argument("--ticker", required=True, help="Trading pair (e.g. BTCUSDT)")
    parser.add_argument("--side", required=True, choices=["LONG", "SHORT"])
    parser.add_argument("--entry", required=True, type=float)
    parser.add_argument("--tp1", required=True, type=float)
    parser.add_argument("--tp2", required=True, type=float)
    parser.add_argument("--tp3", required=True, type=float)
    parser.add_argument("--tp4", required=True, type=float)
    parser.add_argument("--sl", required=True, type=float)
    parser.add_argument("--amount", type=float, default=100, help="USDT per trade")
    parser.add_argument("--signal", type=str, default=None, help="Original signal text")
    args = parser.parse_args()

    # Normalize ticker
    symbol = args.ticker.upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    base = symbol.replace("USDT", "")
    ccxt_symbol = f"{base}/USDT"

    # Sell-blocked check
    if base in SELL_BLOCKED and args.side == "SHORT":
        print(f"BLOCKED: {base} SHORT prohibited.")
        sys.exit(1)

    # Daily loss limit check
    db = TradeDB()
    if db.is_loss_limit_hit():
        print("BLOCKED: Daily loss limit (500 USDT). No new entries for 24h.")
        db.close()
        sys.exit(1)

    print(f"{'=' * 50}")
    print(f"Signal: #{base} - {args.side}")
    print(f"Entry: {args.entry} | SL: {args.sl}")
    print(f"TP: {args.tp1}, {args.tp2}, {args.tp3}, {args.tp4}")
    print(f"Amount: {args.amount} USDT")
    print(f"{'=' * 50}")

    if args.side == "LONG":
        exchange = get_exchange(futures=False)
        result = execute_long(
            exchange, db, ccxt_symbol,
            args.entry, args.tp1, args.tp2, args.tp3, args.tp4,
            args.sl, args.amount, args.signal,
        )
    else:
        exchange = get_exchange(futures=True)
        result = execute_short(
            exchange, db, ccxt_symbol,
            args.entry, args.tp1, args.tp2, args.tp3, args.tp4,
            args.sl, args.amount, args.signal,
        )

    print(f"\nResult: {json.dumps(result, indent=2)}")
    db.close()
    return result


if __name__ == "__main__":
    main()
