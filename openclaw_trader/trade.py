#!/usr/bin/env python3
"""
Trade Executor (Binance + OKX)

Places entry orders with SL/TP, records to the unified database.
Position monitoring is handled separately by watcher.py.

Usage:
    python3 trade.py --ticker BTCUSDT --side LONG --entry 66400 \
        --tp1 68000 --tp2 70000 --tp3 72000 --tp4 74000 \
        --sl 63000 --amount 100 --exchange binance
"""

import argparse
import json
import sys
import time

from shared_settings import (
    init_openclaw, create_exchange, make_symbol,
    create_sl_order, create_tp_order, is_daily_limit_hit,
)
from core.database import (
    db_insert_openclaw_trade, db_update_trade, db_get_trade,
)


def get_tick_info(exchange, symbol):
    market = exchange.market(symbol)
    return {
        "price_precision": market["precision"]["price"],
        "amount_precision": market["precision"]["amount"],
        "min_amount": market.get("limits", {}).get("amount", {}).get("min", 0),
        "min_cost": market.get("limits", {}).get("cost", {}).get("min", 0),
    }


def _wait_for_fill(exchange, order_id, symbol, timeout):
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


def execute_long(exchange, exchange_name, symbol, ticker, entry, tp1, tp2, tp3, tp4,
                 sl, amount_usdt, settings, signal_text=None):
    """Execute LONG trade. Spot on Binance, futures 1x on OKX. Returns result dict."""
    futures = (exchange_name == "okx")
    market_type = "futures" if futures else "spot"
    leverage = 1

    info = get_tick_info(exchange, symbol)
    qty = round(amount_usdt / entry, info["amount_precision"])

    print(f"[{'FUTURES' if futures else 'SPOT'} LONG] {symbol} ({exchange_name})")
    print(f"  Entry: {entry} | Qty: {qty} | Cost: ~{amount_usdt} USDT")
    print(f"  TP1: {tp1} | TP3: {tp3} | SL: {sl}")

    # Set leverage / margin for futures
    if futures:
        try:
            exchange.set_leverage(leverage, symbol)
        except Exception as e:
            print(f"  Leverage note: {e}")
        try:
            exchange.set_margin_mode("isolated", symbol)
        except Exception:
            pass

    # Record position in unified DB
    trade_id = db_insert_openclaw_trade(
        ticker=ticker, side="long", entry_price=entry, qty=qty,
        amount_usdt=amount_usdt, tp1=tp1, tp2=tp2, tp3=tp3, tp4=tp4,
        sl=sl, sl_initial=sl, market_type=market_type,
        leverage=leverage, exchange_name=exchange_name,
        signal_text=signal_text,
    )

    # Place limit buy at entry price
    order = exchange.create_limit_buy_order(symbol, qty, entry)
    order_id = order["id"]
    print(f"  Entry order: {order_id}")

    # Wait for fill
    entry_timeout = settings["ENTRY_TIMEOUT"]
    print(f"  Waiting for entry fill (timeout {entry_timeout}s)...")
    status, filled_qty, avg_price = _wait_for_fill(exchange, order_id, symbol, entry_timeout)

    if status == "canceled":
        print("  Entry CANCELED.")
        db_update_trade(trade_id, status="closed")
        return {"status": "canceled", "reason": "entry_canceled"}

    if status == "timeout":
        print(f"  Entry timeout ({entry_timeout}s). Canceling.")
        try:
            exchange.cancel_order(order_id, symbol)
        except Exception:
            pass
        db_update_trade(trade_id, status="closed")
        return {"status": "timeout", "reason": "entry_timeout"}

    print(f"  FILLED: {filled_qty} @ {avg_price}")
    db_update_trade(trade_id,
                    status="open", filled_price=avg_price,
                    qty=filled_qty, remaining_qty=filled_qty)

    # Place SL order
    try:
        sl_order = create_sl_order(exchange, exchange_name, symbol, "LONG",
                                   filled_qty, sl, futures=futures)
        sl_oid = sl_order["id"]
        print(f"  SL order: {sl_oid} @ {sl}")
        db_update_trade(trade_id, sl_order_id=sl_oid)
    except Exception as e:
        print(f"  WARNING: SL order failed: {e}")

    # Place TP order at TP3
    try:
        tp_order = create_tp_order(exchange, exchange_name, symbol, "LONG",
                                   filled_qty, tp3, futures=futures)
        tp_oid = tp_order["id"]
        print(f"  TP3 order: {tp_oid} @ {tp3}")
        db_update_trade(trade_id, tp_order_id=tp_oid)
    except Exception as e:
        print(f"  WARNING: TP order failed: {e}")

    print("  Position active. watcher.py handles ongoing management.")
    return {
        "status": "active", "trade_id": trade_id,
        "filled_qty": filled_qty, "avg_price": avg_price,
        "sl": sl, "tp3": tp3,
    }


def execute_short(exchange, exchange_name, symbol, ticker, entry, tp1, tp2, tp3, tp4,
                  sl, amount_usdt, settings, signal_text=None):
    """Execute SHORT trade on Futures (1x leverage). Returns result dict."""
    info = get_tick_info(exchange, symbol)
    qty = round(amount_usdt / entry, info["amount_precision"])

    print(f"[FUTURES SHORT] {symbol} ({exchange_name})")
    print(f"  Entry: {entry} | Qty: {qty} | Cost: ~{amount_usdt} USDT")
    print(f"  TP1: {tp1} | TP3: {tp3} | SL: {sl}")

    # Set leverage 1x, isolated margin
    leverage = 1
    try:
        exchange.set_leverage(leverage, symbol)
    except Exception as e:
        print(f"  Leverage note: {e}")
    try:
        exchange.set_margin_mode("isolated", symbol)
    except Exception:
        pass

    # Record position in unified DB
    trade_id = db_insert_openclaw_trade(
        ticker=ticker, side="short", entry_price=entry, qty=qty,
        amount_usdt=amount_usdt, tp1=tp1, tp2=tp2, tp3=tp3, tp4=tp4,
        sl=sl, sl_initial=sl, market_type="futures",
        leverage=leverage, exchange_name=exchange_name,
        signal_text=signal_text,
    )

    # Place limit sell (short) at entry
    order = exchange.create_limit_sell_order(symbol, qty, entry)
    order_id = order["id"]
    print(f"  Short entry order: {order_id}")

    # Wait for fill
    entry_timeout = settings["ENTRY_TIMEOUT"]
    print(f"  Waiting for entry fill (timeout {entry_timeout}s)...")
    status, filled_qty, avg_price = _wait_for_fill(exchange, order_id, symbol, entry_timeout)

    if status == "canceled":
        print("  Entry CANCELED.")
        db_update_trade(trade_id, status="closed")
        return {"status": "canceled", "reason": "entry_canceled"}

    if status == "timeout":
        print(f"  Entry timeout ({entry_timeout}s). Canceling.")
        try:
            exchange.cancel_order(order_id, symbol)
        except Exception:
            pass
        db_update_trade(trade_id, status="closed")
        return {"status": "timeout", "reason": "entry_timeout"}

    print(f"  FILLED: {filled_qty} @ {avg_price}")
    db_update_trade(trade_id,
                    status="open", filled_price=avg_price,
                    qty=filled_qty, remaining_qty=filled_qty)

    # Place SL (exchange-appropriate)
    try:
        sl_order = create_sl_order(exchange, exchange_name, symbol, "SHORT",
                                   filled_qty, sl, futures=True)
        sl_oid = sl_order["id"]
        print(f"  SL order: {sl_oid} @ {sl}")
        db_update_trade(trade_id, sl_order_id=sl_oid)
    except Exception as e:
        print(f"  WARNING: SL order failed: {e}")

    # Place TP at TP3 (exchange-appropriate)
    try:
        tp_order = create_tp_order(exchange, exchange_name, symbol, "SHORT",
                                   filled_qty, tp3, futures=True)
        tp_oid = tp_order["id"]
        print(f"  TP3 order: {tp_oid} @ {tp3}")
        db_update_trade(trade_id, tp_order_id=tp_oid)
    except Exception as e:
        print(f"  WARNING: TP order failed: {e}")

    print("  Position active. watcher.py handles ongoing management.")
    return {
        "status": "active", "trade_id": trade_id,
        "filled_qty": filled_qty, "avg_price": avg_price,
        "sl": sl, "tp3": tp3,
    }


def main():
    # Initialize shared settings + DB first (needed for defaults)
    settings, config = init_openclaw()

    parser = argparse.ArgumentParser(description="Trade Executor (Binance + OKX)")
    parser.add_argument("--ticker", required=True, help="Trading pair (e.g. BTCUSDT)")
    parser.add_argument("--side", required=True, choices=["LONG", "SHORT"])
    parser.add_argument("--entry", required=True, type=float)
    parser.add_argument("--tp1", required=True, type=float)
    parser.add_argument("--tp2", required=True, type=float)
    parser.add_argument("--tp3", required=True, type=float)
    parser.add_argument("--tp4", required=True, type=float)
    parser.add_argument("--sl", required=True, type=float)
    parser.add_argument("--amount", type=float, default=None,
                        help=f"USDT per trade (default: {settings['TRADE_AMOUNT']} from settings)")
    parser.add_argument("--exchange", choices=["binance", "okx"], default="binance",
                        help="Exchange to use (default: binance)")
    parser.add_argument("--signal", type=str, default=None, help="Original signal text")
    args = parser.parse_args()

    # Resolve trade amount: CLI > settings default
    amount_usdt = args.amount if args.amount is not None else settings["TRADE_AMOUNT"]

    # Normalize ticker: ensure USDT suffix, extract base
    symbol_raw = args.ticker.upper()
    if not symbol_raw.endswith("USDT"):
        symbol_raw += "USDT"
    ticker = symbol_raw.replace("USDT", "")  # base name for DB column

    exchange_name = args.exchange

    # Trade-blocked check (all directions)
    if ticker in settings["TRADE_BLOCKED"]:
        print(f"BLOCKED: {ticker} is trade-blocked (all directions).")
        sys.exit(1)

    # Sell-blocked check (SHORT only)
    if ticker in settings["SELL_BLOCKED"] and args.side == "SHORT":
        print(f"BLOCKED: {ticker} SHORT prohibited.")
        sys.exit(1)

    # Daily loss limit check
    if is_daily_limit_hit(settings):
        limit = settings["DAILY_LOSS_LIMIT"]
        print(f"BLOCKED: Daily loss limit ({limit} USDT). No new entries for 24h.")
        sys.exit(1)

    print(f"{'=' * 50}")
    print(f"Signal: #{ticker} - {args.side}")
    print(f"Exchange: {exchange_name}")
    print(f"Entry: {args.entry} | SL: {args.sl}")
    print(f"TP: {args.tp1}, {args.tp2}, {args.tp3}, {args.tp4}")
    print(f"Amount: {amount_usdt} USDT")
    print(f"{'=' * 50}")

    if args.side == "LONG":
        # Binance LONG -> spot (futures=False)
        # OKX LONG -> futures 1x (futures=True)
        futures = (exchange_name == "okx")
        exchange = create_exchange(config, exchange_name, futures=futures)
        ccxt_symbol = make_symbol(ticker, futures=futures, exchange_name=exchange_name)
        result = execute_long(
            exchange, exchange_name, ccxt_symbol, ticker,
            args.entry, args.tp1, args.tp2, args.tp3, args.tp4,
            args.sl, amount_usdt, settings, args.signal,
        )
    else:
        # SHORT always uses futures
        exchange = create_exchange(config, exchange_name, futures=True)
        ccxt_symbol = make_symbol(ticker, futures=True, exchange_name=exchange_name)
        result = execute_short(
            exchange, exchange_name, ccxt_symbol, ticker,
            args.entry, args.tp1, args.tp2, args.tp3, args.tp4,
            args.sl, amount_usdt, settings, args.signal,
        )

    print(f"\nResult: {json.dumps(result, indent=2)}")
    return result


if __name__ == "__main__":
    main()
