#!/usr/bin/env python3
"""
Position Monitor - Reports from both local database and exchange.

Usage:
    python3 monitor.py                  # Full status (DB + exchange)
    python3 monitor.py --positions      # Active positions from DB
    python3 monitor.py --history        # Recent trade history
    python3 monitor.py --pnl            # Today's PnL
    python3 monitor.py --spot           # Spot balances (exchange)
    python3 monitor.py --futures        # Futures positions (exchange)
    python3 monitor.py --orders         # Open orders (exchange)
    python3 monitor.py --cancel SYMBOL  # Cancel all open orders for SYMBOL
"""

import argparse
import json
import os
import sys

# Ensure sibling imports work regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ccxt
from db import TradeDB


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


# --- DB-based views ---

def show_db_positions(db):
    print("\n=== ACTIVE POSITIONS (DB) ===")
    positions = db.get_active_positions()
    if not positions:
        print("  No active positions.")
        return
    for p in positions:
        side = p["side"].upper()
        symbol = p["symbol"]
        entry = p["filled_price"] or p["entry_price"]
        sl = p["sl_price"]
        be = " [BE]" if p["sl_moved_to_breakeven"] else ""
        tp3 = p.get("tp3_price", "?")
        status = p["status"].upper()
        mtype = p["market_type"]
        print(f"  [{p['id']}] {symbol} {side} ({mtype}) | {status}")
        print(f"      Entry: {entry} | SL: {sl}{be} | TP3: {tp3}")
        print(f"      Qty: {p['quantity']} | USDT: {p['amount_usdt']}")


def show_history(db, limit=10):
    print(f"\n=== TRADE HISTORY (last {limit}) ===")
    positions = db.get_position_history(limit)
    if not positions:
        print("  No trade history.")
        return
    for p in positions:
        side = p["side"].upper()
        symbol = p["symbol"]
        reason = p.get("close_reason") or "-"
        pnl = p.get("realized_pnl", 0)
        pnl_str = f"{pnl:+.4f}" if pnl else "0"
        status = p["status"].upper()
        print(f"  [{p['id']}] {symbol} {side} | {status} ({reason}) | PnL: {pnl_str} USDT | {p['created_at']}")


def show_daily_pnl(db):
    print("\n=== TODAY'S PnL ===")
    daily = db.get_daily_pnl()
    if not daily:
        print("  No trades today.")
        return
    pnl = daily["realized_pnl"]
    print(f"  PnL: {pnl:+.4f} USDT")
    print(f"  Trades: {daily['trade_count']} ({daily['wins']}W / {daily['losses']}L)")
    if daily.get("loss_limit_hit"):
        print(f"  *** LOSS LIMIT HIT at {daily['loss_limit_hit_at']} ***")


# --- Exchange-based views ---

def show_spot_balances(exchange):
    print("\n=== SPOT BALANCES ===")
    balance = exchange.fetch_balance()
    has_assets = False
    for currency, amount in balance["total"].items():
        if amount > 0:
            free = balance["free"].get(currency, 0)
            used = balance["used"].get(currency, 0)
            print(f"  {currency}: {amount:.8f} (free: {free:.8f}, in orders: {used:.8f})")
            has_assets = True
    if not has_assets:
        print("  No assets found.")


def show_futures_positions(exchange):
    print("\n=== FUTURES POSITIONS (EXCHANGE) ===")
    positions = exchange.fetch_positions()
    active = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
    if not active:
        print("  No active positions.")
        return
    for p in active:
        symbol = p["symbol"]
        side = p["side"]
        size = p["contracts"]
        entry = p.get("entryPrice", "?")
        pnl = p.get("unrealizedPnl", 0)
        leverage = p.get("leverage", "?")
        margin = p.get("initialMargin", "?")
        print(f"  {symbol} {side.upper()} x{leverage}")
        print(f"    Size: {size} | Entry: {entry}")
        print(f"    Unrealized PnL: {pnl:.4f} USDT | Margin: {margin}")


def show_futures_balance(exchange):
    print("\n=== FUTURES BALANCE ===")
    balance = exchange.fetch_balance()
    usdt = balance.get("USDT", {})
    print(f"  Total: {usdt.get('total', 0):.4f} USDT")
    print(f"  Free: {usdt.get('free', 0):.4f} USDT")
    print(f"  Used: {usdt.get('used', 0):.4f} USDT")


def show_open_orders(exchange, symbol=None):
    print(f"\n=== OPEN ORDERS {f'({symbol})' if symbol else ''} ===")
    orders = exchange.fetch_open_orders(symbol)
    if not orders:
        print("  No open orders.")
        return
    for o in orders:
        side = o["side"].upper()
        otype = o["type"]
        price = o.get("price", "market")
        stop = o.get("stopPrice", "")
        amount = o["amount"]
        filled = o.get("filled", 0)
        print(f"  [{o['id']}] {o['symbol']} {side} {otype} @ {price} (stop: {stop})")
        print(f"    Amount: {amount} | Filled: {filled}")


def cancel_all_orders(exchange, symbol):
    ccxt_symbol = symbol.upper()
    if "/" not in ccxt_symbol:
        base = ccxt_symbol.replace("USDT", "")
        ccxt_symbol = f"{base}/USDT"
    orders = exchange.fetch_open_orders(ccxt_symbol)
    if not orders:
        print(f"No open orders for {ccxt_symbol}.")
        return
    for o in orders:
        exchange.cancel_order(o["id"], ccxt_symbol)
        print(f"Canceled: {o['id']} ({o['side']} {o['type']} @ {o.get('price', 'market')})")
    print(f"Canceled {len(orders)} orders for {ccxt_symbol}.")


def main():
    parser = argparse.ArgumentParser(description="Position Monitor")
    parser.add_argument("--positions", action="store_true", help="Active positions from DB")
    parser.add_argument("--history", action="store_true", help="Trade history from DB")
    parser.add_argument("--pnl", action="store_true", help="Today's PnL")
    parser.add_argument("--spot", action="store_true", help="Spot balances (exchange)")
    parser.add_argument("--futures", action="store_true", help="Futures positions (exchange)")
    parser.add_argument("--orders", action="store_true", help="Open orders (exchange)")
    parser.add_argument("--cancel", type=str, help="Cancel all orders for SYMBOL")
    parser.add_argument("--symbol", type=str, help="Filter by symbol")
    args = parser.parse_args()

    show_all = not (args.positions or args.history or args.pnl or
                    args.spot or args.futures or args.orders or args.cancel)

    db = TradeDB()

    if args.cancel:
        exchange = get_exchange(futures=True)
        cancel_all_orders(exchange, args.cancel)
        spot_exchange = get_exchange(futures=False)
        cancel_all_orders(spot_exchange, args.cancel)
        db.close()
        return

    if show_all or args.positions:
        show_db_positions(db)

    if show_all or args.pnl:
        show_daily_pnl(db)

    if args.history:
        show_history(db)

    if show_all or args.spot:
        spot = get_exchange(futures=False)
        show_spot_balances(spot)

    if show_all or args.futures:
        futures = get_exchange(futures=True)
        show_futures_positions(futures)
        show_futures_balance(futures)

    if show_all or args.orders:
        spot = get_exchange(futures=False)
        show_open_orders(spot, args.symbol)
        futures = get_exchange(futures=True)
        show_open_orders(futures, args.symbol)

    db.close()


if __name__ == "__main__":
    main()
