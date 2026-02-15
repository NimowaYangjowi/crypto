#!/usr/bin/env python3
"""
Position Monitor - Reports from the shared database and exchange.

Uses the unified trades table (source='openclaw') and shared settings.

Usage:
    python3 monitor.py                  # Full status (DB + exchange)
    python3 monitor.py --positions      # Active positions from DB
    python3 monitor.py --history        # Recent trade history
    python3 monitor.py --pnl            # Today's PnL
    python3 monitor.py --spot           # Spot balances (exchange)
    python3 monitor.py --futures        # Futures positions (exchange)
    python3 monitor.py --orders         # Open orders (exchange)
    python3 monitor.py --cancel SYMBOL  # Cancel all open orders for SYMBOL
    python3 monitor.py --exchange okx   # Use OKX instead of Binance
"""

import argparse
import sqlite3
import sys

from shared_settings import init_openclaw, create_exchange, make_symbol
from core.database import (
    db_get_active_openclaw_trades,
    db_get_today_pnl,
    db_load_settings,
    DB_PATH,
)


# ── DB helpers ────────────────────────────────────────────

def get_openclaw_history(limit=20):
    """Return recent trades from the shared DB filtered to source='openclaw'."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM trades WHERE source='openclaw' ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_openclaw_today_stats():
    """Return win/loss/count stats for today's openclaw trades."""
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """SELECT
                 COUNT(*) as trade_count,
                 COALESCE(SUM(pnl_usdt), 0) as pnl_usdt,
                 COUNT(CASE WHEN pnl_usdt > 0 THEN 1 END) as wins,
                 COUNT(CASE WHEN pnl_usdt < 0 THEN 1 END) as losses
               FROM trades
               WHERE source='openclaw' AND status='closed' AND closed_at LIKE ?""",
            (f"{today}%",),
        ).fetchone()
        return dict(row) if row else None


# ── DB-based views ────────────────────────────────────────

def show_db_positions():
    """Show active openclaw positions from the shared trades table."""
    print("\n=== ACTIVE POSITIONS (DB) ===")
    positions = db_get_active_openclaw_trades()
    if not positions:
        print("  No active positions.")
        return
    for p in positions:
        side = p["side"].upper()
        ticker = p["ticker"]
        entry = p["filled_price"] or p["entry_price"]
        sl = p["sl"]
        be = " [BE]" if p.get("sl_moved") else ""
        tp3 = p.get("tp3", "?")
        status = p["status"].upper()
        mtype = p.get("market_type", "spot")
        lev = p.get("leverage", 1)
        print(f"  [{p['id']}] {ticker} {side} ({mtype} x{lev}) | {status}")
        print(f"      Entry: {entry} | SL: {sl}{be} | TP3: {tp3}")
        print(f"      Qty: {p.get('qty', '-')} | USDT: {p.get('amount_usdt', '-')}")


def show_history(limit=20):
    """Show recent openclaw trade history from the shared DB."""
    print(f"\n=== TRADE HISTORY (last {limit}) ===")
    trades = get_openclaw_history(limit)
    if not trades:
        print("  No trade history.")
        return
    for t in trades:
        side = t["side"].upper()
        ticker = t["ticker"]
        result = t.get("result") or "-"
        pnl = t.get("pnl_usdt", 0)
        pnl_str = f"{pnl:+.4f}" if pnl else "0"
        status = t["status"].upper()
        print(f"  [{t['id']}] {ticker} {side} | {status} ({result}) | PnL: {pnl_str} USDT | {t['created_at']}")


def show_daily_pnl(settings):
    """Show today's PnL from the shared DB, with daily loss limit context."""
    print("\n=== TODAY'S PnL ===")

    # Overall PnL (all sources)
    overall_pnl = db_get_today_pnl()

    # OpenClaw-specific stats
    stats = get_openclaw_today_stats()

    if not stats or stats["trade_count"] == 0:
        print("  No openclaw trades closed today.")
        print(f"  Overall PnL (all sources): {overall_pnl:+.4f} USDT")
    else:
        print(f"  OpenClaw PnL: {stats['pnl_usdt']:+.4f} USDT")
        print(f"  Trades: {stats['trade_count']} ({stats['wins']}W / {stats['losses']}L)")
        if overall_pnl != stats["pnl_usdt"]:
            print(f"  Overall PnL (all sources): {overall_pnl:+.4f} USDT")

    loss_limit = settings.get("DAILY_LOSS_LIMIT", 0)
    print(f"  Daily Loss Limit: {loss_limit:.2f} USDT")
    if overall_pnl <= -loss_limit:
        print("  *** DAILY LOSS LIMIT HIT ***")


# ── Exchange-based views ──────────────────────────────────

def show_spot_balances(config, exchange_name):
    print(f"\n=== SPOT BALANCES ({exchange_name.upper()}) ===")
    exchange = create_exchange(config, exchange_name, futures=False)
    balance = exchange.fetch_balance()
    has_assets = False
    for currency, amount in balance["total"].items():
        if amount and amount > 0:
            free = balance["free"].get(currency, 0)
            used = balance["used"].get(currency, 0)
            print(f"  {currency}: {amount:.8f} (free: {free:.8f}, in orders: {used:.8f})")
            has_assets = True
    if not has_assets:
        print("  No assets found.")


def show_futures_positions(config, exchange_name):
    print(f"\n=== FUTURES POSITIONS ({exchange_name.upper()}) ===")
    exchange = create_exchange(config, exchange_name, futures=True)
    positions = exchange.fetch_positions()
    active = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
    if not active:
        print("  No active positions.")
        return exchange
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
        pnl_val = float(pnl) if pnl else 0
        print(f"    Unrealized PnL: {pnl_val:.4f} USDT | Margin: {margin}")
    return exchange


def show_futures_balance(exchange, exchange_name):
    print(f"\n=== FUTURES BALANCE ({exchange_name.upper()}) ===")
    balance = exchange.fetch_balance()
    usdt = balance.get("USDT", {})
    print(f"  Total: {usdt.get('total', 0):.4f} USDT")
    print(f"  Free: {usdt.get('free', 0):.4f} USDT")
    print(f"  Used: {usdt.get('used', 0):.4f} USDT")


def show_open_orders(exchange, exchange_name, symbol=None):
    label = f" ({symbol})" if symbol else ""
    print(f"\n=== OPEN ORDERS {label} ({exchange_name.upper()}) ===")
    if not symbol:
        exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
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


def cancel_all_orders(config, exchange_name, symbol):
    """Cancel all open orders for a symbol on both spot and futures."""
    ccxt_symbol = make_symbol(symbol.upper().replace("USDT", "").replace("/", ""),
                              futures=False, exchange_name=exchange_name)

    # Spot
    spot = create_exchange(config, exchange_name, futures=False)
    orders = spot.fetch_open_orders(ccxt_symbol)
    if orders:
        for o in orders:
            spot.cancel_order(o["id"], ccxt_symbol)
            print(f"[spot] Canceled: {o['id']} ({o['side']} {o['type']} @ {o.get('price', 'market')})")
        print(f"Canceled {len(orders)} spot orders for {ccxt_symbol}.")
    else:
        print(f"No open spot orders for {ccxt_symbol}.")

    # Futures
    futures_symbol = make_symbol(symbol.upper().replace("USDT", "").replace("/", ""),
                                  futures=True, exchange_name=exchange_name)
    futures = create_exchange(config, exchange_name, futures=True)
    orders = futures.fetch_open_orders(futures_symbol)
    if orders:
        for o in orders:
            futures.cancel_order(o["id"], futures_symbol)
            print(f"[futures] Canceled: {o['id']} ({o['side']} {o['type']} @ {o.get('price', 'market')})")
        print(f"Canceled {len(orders)} futures orders for {futures_symbol}.")
    else:
        print(f"No open futures orders for {futures_symbol}.")


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OpenClaw Position Monitor")
    parser.add_argument("--positions", action="store_true", help="Active positions from DB")
    parser.add_argument("--history", action="store_true", help="Trade history from DB")
    parser.add_argument("--pnl", action="store_true", help="Today's PnL")
    parser.add_argument("--spot", action="store_true", help="Spot balances (exchange)")
    parser.add_argument("--futures", action="store_true", help="Futures positions (exchange)")
    parser.add_argument("--orders", action="store_true", help="Open orders (exchange)")
    parser.add_argument("--cancel", type=str, help="Cancel all orders for SYMBOL")
    parser.add_argument("--symbol", type=str, help="Filter orders by symbol")
    parser.add_argument("--exchange", type=str, choices=["binance", "okx"],
                        default="binance", help="Exchange to query (default: binance)")
    args = parser.parse_args()

    show_all = not (args.positions or args.history or args.pnl or
                    args.spot or args.futures or args.orders or args.cancel)

    # Initialize shared DB and config
    settings, config = init_openclaw()
    exchange_name = args.exchange

    if args.cancel:
        cancel_all_orders(config, exchange_name, args.cancel)
        return

    if show_all or args.positions:
        show_db_positions()

    if show_all or args.pnl:
        show_daily_pnl(settings)

    if args.history:
        show_history()

    if show_all or args.spot:
        show_spot_balances(config, exchange_name)

    if show_all or args.futures:
        exc = show_futures_positions(config, exchange_name)
        show_futures_balance(exc, exchange_name)

    if show_all or args.orders:
        spot = create_exchange(config, exchange_name, futures=False)
        show_open_orders(spot, exchange_name, args.symbol)
        futures = create_exchange(config, exchange_name, futures=True)
        show_open_orders(futures, exchange_name, args.symbol)


if __name__ == "__main__":
    main()
