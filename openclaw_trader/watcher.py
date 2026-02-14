#!/usr/bin/env python3
"""
Position Watcher - Real-time price monitoring and active position management.

Runs as a persistent daemon:
- WebSocket price feeds for all active position symbols
- TP1 hit detection -> move SL to breakeven
- Order status reconciliation (SL/TP fill detection)
- Daily PnL tracking and loss limit enforcement

Usage:
    python3 watcher.py          # Foreground
    nohup python3 watcher.py &  # Background daemon
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

# Ensure sibling imports work regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import websockets
import ccxt.async_support as ccxt_async

from db import TradeDB

# --- Logging ---

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watcher.log")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH),
    ],
)
log = logging.getLogger("watcher")

# --- Constants ---

SPOT_WS = "wss://stream.binance.com:9443/stream"
FUTURES_WS = "wss://fstream.binance.com/stream"

RECONCILE_INTERVAL = 30   # Check order statuses every 30s
SYMBOL_REFRESH = 300      # Refresh WebSocket symbol list every 5min
WS_RECONNECT_DELAY = 5    # Seconds before reconnection attempt


class Watcher:
    def __init__(self):
        self.db = TradeDB()
        self.prices = {}       # "BTCUSDT" -> latest price
        self.running = True
        self._spot = None
        self._futures = None

    async def _init_exchanges(self):
        api_key = os.environ.get("BINANCE_API_KEY")
        secret = os.environ.get("BINANCE_SECRET_KEY")
        if not api_key or not secret:
            log.error("BINANCE_API_KEY and BINANCE_SECRET_KEY must be set")
            sys.exit(1)

        self._spot = ccxt_async.binance({
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
        })
        self._futures = ccxt_async.binance({
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        })
        await self._spot.load_markets()
        await self._futures.load_markets()

    def _exchange(self, market_type):
        return self._futures if market_type == "futures" else self._spot

    async def run(self):
        await self._init_exchanges()
        log.info("Watcher started. Monitoring active positions.")
        try:
            await asyncio.gather(
                self._ws_manager("spot"),
                self._ws_manager("futures"),
                self._reconcile_loop(),
            )
        finally:
            if self._spot:
                await self._spot.close()
            if self._futures:
                await self._futures.close()
            self.db.close()
            log.info("Watcher stopped.")

    # ==============================
    # WebSocket Price Feeds
    # ==============================

    async def _ws_manager(self, market_type):
        """Manages WebSocket connection with auto-reconnection and symbol refresh."""
        ws_base = FUTURES_WS if market_type == "futures" else SPOT_WS

        while self.running:
            symbols = self._watched_symbols(market_type)
            if not symbols:
                await asyncio.sleep(10)
                continue

            streams = "/".join(f"{s.lower()}@miniTicker" for s in symbols)
            url = f"{ws_base}?streams={streams}"

            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    log.info(f"[WS:{market_type}] Connected. Symbols: {sorted(symbols)}")
                    start = asyncio.get_event_loop().time()

                    async for msg in ws:
                        if not self.running:
                            return

                        data = json.loads(msg)
                        ticker = data.get("data", {})
                        raw = ticker.get("s", "")
                        price = float(ticker.get("c", 0))

                        if raw and price > 0:
                            self.prices[raw] = price
                            await self._on_price(raw, price, market_type)

                        # Periodically check if symbol list changed
                        elapsed = asyncio.get_event_loop().time() - start
                        if elapsed > SYMBOL_REFRESH:
                            new_symbols = self._watched_symbols(market_type)
                            if set(new_symbols) != set(symbols):
                                log.info(f"[WS:{market_type}] Symbols changed. Reconnecting.")
                                break
                            start = asyncio.get_event_loop().time()

            except websockets.ConnectionClosed:
                log.warning(f"[WS:{market_type}] Connection closed. Reconnecting...")
            except Exception as e:
                log.warning(f"[WS:{market_type}] Error: {e}. Reconnecting in {WS_RECONNECT_DELAY}s...")

            await asyncio.sleep(WS_RECONNECT_DELAY)

    def _watched_symbols(self, market_type):
        """Get raw symbols (e.g., 'BTCUSDT') for active positions of given market type."""
        positions = self.db.get_active_positions()
        symbols = set()
        for p in positions:
            if p["market_type"] == market_type and p["status"] == "active":
                symbols.add(p["symbol"].replace("/", ""))
        return sorted(symbols)

    async def _on_price(self, raw_symbol, price, market_type):
        """Process price update. Check TP1 -> breakeven condition."""
        ccxt_sym = self._raw_to_ccxt(raw_symbol)
        positions = self.db.get_active_positions_by_symbol(ccxt_sym)

        for pos in positions:
            if pos["market_type"] != market_type or pos["status"] != "active":
                continue

            # TP1 reached -> move SL to breakeven
            if not pos["sl_moved_to_breakeven"] and pos.get("tp1_price"):
                hit = False
                if pos["side"] == "long" and price >= pos["tp1_price"]:
                    hit = True
                elif pos["side"] == "short" and price <= pos["tp1_price"]:
                    hit = True

                if hit:
                    await self._move_sl_breakeven(pos)

    @staticmethod
    def _raw_to_ccxt(raw):
        """Convert 'BTCUSDT' -> 'BTC/USDT'."""
        if raw.endswith("USDT"):
            return f"{raw[:-4]}/USDT"
        return raw

    # ==============================
    # SL Management
    # ==============================

    async def _move_sl_breakeven(self, pos):
        """Cancel current SL, place new SL at entry price (breakeven)."""
        exchange = self._exchange(pos["market_type"])
        symbol = pos["symbol"]
        entry = pos["filled_price"] or pos["entry_price"]
        qty = pos["remaining_qty"] or pos["quantity"]

        log.info(f"[{symbol}] TP1 hit. Moving SL -> breakeven ({entry})")

        try:
            # Cancel old SL
            if pos["sl_order_id"]:
                try:
                    await exchange.cancel_order(pos["sl_order_id"], symbol)
                    log.info(f"[{symbol}] Old SL canceled: {pos['sl_order_id']}")
                except Exception as e:
                    log.warning(f"[{symbol}] Cancel old SL failed (may already be triggered): {e}")

            # Place new SL at breakeven
            if pos["market_type"] == "futures":
                sl_side = "buy" if pos["side"] == "short" else "sell"
                new_sl = await exchange.create_order(
                    symbol, "stop_market", sl_side, qty, None,
                    {"stopPrice": entry, "reduceOnly": True},
                )
            else:
                sl_side = "sell" if pos["side"] == "long" else "buy"
                new_sl = await exchange.create_order(
                    symbol, "stop_loss_limit", sl_side, qty, entry,
                    {"stopPrice": entry},
                )

            self.db.update_position(pos["id"], {
                "sl_price": entry,
                "sl_order_id": new_sl["id"],
                "sl_moved_to_breakeven": 1,
            })
            log.info(f"[{symbol}] SL moved to breakeven: {new_sl['id']} @ {entry}")

        except Exception as e:
            log.error(f"[{symbol}] Failed to move SL to breakeven: {e}")

    # ==============================
    # Order Reconciliation
    # ==============================

    async def _reconcile_loop(self):
        """Periodically check order statuses for all active positions."""
        while self.running:
            await asyncio.sleep(RECONCILE_INTERVAL)
            if not self.running:
                return
            try:
                await self._reconcile()
            except Exception as e:
                log.error(f"Reconcile error: {e}")

    async def _reconcile(self):
        positions = self.db.get_active_positions()
        for pos in positions:
            if pos["status"] != "active":
                continue
            await self._check_active(pos)

    async def _check_active(self, pos):
        """Check SL and TP order statuses for an active position."""
        exchange = self._exchange(pos["market_type"])
        symbol = pos["symbol"]

        # Check TP order
        if pos.get("tp_order_id"):
            try:
                tp = await exchange.fetch_order(pos["tp_order_id"], symbol)
                if tp["status"] == "closed":
                    await self._on_tp_filled(pos, tp)
                    return
            except Exception as e:
                log.debug(f"[{symbol}] TP check: {e}")

        # Check SL order
        if pos.get("sl_order_id"):
            try:
                sl = await exchange.fetch_order(pos["sl_order_id"], symbol)
                if sl["status"] == "closed":
                    await self._on_sl_filled(pos, sl)
                    return
            except Exception as e:
                log.debug(f"[{symbol}] SL check: {e}")

        # Futures: verify position still exists on exchange
        if pos["market_type"] == "futures":
            try:
                expos = await exchange.fetch_positions([symbol])
                active = [p for p in expos if abs(float(p.get("contracts", 0))) > 0]
                if not active:
                    await self._on_external_close(pos)
            except Exception as e:
                log.debug(f"[{symbol}] Position check: {e}")

    async def _on_tp_filled(self, pos, tp_order):
        """Handle take-profit order filled."""
        exchange = self._exchange(pos["market_type"])
        symbol = pos["symbol"]
        fill_price = tp_order.get("average") or tp_order.get("price", 0)
        entry = pos["filled_price"] or pos["entry_price"]
        qty = pos["remaining_qty"] or pos["quantity"]

        if pos["side"] == "long":
            pnl = (fill_price - entry) * qty
        else:
            pnl = (entry - fill_price) * qty

        log.info(f"[{symbol}] TP HIT @ {fill_price} | PnL: {pnl:+.4f} USDT")

        # Cancel SL
        if pos.get("sl_order_id"):
            try:
                await exchange.cancel_order(pos["sl_order_id"], symbol)
            except Exception:
                pass

        self.db.close_position(pos["id"], "tp_hit", pnl, fill_price)
        self.db.update_daily_pnl(pnl, is_win=(pnl > 0))
        self._check_daily_limit()

    async def _on_sl_filled(self, pos, sl_order):
        """Handle stop-loss order filled."""
        exchange = self._exchange(pos["market_type"])
        symbol = pos["symbol"]
        fill_price = sl_order.get("average") or sl_order.get("price") or pos["sl_price"]
        entry = pos["filled_price"] or pos["entry_price"]
        qty = pos["remaining_qty"] or pos["quantity"]

        if pos["side"] == "long":
            pnl = (fill_price - entry) * qty
        else:
            pnl = (entry - fill_price) * qty

        log.info(f"[{symbol}] SL HIT @ {fill_price} | PnL: {pnl:+.4f} USDT")

        # Cancel TP
        if pos.get("tp_order_id"):
            try:
                await exchange.cancel_order(pos["tp_order_id"], symbol)
            except Exception:
                pass

        self.db.close_position(pos["id"], "sl_hit", pnl, fill_price)
        self.db.update_daily_pnl(pnl, is_win=(pnl > 0))
        self._check_daily_limit()

    async def _on_external_close(self, pos):
        """Handle position closed externally (manual, liquidation)."""
        exchange = self._exchange(pos["market_type"])
        symbol = pos["symbol"]
        log.info(f"[{symbol}] Position closed externally. Cleaning up orders.")

        for oid in [pos.get("sl_order_id"), pos.get("tp_order_id")]:
            if oid:
                try:
                    await exchange.cancel_order(oid, symbol)
                except Exception:
                    pass

        self.db.close_position(pos["id"], "external", 0, None)

    def _check_daily_limit(self):
        daily = self.db.get_daily_pnl()
        if daily and daily["realized_pnl"] <= -500:
            if not daily.get("loss_limit_hit"):
                log.warning(f"=== DAILY LOSS LIMIT HIT: {daily['realized_pnl']:.2f} USDT ===")
                self.db.set_loss_limit_hit()


# ==============================
# Entry Point
# ==============================

def main():
    watcher = Watcher()

    def shutdown(sig, frame):
        log.info("Shutdown signal received.")
        watcher.running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.run(watcher.run())


if __name__ == "__main__":
    main()
