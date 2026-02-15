#!/usr/bin/env python3
"""
Position Watcher - Real-time price monitoring and active position management.

Runs as a persistent daemon using the unified TGForwarder database:
- WebSocket price feeds for Binance (spot + futures streams)
- Polling-based reconciliation for OKX positions
- TP1 hit detection -> move SL to breakeven
- Order status reconciliation (SL/TP fill detection)
- Daily PnL tracking and loss limit enforcement (configurable via dashboard)
- External position close detection (futures)

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
from datetime import datetime

# Ensure sibling imports work regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import websockets

from shared_settings import (
    init_openclaw, create_async_exchange, make_symbol,
)
from core.database import (
    db_get_active_openclaw_trades, db_get_active_trades_by_symbol,
    db_update_trade, db_get_today_pnl, db_load_settings, db_get_trade,
)

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

BINANCE_SPOT_WS = "wss://stream.binance.com:9443/stream"
BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"

RECONCILE_INTERVAL = 30   # Check order statuses every 30s
SYMBOL_REFRESH = 300      # Refresh WebSocket symbol list every 5min
WS_RECONNECT_DELAY = 5    # Seconds before reconnection attempt


class Watcher:
    def __init__(self):
        self.settings = {}
        self.config = None
        self.prices = {}       # "BTCUSDT" -> latest price
        self.running = True

        # Async exchange instances keyed by (exchange_name, is_futures)
        # e.g. ("binance", True) -> ccxt_async.binance futures instance
        self._exchanges = {}

    async def _init(self):
        """Load shared settings/config and create exchange instances as needed."""
        self.settings, self.config = init_openclaw()
        log.info(f"Settings loaded: DAILY_LOSS_LIMIT={self.settings['DAILY_LOSS_LIMIT']}")

        # Pre-create exchange instances for active trades
        trades = db_get_active_openclaw_trades()
        needed = set()
        for t in trades:
            is_futures = t["market_type"] == "futures"
            needed.add((t["exchange_name"] or "binance", is_futures))

        for exchange_name, is_futures in needed:
            await self._get_exchange(exchange_name, is_futures)

    async def _get_exchange(self, exchange_name, futures=False):
        """Get or create an async exchange instance."""
        key = (exchange_name, futures)
        if key not in self._exchanges:
            try:
                exc = await create_async_exchange(self.config, exchange_name, futures)
                self._exchanges[key] = exc
                log.info(f"Exchange created: {exchange_name} (futures={futures})")
            except Exception as e:
                log.error(f"Failed to create exchange {exchange_name} (futures={futures}): {e}")
                return None
        return self._exchanges[key]

    def _exchange_for_trade(self, trade):
        """Return the cached exchange instance for a trade (synchronous lookup)."""
        exchange_name = trade.get("exchange_name") or "binance"
        is_futures = trade["market_type"] == "futures"
        return self._exchanges.get((exchange_name, is_futures))

    async def run(self):
        await self._init()
        log.info("Watcher started. Monitoring active openclaw trades.")
        try:
            await asyncio.gather(
                self._ws_manager("spot"),
                self._ws_manager("futures"),
                self._reconcile_loop(),
            )
        finally:
            for exc in self._exchanges.values():
                try:
                    await exc.close()
                except Exception:
                    pass
            log.info("Watcher stopped.")

    # ==============================
    # Symbol Helpers
    # ==============================

    @staticmethod
    def _ccxt_symbol(trade):
        """Reconstruct ccxt symbol from a trade row."""
        return make_symbol(
            trade["ticker"],
            futures=(trade["market_type"] == "futures"),
            exchange_name=trade.get("exchange_name") or "binance",
        )

    @staticmethod
    def _raw_symbol(trade):
        """Get raw exchange symbol (e.g. 'BTCUSDT') for Binance WebSocket streams."""
        return trade["ticker"] + "USDT"

    @staticmethod
    def _raw_to_ticker(raw):
        """Convert 'BTCUSDT' -> 'BTC'."""
        if raw.endswith("USDT"):
            return raw[:-4]
        return raw

    # ==============================
    # WebSocket Price Feeds (Binance only)
    # ==============================

    async def _ws_manager(self, market_type):
        """Manages Binance WebSocket connection with auto-reconnection and symbol refresh.

        OKX trades are monitored via the reconcile loop (polling) only.
        """
        ws_base = BINANCE_FUTURES_WS if market_type == "futures" else BINANCE_SPOT_WS

        while self.running:
            symbols = self._watched_binance_symbols(market_type)
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
                            new_symbols = self._watched_binance_symbols(market_type)
                            if set(new_symbols) != set(symbols):
                                log.info(f"[WS:{market_type}] Symbols changed. Reconnecting.")
                                break
                            start = asyncio.get_event_loop().time()

            except websockets.ConnectionClosed:
                log.warning(f"[WS:{market_type}] Connection closed. Reconnecting...")
            except Exception as e:
                log.warning(f"[WS:{market_type}] Error: {e}. Reconnecting in {WS_RECONNECT_DELAY}s...")

            await asyncio.sleep(WS_RECONNECT_DELAY)

    def _watched_binance_symbols(self, market_type):
        """Get raw symbols (e.g. 'BTCUSDT') for active Binance trades of given market type."""
        trades = db_get_active_openclaw_trades()
        symbols = set()
        for t in trades:
            exchange_name = t.get("exchange_name") or "binance"
            if exchange_name != "binance":
                continue
            if t["market_type"] == market_type and t["status"] == "open":
                symbols.add(self._raw_symbol(t))
        return sorted(symbols)

    async def _on_price(self, raw_symbol, price, market_type):
        """Process price update from Binance WS. Check TP1 -> breakeven condition."""
        ticker = self._raw_to_ticker(raw_symbol)
        trades = db_get_active_trades_by_symbol(ticker, source="openclaw")

        for trade in trades:
            exchange_name = trade.get("exchange_name") or "binance"
            if exchange_name != "binance":
                continue
            if trade["market_type"] != market_type or trade["status"] != "open":
                continue

            # TP1 reached -> move SL to breakeven
            if not trade.get("sl_moved") and trade.get("tp1"):
                hit = False
                side = trade["side"].upper()
                if side == "LONG" and price >= trade["tp1"]:
                    hit = True
                elif side == "SHORT" and price <= trade["tp1"]:
                    hit = True

                if hit:
                    await self._move_sl_breakeven(trade)

    # ==============================
    # SL Management
    # ==============================

    async def _move_sl_breakeven(self, trade):
        """Cancel current SL, place new SL at entry price (breakeven)."""
        exchange_name = trade.get("exchange_name") or "binance"
        is_futures = trade["market_type"] == "futures"
        exchange = await self._get_exchange(exchange_name, is_futures)
        if not exchange:
            log.error(f"[{trade['ticker']}] No exchange for SL breakeven move")
            return

        symbol = self._ccxt_symbol(trade)
        entry = trade["filled_price"] or trade["entry_price"]
        qty = trade.get("remaining_qty") or trade["qty"]
        side = trade["side"].upper()

        log.info(f"[{symbol}] TP1 hit. Moving SL -> breakeven ({entry})")

        try:
            # Cancel old SL
            if trade.get("sl_order_id"):
                try:
                    await self._cancel_order_async(exchange, exchange_name,
                                                   trade["sl_order_id"], symbol)
                    log.info(f"[{symbol}] Old SL canceled: {trade['sl_order_id']}")
                except Exception as e:
                    log.warning(f"[{symbol}] Cancel old SL failed (may already be triggered): {e}")

            # Place new SL at breakeven
            new_sl = await self._create_sl_order_async(
                exchange, exchange_name, symbol, side, qty, entry, is_futures
            )

            db_update_trade(trade["id"],
                            sl=entry,
                            sl_order_id=new_sl["id"],
                            sl_moved=1)
            log.info(f"[{symbol}] SL moved to breakeven: {new_sl['id']} @ {entry}")

        except Exception as e:
            log.error(f"[{symbol}] Failed to move SL to breakeven: {e}")

    # ==============================
    # Async Order Helpers
    # ==============================

    @staticmethod
    async def _cancel_order_async(exchange, exchange_name, order_id, symbol):
        """Cancel an order, handling OKX trigger order params."""
        if exchange_name == "okx":
            await exchange.cancel_order(order_id, symbol, params={"stop": True})
        else:
            await exchange.cancel_order(order_id, symbol)

    @staticmethod
    async def _fetch_order_async(exchange, exchange_name, order_id, symbol):
        """Fetch an order, handling OKX trigger order params."""
        if exchange_name == "okx":
            return await exchange.fetch_order(order_id, symbol, params={"stop": True})
        return await exchange.fetch_order(order_id, symbol)

    @staticmethod
    async def _create_sl_order_async(exchange, exchange_name, symbol, side, qty, price, futures):
        """Place a stop-loss order on the exchange (async)."""
        close_side = "sell" if side == "LONG" else "buy"
        if exchange_name == "okx":
            params = {"triggerPrice": str(price), "triggerType": "last"}
            return await exchange.create_order(symbol, "trigger", close_side, qty, price, params)
        else:
            if futures:
                return await exchange.create_order(
                    symbol, "stop_market", close_side, qty, None,
                    {"stopPrice": price, "reduceOnly": True},
                )
            elif side == "LONG":
                return await exchange.create_order(
                    symbol, "stop_loss_limit", close_side, qty, price,
                    {"stopPrice": price},
                )
            else:
                return await exchange.create_order(
                    symbol, "stop_market", close_side, qty, None,
                    {"stopPrice": price, "reduceOnly": True},
                )

    # ==============================
    # Order Reconciliation
    # ==============================

    async def _reconcile_loop(self):
        """Periodically check order statuses for all active openclaw trades.

        This covers both Binance and OKX trades.
        """
        while self.running:
            await asyncio.sleep(RECONCILE_INTERVAL)
            if not self.running:
                return
            try:
                # Reload settings periodically (DAILY_LOSS_LIMIT may have changed)
                self.settings["DAILY_LOSS_LIMIT"] = float(
                    db_load_settings().get("DAILY_LOSS_LIMIT",
                                           str(self.config.daily_loss_limit))
                )
                await self._reconcile()
            except Exception as e:
                log.error(f"Reconcile error: {e}")

    async def _reconcile(self):
        trades = db_get_active_openclaw_trades()
        for trade in trades:
            if trade["status"] != "open":
                continue

            # Ensure exchange instance exists
            exchange_name = trade.get("exchange_name") or "binance"
            is_futures = trade["market_type"] == "futures"
            exchange = await self._get_exchange(exchange_name, is_futures)
            if not exchange:
                continue

            await self._check_active(trade, exchange, exchange_name)

    async def _check_active(self, trade, exchange, exchange_name):
        """Check SL and TP order statuses for an active trade."""
        symbol = self._ccxt_symbol(trade)

        # Check TP order
        if trade.get("tp_order_id"):
            try:
                tp = await self._fetch_order_async(exchange, exchange_name,
                                                   trade["tp_order_id"], symbol)
                if tp["status"] == "closed":
                    await self._on_tp_filled(trade, tp, exchange, exchange_name)
                    return
            except Exception as e:
                log.debug(f"[{symbol}] TP check: {e}")

        # Check SL order
        if trade.get("sl_order_id"):
            try:
                sl = await self._fetch_order_async(exchange, exchange_name,
                                                   trade["sl_order_id"], symbol)
                if sl["status"] == "closed":
                    await self._on_sl_filled(trade, sl, exchange, exchange_name)
                    return
            except Exception as e:
                log.debug(f"[{symbol}] SL check: {e}")

        # Futures: verify position still exists on exchange
        if trade["market_type"] == "futures":
            try:
                positions = await exchange.fetch_positions([symbol])
                active = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
                if not active:
                    await self._on_external_close(trade, exchange, exchange_name)
            except Exception as e:
                log.debug(f"[{symbol}] Position check: {e}")

        # OKX + non-futures: also check TP1 via last price (polling path)
        if exchange_name == "okx" and not trade.get("sl_moved") and trade.get("tp1"):
            try:
                ticker_data = await exchange.fetch_ticker(symbol)
                last_price = ticker_data.get("last", 0)
                if last_price and last_price > 0:
                    side = trade["side"].upper()
                    hit = False
                    if side == "LONG" and last_price >= trade["tp1"]:
                        hit = True
                    elif side == "SHORT" and last_price <= trade["tp1"]:
                        hit = True
                    if hit:
                        await self._move_sl_breakeven(trade)
            except Exception as e:
                log.debug(f"[{symbol}] OKX TP1 price check: {e}")

    async def _on_tp_filled(self, trade, tp_order, exchange, exchange_name):
        """Handle take-profit order filled."""
        symbol = self._ccxt_symbol(trade)
        fill_price = tp_order.get("average") or tp_order.get("price", 0)
        entry = trade["filled_price"] or trade["entry_price"]
        qty = trade.get("remaining_qty") or trade["qty"]
        side = trade["side"].upper()

        if side == "LONG":
            pnl_usdt = (fill_price - entry) * qty
        else:
            pnl_usdt = (entry - fill_price) * qty

        pnl_pct = (pnl_usdt / (entry * qty) * 100) if entry and qty else 0

        log.info(f"[{symbol}] TP HIT @ {fill_price} | PnL: {pnl_usdt:+.4f} USDT ({pnl_pct:+.2f}%)")

        # Cancel SL
        if trade.get("sl_order_id"):
            try:
                await self._cancel_order_async(exchange, exchange_name,
                                               trade["sl_order_id"], symbol)
            except Exception:
                pass

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_update_trade(trade["id"],
                        status="closed",
                        result="tp_hit",
                        exit_price=fill_price,
                        pnl_pct=round(pnl_pct, 2),
                        pnl_usdt=round(pnl_usdt, 4),
                        closed_at=now)

        self._check_daily_limit()

    async def _on_sl_filled(self, trade, sl_order, exchange, exchange_name):
        """Handle stop-loss order filled."""
        symbol = self._ccxt_symbol(trade)
        fill_price = sl_order.get("average") or sl_order.get("price") or trade["sl"]
        entry = trade["filled_price"] or trade["entry_price"]
        qty = trade.get("remaining_qty") or trade["qty"]
        side = trade["side"].upper()

        if side == "LONG":
            pnl_usdt = (fill_price - entry) * qty
        else:
            pnl_usdt = (entry - fill_price) * qty

        pnl_pct = (pnl_usdt / (entry * qty) * 100) if entry and qty else 0

        log.info(f"[{symbol}] SL HIT @ {fill_price} | PnL: {pnl_usdt:+.4f} USDT ({pnl_pct:+.2f}%)")

        # Cancel TP
        if trade.get("tp_order_id"):
            try:
                await self._cancel_order_async(exchange, exchange_name,
                                               trade["tp_order_id"], symbol)
            except Exception:
                pass

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_update_trade(trade["id"],
                        status="closed",
                        result="sl_hit",
                        exit_price=fill_price,
                        pnl_pct=round(pnl_pct, 2),
                        pnl_usdt=round(pnl_usdt, 4),
                        closed_at=now)

        self._check_daily_limit()

    async def _on_external_close(self, trade, exchange, exchange_name):
        """Handle position closed externally (manual, liquidation)."""
        symbol = self._ccxt_symbol(trade)
        log.info(f"[{symbol}] Position closed externally. Cleaning up orders.")

        for oid in [trade.get("sl_order_id"), trade.get("tp_order_id")]:
            if oid:
                try:
                    await self._cancel_order_async(exchange, exchange_name, oid, symbol)
                except Exception:
                    pass

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_update_trade(trade["id"],
                        status="closed",
                        result="external",
                        closed_at=now)

    def _check_daily_limit(self):
        """Check daily loss limit using unified db_get_today_pnl (all sources)."""
        today_pnl = db_get_today_pnl()
        limit = self.settings.get("DAILY_LOSS_LIMIT", 500)
        if today_pnl <= -limit:
            log.warning(f"=== DAILY LOSS LIMIT HIT: {today_pnl:.2f} USDT (limit: -{limit}) ===")


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
