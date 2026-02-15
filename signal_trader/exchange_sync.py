"""Exchange trade sync — fetches recent trades from exchange and records ones not in DB."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

import ccxt

from core.database import (
    db_get_known_exchange_order_ids,
    db_get_sync_state,
    db_insert_synced_trade,
    db_set_sync_state,
)

logger = logging.getLogger("exchange_sync")

SYNC_COOLDOWN_SEC = 300  # 5 minutes between syncs


def _create_exchange(config, futures=False, exchange_name="binance"):
    """Create a ccxt exchange instance (mirrors TraderModule._create_exchange)."""
    if exchange_name == "okx":
        cfg = {
            "apiKey": config.okx_api_key,
            "secret": config.okx_secret_key,
            "password": config.okx_passphrase,
            "enableRateLimit": True,
            "hostname": "www.okx.cab",
        }
        if futures:
            cfg["options"] = {"defaultType": "swap"}
        exchange = ccxt.okx(cfg)
        exchange.load_markets()
    else:
        cfg = {
            "apiKey": config.binance_api_key,
            "secret": config.binance_secret_key,
            "enableRateLimit": True,
        }
        if futures:
            cfg["options"] = {"defaultType": "future"}
        exchange = ccxt.binance(cfg)
        exchange.load_markets()
    return exchange


def _discover_futures_symbols(exchange, exchange_name, since_ms):
    """Find symbols with recent futures activity."""
    symbols = set()

    # 1. Current open positions
    try:
        positions = exchange.fetch_positions()
        for p in positions:
            if abs(float(p.get("contracts", 0))) > 0:
                symbols.add(p["symbol"])
    except Exception as e:
        logger.debug(f"fetch_positions failed: {e}")

    # 2. Recent realized PnL (closed positions)
    if exchange_name == "binance":
        try:
            incomes = exchange.fapiPrivateGetIncome({
                "incomeType": "REALIZED_PNL",
                "startTime": since_ms,
                "limit": 1000,
            })
            for inc in incomes:
                raw_sym = inc["symbol"]  # e.g. "BTCUSDT"
                # Convert to ccxt format: BTC/USDT:USDT
                base = raw_sym.replace("USDT", "")
                ccxt_sym = f"{base}/USDT:USDT"
                if ccxt_sym in exchange.markets:
                    symbols.add(ccxt_sym)
        except Exception as e:
            logger.debug(f"fapiPrivateGetIncome failed: {e}")
    elif exchange_name == "okx":
        try:
            result = exchange.privateGetAccountPositionsHistory({
                "instType": "SWAP",
            })
            for p in result.get("data", []):
                inst_id = p.get("instId", "")  # e.g. "BTC-USDT-SWAP"
                base = inst_id.split("-")[0]
                ccxt_sym = f"{base}/USDT:USDT"
                if ccxt_sym in exchange.markets:
                    symbols.add(ccxt_sym)
        except Exception as e:
            logger.debug(f"OKX position history failed: {e}")

    return symbols


def _discover_spot_symbols(exchange, exchange_name):
    """Find symbols with non-zero spot balances."""
    symbols = set()
    try:
        balance = exchange.fetch_balance()
        skip_tokens = {"USDT", "BUSD", "USD", "USDC"}
        for token, amounts in balance.get("total", {}).items():
            if token in skip_tokens:
                continue
            if isinstance(amounts, (int, float)) and amounts > 0:
                ccxt_sym = f"{token}/USDT"
                if ccxt_sym in exchange.markets:
                    symbols.add(ccxt_sym)
    except Exception as e:
        logger.debug(f"fetch_balance failed: {e}")
    return symbols


def _group_fills_to_orders(fills):
    """Group individual fills by order ID into aggregated orders."""
    orders = defaultdict(lambda: {
        "fills": [], "total_qty": 0.0, "total_cost": 0.0,
        "total_realized_pnl": 0.0,
    })
    for fill in fills:
        oid = str(fill.get("order", fill["id"]))
        entry = orders[oid]
        entry["fills"].append(fill)
        qty = float(fill["amount"])
        price = float(fill["price"])
        entry["total_qty"] += qty
        entry["total_cost"] += qty * price

        # Realized PnL from exchange (futures)
        info = fill.get("info", {})
        rpnl = float(info.get("realizedPnl", 0) or info.get("pnl", 0) or 0)
        entry["total_realized_pnl"] += rpnl

    result = []
    for oid, data in orders.items():
        if data["total_qty"] <= 0:
            continue
        avg_price = data["total_cost"] / data["total_qty"]
        first = data["fills"][0]
        result.append({
            "order_id": oid,
            "symbol": first["symbol"],
            "side": first["side"],
            "avg_price": avg_price,
            "total_qty": data["total_qty"],
            "amount_usdt": data["total_cost"],
            "realized_pnl": data["total_realized_pnl"],
            "timestamp": first["timestamp"],
        })
    return result


def _sync_market(exchange, exchange_name, market_type, symbols, since_ms, known_ids):
    """Fetch trades for discovered symbols and insert new ones into DB."""
    synced = 0

    for symbol in symbols:
        try:
            fills = exchange.fetch_my_trades(symbol, since=since_ms, limit=500)
        except Exception as e:
            logger.debug(f"fetch_my_trades({symbol}) failed: {e}")
            continue

        if not fills:
            continue

        grouped = _group_fills_to_orders(fills)

        for order in grouped:
            oid = order["order_id"]
            if oid in known_ids:
                continue

            # Determine ticker and side
            sym_parts = order["symbol"].split("/")
            ticker = sym_parts[0]  # e.g. "BTC"
            raw_side = order["side"]  # "buy" or "sell"

            if market_type == "futures":
                # For futures, determine LONG/SHORT from positionSide or trade side
                first_info = order.get("fills", [{}])[0] if "fills" in order else {}
                # fallback: buy = opening LONG or closing SHORT, sell = opening SHORT or closing LONG
                side = "LONG" if raw_side == "buy" else "SHORT"
            else:
                side = "LONG" if raw_side == "buy" else "SHORT"

            # PnL: only meaningful for futures closing fills
            rpnl = order["realized_pnl"]
            pnl_usdt = round(rpnl, 2) if abs(rpnl) > 0.001 else None
            pnl_pct = None
            if pnl_usdt and order["amount_usdt"] > 0:
                pnl_pct = round(rpnl / order["amount_usdt"] * 100, 2)

            ts = order["timestamp"]
            created_at = datetime.fromtimestamp(ts / 1000).isoformat() if ts else datetime.now().isoformat()

            # Determine status: if there's realized PnL, treat as closed
            if pnl_usdt is not None:
                status = "closed"
                result_val = "exchange_sync"
                closed_at = created_at
                exit_price = order["avg_price"]
            else:
                status = "closed"
                result_val = "exchange_sync"
                closed_at = created_at
                exit_price = None

            db_insert_synced_trade(
                ticker=ticker,
                side=side,
                status=status,
                filled_price=order["avg_price"],
                qty=order["total_qty"],
                amount_usdt=round(order["amount_usdt"], 2),
                exit_price=exit_price,
                pnl_pct=pnl_pct,
                pnl_usdt=pnl_usdt,
                exchange_order_id=oid,
                exchange_name=exchange_name,
                created_at=created_at,
                closed_at=closed_at,
                result=result_val,
            )
            known_ids.add(oid)
            synced += 1
            logger.info(f"[SYNC] {ticker} {side} {order['total_qty']} @ {order['avg_price']:.4f} "
                        f"(PnL: {pnl_usdt or 'N/A'}) [{exchange_name}/{market_type}]")

    return synced


async def sync_exchange_trades(config, exchange_name=None, force=False):
    """Main sync entry point. Fetches recent exchange trades and inserts new ones into DB.

    Called automatically when the bot executes a trade, or manually from the dashboard.
    Set force=True to bypass the cooldown (for manual trigger).
    """
    # Determine which exchanges to sync
    exchanges_to_sync = []
    if exchange_name:
        exchanges_to_sync.append(exchange_name)
    else:
        if config.binance_api_key and config.binance_secret_key:
            exchanges_to_sync.append("binance")
        if config.okx_api_key and config.okx_secret_key and config.okx_passphrase:
            exchanges_to_sync.append("okx")

    if not exchanges_to_sync:
        return 0

    # Cooldown check (skip if forced)
    if not force:
        last_run_str = db_get_sync_state("last_sync_run")
        if last_run_str:
            try:
                elapsed = (datetime.now() - datetime.fromisoformat(last_run_str)).total_seconds()
                if elapsed < SYNC_COOLDOWN_SEC:
                    logger.debug(f"Sync skipped (cooldown: {SYNC_COOLDOWN_SEC - elapsed:.0f}s remaining)")
                    return 0
            except ValueError:
                pass

    db_set_sync_state("last_sync_run", datetime.now().isoformat())
    total_synced = 0

    for ex_name in exchanges_to_sync:
        known_ids = db_get_known_exchange_order_ids(ex_name)

        # Load per-exchange sync cursor (default: 7 days ago)
        default_since = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)

        # --- Futures ---
        futures_key = f"last_sync_{ex_name}_futures"
        futures_since_str = db_get_sync_state(futures_key)
        futures_since = int(datetime.fromisoformat(futures_since_str).timestamp() * 1000) if futures_since_str else default_since

        has_credentials = (
            (ex_name == "binance" and config.binance_api_key) or
            (ex_name == "okx" and config.okx_api_key)
        )
        if has_credentials:
            try:
                exchange_futures = _create_exchange(config, futures=True, exchange_name=ex_name)
                futures_symbols = _discover_futures_symbols(exchange_futures, ex_name, futures_since)
                if futures_symbols:
                    count = _sync_market(exchange_futures, ex_name, "futures", futures_symbols, futures_since, known_ids)
                    total_synced += count
                    logger.info(f"[SYNC] {ex_name} futures: {count} new trades from {len(futures_symbols)} symbols")
                db_set_sync_state(futures_key, datetime.now().isoformat())
            except Exception as e:
                logger.warning(f"[SYNC] {ex_name} futures sync failed: {e}")

            # --- Spot ---
            spot_key = f"last_sync_{ex_name}_spot"
            spot_since_str = db_get_sync_state(spot_key)
            spot_since = int(datetime.fromisoformat(spot_since_str).timestamp() * 1000) if spot_since_str else default_since

            try:
                exchange_spot = _create_exchange(config, futures=False, exchange_name=ex_name)
                spot_symbols = _discover_spot_symbols(exchange_spot, ex_name)
                if spot_symbols:
                    count = _sync_market(exchange_spot, ex_name, "spot", spot_symbols, spot_since, known_ids)
                    total_synced += count
                    logger.info(f"[SYNC] {ex_name} spot: {count} new trades from {len(spot_symbols)} symbols")
                db_set_sync_state(spot_key, datetime.now().isoformat())
            except Exception as e:
                logger.warning(f"[SYNC] {ex_name} spot sync failed: {e}")

    if total_synced > 0:
        logger.info(f"[SYNC] Total synced: {total_synced} new trades")

    return total_synced
