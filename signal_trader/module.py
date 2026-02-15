"""Signal Trader module — monitors Telegram signals and executes trades on Binance/OKX."""

import asyncio
import logging
import time
from datetime import datetime
from telethon import TelegramClient, events, utils as tl_utils
import ccxt
import httpx

from core.config import AppConfig
from core.database import (
    db_insert_trade, db_update_trade, db_get_trades, db_get_stats,
    db_get_today_pnl, db_load_settings, db_save_settings,
    db_get_channel_formats, db_get_performance_stats, db_get_performance_table,
)
from signal_trader.parser import (
    parse_signal, compile_template, parse_with_template,
    fill_signal_defaults,
)
from signal_trader.exchange_sync import sync_exchange_trades

logger = logging.getLogger("signal_trader")


class TraderModule:
    def __init__(self, client: TelegramClient, config: AppConfig):
        self.client = client
        self.config = config
        self.enabled = False

        # Mutable settings (can be changed at runtime)
        self.trade_amount = config.trade_amount
        self.sell_blocked = set(config.sell_blocked)
        self.trade_blocked = set(config.trade_blocked)
        self.max_concurrent = config.max_concurrent
        self.daily_loss_limit = config.daily_loss_limit
        self.entry_timeout = config.entry_timeout
        self.max_leverage = config.max_leverage

        # Runtime state
        self.active_trades = {}
        self.daily_realized_pnl = 0.0
        self._daily_reset_date = datetime.now().date()
        self._http_client = httpx.AsyncClient(timeout=10)
        self._channel_templates = {}  # chat_id -> {regex, fields, default_side}

    def apply_settings_from_db(self):
        saved = db_load_settings()
        if not saved:
            # First run: seed database with config defaults
            db_save_settings({
                "TRADE_AMOUNT": str(self.trade_amount),
                "SELL_BLOCKED": ",".join(sorted(self.sell_blocked)),
                "TRADE_BLOCKED": ",".join(sorted(self.trade_blocked)),
                "MAX_CONCURRENT": str(self.max_concurrent),
                "DAILY_LOSS_LIMIT": str(self.daily_loss_limit),
                "ENTRY_TIMEOUT": str(self.entry_timeout),
                "MAX_LEVERAGE": str(self.max_leverage),
            })
            logger.info("Settings seeded to database from config defaults")
        else:
            if "TRADE_AMOUNT" in saved:
                self.trade_amount = float(saved["TRADE_AMOUNT"])
            if "SELL_BLOCKED" in saved:
                self.sell_blocked = {s.strip().upper() for s in saved["SELL_BLOCKED"].split(",") if s.strip()}
            if "TRADE_BLOCKED" in saved:
                self.trade_blocked = {s.strip().upper() for s in saved["TRADE_BLOCKED"].split(",") if s.strip()}
            if "MAX_CONCURRENT" in saved:
                self.max_concurrent = int(saved["MAX_CONCURRENT"])
            if "DAILY_LOSS_LIMIT" in saved:
                self.daily_loss_limit = float(saved["DAILY_LOSS_LIMIT"])
            if "ENTRY_TIMEOUT" in saved:
                self.entry_timeout = int(saved["ENTRY_TIMEOUT"])
            if "MAX_LEVERAGE" in saved:
                self.max_leverage = int(saved["MAX_LEVERAGE"])
        logger.info(f"Settings loaded: TRADE_AMOUNT={self.trade_amount}, SELL_BLOCKED={self.sell_blocked}, "
                     f"TRADE_BLOCKED={self.trade_blocked}, MAX_CONCURRENT={self.max_concurrent}, "
                     f"DAILY_LOSS_LIMIT={self.daily_loss_limit}, ENTRY_TIMEOUT={self.entry_timeout}, "
                     f"MAX_LEVERAGE={self.max_leverage}")

    def _create_exchange(self, futures=False, exchange_name="binance"):
        if exchange_name == "okx":
            config = {
                "apiKey": self.config.okx_api_key,
                "secret": self.config.okx_secret_key,
                "password": self.config.okx_passphrase,
                "enableRateLimit": True,
                "hostname": "www.okx.cab",
            }
            if futures:
                config["options"] = {"defaultType": "swap"}
            exchange = ccxt.okx(config)
            exchange.load_markets()
            # OKX: set net position mode (avoids reduceOnly issues in hedged mode)
            if futures:
                try:
                    exchange.set_position_mode(False)
                except Exception as e:
                    logger.debug(f"OKX set_position_mode(net): {e}")
        else:
            config = {
                "apiKey": self.config.binance_api_key,
                "secret": self.config.binance_secret_key,
                "enableRateLimit": True,
            }
            if futures:
                config["options"] = {"defaultType": "future"}
            exchange = ccxt.binance(config)
            exchange.load_markets()
        return exchange

    def _make_symbol(self, ticker, futures=False, exchange_name="binance"):
        if exchange_name == "okx" and futures:
            return f"{ticker}/USDT:USDT"
        return f"{ticker}/USDT"

    # ── OKX-aware order helpers ───────────────────────────

    def _fetch_exit_order(self, exchange, exchange_name, order_id, symbol):
        """Fetch SL/TP order status.
        OKX trigger (algo) orders require params={'stop': True} to query
        the algo-order endpoint instead of the regular order endpoint.
        """
        if exchange_name == "okx":
            return exchange.fetch_order(order_id, symbol, params={"stop": True})
        return exchange.fetch_order(order_id, symbol)

    def _cancel_exit_order(self, exchange, exchange_name, order_id, symbol):
        """Cancel a single SL/TP order.
        OKX trigger (algo) orders require params={'stop': True}.
        """
        if exchange_name == "okx":
            exchange.cancel_order(order_id, symbol, params={"stop": True})
        else:
            exchange.cancel_order(order_id, symbol)

    def _cancel_exit_orders_safe(self, exchange, exchange_name, symbol, order_ids):
        """Cancel a list of SL/TP orders, ignoring errors (already filled/cancelled)."""
        for oid in order_ids:
            try:
                self._cancel_exit_order(exchange, exchange_name, oid, symbol)
            except Exception:
                pass

    def _create_sl_order(self, exchange, exchange_name, symbol, side, qty, price, futures=False):
        """Create a stop-loss order appropriate to the exchange."""
        close_side = "sell" if side == "LONG" else "buy"
        if exchange_name == "okx":
            params = {"triggerPrice": str(price), "triggerType": "last"}
            return exchange.create_order(symbol, "trigger", close_side, qty, price, params)
        else:
            if futures:
                return exchange.create_order(symbol, "stop_market", close_side, qty, None, {"stopPrice": price, "reduceOnly": True})
            elif side == "LONG":
                return exchange.create_order(symbol, "stop_loss_limit", close_side, qty, price, {"stopPrice": price})
            else:
                return exchange.create_order(symbol, "stop_market", close_side, qty, None, {"stopPrice": price, "reduceOnly": True})

    def _create_tp_order(self, exchange, exchange_name, symbol, side, qty, price, futures=False):
        """Create a take-profit order appropriate to the exchange."""
        close_side = "sell" if side == "LONG" else "buy"
        if exchange_name == "okx":
            params = {"triggerPrice": str(price), "triggerType": "last"}
            return exchange.create_order(symbol, "trigger", close_side, qty, price, params)
        else:
            if futures:
                return exchange.create_order(symbol, "take_profit_market", close_side, qty, None, {"stopPrice": price, "reduceOnly": True})
            elif side == "LONG":
                return exchange.create_limit_sell_order(symbol, qty, price)
            else:
                return exchange.create_order(symbol, "take_profit_market", close_side, qty, None, {"stopPrice": price, "reduceOnly": True})

    def _close_ghost_position(self, exchange, exchange_name, symbol, expected_side):
        """Detect and close unexpected positions created by trigger orders firing after external close."""
        try:
            positions = exchange.fetch_positions([symbol])
            for p in positions:
                contracts = abs(float(p.get("contracts", 0)))
                if contracts <= 0:
                    continue
                pos_side = p.get("side", "").lower()
                is_ghost = (
                    (expected_side == "LONG" and pos_side == "short") or
                    (expected_side == "SHORT" and pos_side == "long")
                )
                if is_ghost:
                    logger.warning(f"Ghost position detected: {symbol} {pos_side} {contracts}. Closing immediately.")
                    if pos_side == "long":
                        exchange.create_market_sell_order(symbol, contracts)
                    else:
                        exchange.create_market_buy_order(symbol, contracts)
                    logger.info(f"Ghost position closed: {symbol} {pos_side} {contracts}")
        except Exception as e:
            logger.error(f"Ghost position check failed for {symbol}: {e}")

    def _place_exit_orders(self, exchange, exchange_name, symbol, side, qty, sl_price, tp_price, futures=False):
        """Place SL + TP atomically. If either fails, cancel the other and raise."""
        sl_order = self._create_sl_order(exchange, exchange_name, symbol, side, qty, sl_price, futures)
        sl_id = sl_order["id"]
        try:
            tp_order = self._create_tp_order(exchange, exchange_name, symbol, side, qty, tp_price, futures)
        except Exception:
            try:
                self._cancel_exit_order(exchange, exchange_name, sl_id, symbol)
            except Exception:
                pass
            raise
        return sl_id, tp_order["id"]

    def _set_leverage_and_margin(self, exchange, exchange_name, leverage, symbol):
        """Set leverage and margin mode, handling OKX's combined API requirement."""
        try:
            if exchange_name == "okx":
                # OKX set_margin_mode requires lever param
                exchange.set_margin_mode("isolated", symbol, params={"lever": str(leverage)})
            else:
                exchange.set_leverage(leverage, symbol)
                exchange.set_margin_mode("isolated", symbol)
        except Exception as e:
            logger.warning(f"set_leverage/margin_mode ({exchange_name}): {e}")

    def _fetch_leverage(self, exchange, exchange_name, symbol, fallback=1):
        """Fetch the actual leverage set on the exchange for a symbol.
        Returns the real leverage so that margin = trade_amount regardless of
        whether _set_leverage_and_margin succeeded or not.
        """
        try:
            if exchange_name == "okx":
                market = exchange.market(symbol)
                inst_id = market["id"]  # e.g. "SOL-USDT-SWAP"
                result = exchange.privateGetAccountLeverageInfo({
                    "instId": inst_id,
                    "mgnMode": "isolated",
                })
                data = result.get("data", [])
                if data:
                    lever = int(float(data[0].get("lever", fallback)))
                    logger.info(f"Actual leverage on {exchange_name} for {symbol}: {lever}x")
                    return lever
            else:
                # Binance: fetch position risk to get leverage
                raw_symbol = symbol.replace("/", "").replace(":USDT", "")
                positions = exchange.fapiPrivateV2GetPositionRisk({"symbol": raw_symbol})
                if positions:
                    lever = int(float(positions[0].get("leverage", fallback)))
                    logger.info(f"Actual leverage on {exchange_name} for {symbol}: {lever}x")
                    return lever
        except Exception as e:
            logger.warning(f"Failed to fetch leverage for {symbol} ({exchange_name}): {e}")
        return fallback

    async def _emergency_close(self, exchange, symbol, side, filled_qty, avg_price, trade_id, ticker, reason, tag=""):
        """Market-close a position when SL/TP placement fails."""
        try:
            if side == "LONG":
                close_order = exchange.create_market_sell_order(symbol, filled_qty)
            else:
                close_order = exchange.create_market_buy_order(symbol, filled_qty)
            close_price = close_order.get("average") or close_order.get("price")
            if side == "LONG":
                pnl_pct = round((close_price - avg_price) / avg_price * 100, 2)
                pnl_usdt = round((close_price - avg_price) * filled_qty, 2)
            else:
                pnl_pct = round((avg_price - close_price) / avg_price * 100, 2)
                pnl_usdt = round((avg_price - close_price) * filled_qty, 2)
            self._record_pnl(pnl_usdt)
            db_update_trade(trade_id, status="closed", result="sl_tp_failed",
                            exit_price=close_price, pnl_pct=pnl_pct, pnl_usdt=pnl_usdt,
                            closed_at=datetime.now().isoformat())
            logger.error(f"[{side}] {symbol} SL/TP failed, emergency closed @ {close_price}: {reason}")
            await self._notify(
                f"{tag}⚠️ {ticker} {side} SL/TP 설정 실패 → 즉시 청산\n"
                f"청산가: {close_price} | PnL: {pnl_pct}%\n원인: {reason}"
            )
        except Exception as close_err:
            logger.error(f"[{side}] {symbol} CRITICAL: emergency close also failed: {close_err}")
            db_update_trade(trade_id, status="error",
                            result=f"sl_tp_failed+close_failed: {reason}",
                            closed_at=datetime.now().isoformat())
            await self._notify(
                f"{tag}🚨 {ticker} {side} SL/TP 실패 + 청산도 실패!\n"
                f"수동 확인 필요! 원인: {reason}"
            )

    async def _notify(self, message):
        if not self.config.bot_token or not self.config.my_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        try:
            resp = await self._http_client.post(url, json={"chat_id": self.config.my_chat_id, "text": message})
            if not resp.json().get("ok"):
                logger.error(f"Notify failed: {resp.text}")
        except Exception as e:
            logger.error(f"Failed to notify: {e}")

    async def _fetch_current_price(self, ticker, exchange_name="binance"):
        """Fetch current price from exchange public API."""
        if exchange_name == "okx":
            url = f"https://www.okx.cab/api/v5/market/ticker?instId={ticker}-USDT"
            resp = await self._http_client.get(url)
            data = resp.json()
            return float(data["data"][0]["last"])
        else:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={ticker}USDT"
            resp = await self._http_client.get(url)
            data = resp.json()
            return float(data['price'])

    def _check_daily_reset(self):
        today = datetime.now().date()
        if today != self._daily_reset_date:
            self.daily_realized_pnl = 0.0
            self._daily_reset_date = today
            logger.info("Daily PnL reset")

    def _record_pnl(self, pnl_usdt):
        self.daily_realized_pnl += pnl_usdt
        logger.info(f"Daily realized PnL: {self.daily_realized_pnl:.2f} USDT")

    def _make_tag(self, channel_name="", exchange_name=""):
        """Build a [channel | EXCHANGE] prefix tag for notifications."""
        parts = []
        if channel_name:
            parts.append(channel_name)
        if exchange_name:
            parts.append(exchange_name.upper())
        if parts:
            return f"[{' | '.join(parts)}]\n"
        return ""

    # ── Trade Execution ───────────────────────────────────

    async def _execute_spot_long(self, signal):
        ticker = signal["ticker"]
        exchange_name = signal.get("exchange_name", "binance")
        symbol = self._make_symbol(ticker, futures=False, exchange_name=exchange_name)
        entry = signal["entry"]
        tp1, tp3, sl = signal["tp1"], signal["tp3"], signal["sl"]
        trade_amount = signal.get("trade_amount", self.trade_amount)
        channel_name = signal.get("channel_name", "")
        tag = self._make_tag(channel_name, exchange_name)
        trade_id = None

        try:
            if exchange_name == "okx" and not self.config.okx_api_key:
                await self._notify(f"{tag}⚠️ OKX API 키가 설정되지 않았습니다. {ticker} 거래 불가.")
                return
            if exchange_name == "binance" and not self.config.binance_api_key:
                await self._notify(f"{tag}⚠️ Binance API 키가 설정되지 않았습니다. {ticker} 거래 불가.")
                return

            exchange = self._create_exchange(futures=False, exchange_name=exchange_name)
            qty = float(exchange.amount_to_precision(symbol, trade_amount / entry))

            trade_id = db_insert_trade(
                ticker, "LONG", entry, qty, trade_amount,
                signal["tp1"], signal["tp2"], signal["tp3"], sl, channel_name,
            )

            is_market = signal.get("market_order", False)

            if is_market:
                order = exchange.create_market_buy_order(symbol, qty)
                filled_qty = order["filled"]
                avg_price = order["average"] or order.get("price") or entry
                logger.info(f"[SPOT LONG] {symbol} MARKET FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat(),
                                exchange_order_id=str(order["id"]), exchange_name=exchange_name)
                await self._notify(
                    f"{tag}✅ {ticker} LONG 시장가 체결\n"
                    f"체결가: {avg_price} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {filled_qty} | 투입: ~{trade_amount} USDT"
                )
            else:
                order = exchange.create_limit_buy_order(symbol, qty, entry)
                order_id = order["id"]
                db_update_trade(trade_id, exchange_order_id=str(order_id), exchange_name=exchange_name)
                logger.info(f"[SPOT LONG] {symbol} entry order: {order_id} qty={qty} @ {entry}")

                await self._notify(
                    f"{tag}✅ {ticker} LONG 주문 접수\n"
                    f"진입: {entry} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {qty} | 투입: ~{trade_amount} USDT"
                )

                wait_start = time.time()
                while True:
                    if time.time() - wait_start > self.entry_timeout:
                        try:
                            exchange.cancel_order(order_id, symbol)
                        except Exception:
                            pass
                        logger.info(f"[SPOT LONG] {symbol} entry TIMEOUT ({self.entry_timeout}s)")
                        db_update_trade(trade_id, status="timeout", result="timeout",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"{tag}⏰ {ticker} LONG 진입 미체결 ({self.entry_timeout // 60}분). 주문 취소.")
                        return
                    o = exchange.fetch_order(order_id, symbol)
                    if o["status"] == "closed":
                        filled_qty = o["filled"]
                        avg_price = o["average"] or entry
                        logger.info(f"[SPOT LONG] {symbol} FILLED: {filled_qty} @ {avg_price}")
                        db_update_trade(trade_id, status="open", filled_price=avg_price,
                                        qty=filled_qty, filled_at=datetime.now().isoformat())
                        await self._notify(f"{tag}📥 {ticker} 진입 체결: {filled_qty} @ {avg_price}")
                        break
                    if o["status"] == "canceled":
                        logger.info(f"[SPOT LONG] {symbol} entry CANCELED")
                        db_update_trade(trade_id, status="cancelled", result="cancelled",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"{tag}❌ {ticker} 진입 주문 취소됨")
                        return
                    await asyncio.sleep(5)

            try:
                sl_order_id, tp_order_id = self._place_exit_orders(
                    exchange, exchange_name, symbol, "LONG", filled_qty, sl, tp3)
            except Exception as e:
                await self._emergency_close(exchange, symbol, "LONG", filled_qty, avg_price, trade_id, ticker, str(e), tag=tag)
                return
            logger.info(f"[SPOT LONG] {symbol} SL: {sl_order_id} @ {sl}, TP3: {tp_order_id} @ {tp3}")

            sl_moved = False
            while True:
                try:
                    # 1. Position check FIRST — cancel orders before they can fire
                    balance = exchange.fetch_balance()
                    token_total = float(balance.get(ticker, {}).get("total", 0))
                    if token_total < filled_qty * 0.95:
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [sl_order_id, tp_order_id])
                        db_update_trade(trade_id, status="closed", result="external",
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[SPOT LONG] {symbol} position closed externally")
                        await self._notify(f"{tag}📊 {ticker} LONG 포지션 외부에서 종료됨")
                        return

                    # 2. Price check for trailing SL
                    ticker_data = exchange.fetch_ticker(symbol)
                    price = ticker_data["last"]

                    if not sl_moved and price >= tp1:
                        logger.info(f"[SPOT LONG] {symbol} TP1 reached ({price}). Moving SL to {avg_price}")
                        try:
                            self._cancel_exit_order(exchange, exchange_name, sl_order_id, symbol)
                            sl_order = self._create_sl_order(exchange, exchange_name, symbol, "LONG", filled_qty, avg_price)
                            sl_order_id = sl_order["id"]
                            sl_moved = True
                            db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                            await self._notify(f"{tag}🔄 {ticker} TP1 도달! SL → 진입점({avg_price}) 이동")
                        except Exception as e:
                            logger.error(f"Failed to move SL: {e}")

                    # 3. Check TP/SL status (OKX: uses algo order API via _fetch_exit_order)
                    tp_status = self._fetch_exit_order(exchange, exchange_name, tp_order_id, symbol)
                    if tp_status["status"] == "closed":
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [sl_order_id])
                        pnl = round((tp3 - avg_price) / avg_price * 100, 2)
                        pnl_usdt = round((tp3 - avg_price) * filled_qty, 2)
                        self._record_pnl((tp3 - avg_price) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="tp3_hit",
                                        exit_price=tp3, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[SPOT LONG] {symbol} TP3 HIT! PnL: {pnl}%")
                        await self._notify(f"{tag}📊 {ticker} LONG 거래 완료\n결과: TP3 도달\n수익률: {pnl}%")
                        return

                    sl_status = self._fetch_exit_order(exchange, exchange_name, sl_order_id, symbol)
                    if sl_status["status"] == "closed":
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [tp_order_id])
                        sl_fill = sl_status["average"] or sl
                        pnl = round((sl_fill - avg_price) / avg_price * 100, 2)
                        pnl_usdt = round((sl_fill - avg_price) * filled_qty, 2)
                        self._record_pnl((sl_fill - avg_price) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="sl_hit",
                                        exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[SPOT LONG] {symbol} SL HIT @ {sl_fill}. PnL: {pnl}%")
                        await self._notify(f"{tag}📊 {ticker} LONG 거래 완료\n결과: SL 도달 @ {sl_fill}\n수익률: {pnl}%")
                        return

                except ccxt.NetworkError as e:
                    logger.warning(f"Network error: {e}")

                await asyncio.sleep(10)

        except Exception as e:
            if trade_id:
                db_update_trade(trade_id, status="error", result=str(e)[:200],
                                closed_at=datetime.now().isoformat())
            logger.error(f"[SPOT LONG] {symbol} error: {e}")
            await self._notify(f"{tag}⚠️ {ticker} LONG 에러: {e}")

    async def _execute_futures_long(self, signal):
        ticker = signal["ticker"]
        exchange_name = signal.get("exchange_name", "binance")
        symbol = self._make_symbol(ticker, futures=True, exchange_name=exchange_name)
        entry = signal["entry"]
        tp1, tp3, sl = signal["tp1"], signal["tp3"], signal["sl"]
        leverage = signal.get("leverage", 1)
        trade_amount = signal.get("trade_amount", self.trade_amount)
        channel_name = signal.get("channel_name", "")
        tag = self._make_tag(channel_name, exchange_name)
        trade_id = None

        try:
            if exchange_name == "okx" and not self.config.okx_api_key:
                await self._notify(f"{tag}⚠️ OKX API 키가 설정되지 않았습니다. {ticker} 거래 불가.")
                return
            if exchange_name == "binance" and not self.config.binance_api_key:
                await self._notify(f"{tag}⚠️ Binance API 키가 설정되지 않았습니다. {ticker} 거래 불가.")
                return

            exchange = self._create_exchange(futures=True, exchange_name=exchange_name)

            # Set leverage FIRST, then fetch actual leverage for margin-based qty calc
            self._set_leverage_and_margin(exchange, exchange_name, leverage, symbol)
            actual_leverage = self._fetch_leverage(exchange, exchange_name, symbol, fallback=leverage)
            notional = trade_amount * actual_leverage
            qty = float(exchange.amount_to_precision(symbol, notional / entry))
            logger.info(f"[FUTURES LONG] {symbol} margin={trade_amount} * {actual_leverage}x = {notional} notional, qty={qty}")

            trade_id = db_insert_trade(
                ticker, "LONG", entry, qty, trade_amount,
                signal["tp1"], signal["tp2"], signal["tp3"], sl, channel_name,
            )

            is_market = signal.get("market_order", False)

            if is_market:
                order = exchange.create_market_buy_order(symbol, qty)
                filled_qty = order["filled"]
                avg_price = order["average"] or order.get("price") or entry
                logger.info(f"[FUTURES LONG] {symbol} MARKET FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat(),
                                exchange_order_id=str(order["id"]), exchange_name=exchange_name)
                await self._notify(
                    f"{tag}✅ {ticker} LONG 선물 시장가 체결\n"
                    f"체결가: {avg_price} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {filled_qty} | 증거금: ~{trade_amount} USDT | {actual_leverage}x"
                )
            else:
                order = exchange.create_limit_buy_order(symbol, qty, entry)
                order_id = order["id"]
                db_update_trade(trade_id, exchange_order_id=str(order_id), exchange_name=exchange_name)
                logger.info(f"[FUTURES LONG] {symbol} entry order: {order_id} qty={qty} @ {entry}")

                await self._notify(
                    f"{tag}✅ {ticker} LONG 선물 주문 접수\n"
                    f"진입: {entry} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {qty} | 증거금: ~{trade_amount} USDT | {actual_leverage}x"
                )

                wait_start = time.time()
                while True:
                    if time.time() - wait_start > self.entry_timeout:
                        try:
                            exchange.cancel_order(order_id, symbol)
                        except Exception:
                            pass
                        logger.info(f"[FUTURES LONG] {symbol} entry TIMEOUT ({self.entry_timeout}s)")
                        db_update_trade(trade_id, status="timeout", result="timeout",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"{tag}⏰ {ticker} LONG 진입 미체결 ({self.entry_timeout // 60}분). 주문 취소.")
                        return
                    o = exchange.fetch_order(order_id, symbol)
                    if o["status"] == "closed":
                        filled_qty = o["filled"]
                        avg_price = o["average"] or entry
                        logger.info(f"[FUTURES LONG] {symbol} FILLED: {filled_qty} @ {avg_price}")
                        db_update_trade(trade_id, status="open", filled_price=avg_price,
                                        qty=filled_qty, filled_at=datetime.now().isoformat())
                        await self._notify(f"{tag}📥 {ticker} 롱 진입 체결: {filled_qty} @ {avg_price}")
                        break
                    if o["status"] == "canceled":
                        logger.info(f"[FUTURES LONG] {symbol} entry CANCELED")
                        db_update_trade(trade_id, status="cancelled", result="cancelled",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"{tag}❌ {ticker} 진입 주문 취소됨")
                        return
                    await asyncio.sleep(5)

            try:
                sl_order_id, tp_order_id = self._place_exit_orders(
                    exchange, exchange_name, symbol, "LONG", filled_qty, sl, tp3, futures=True)
            except Exception as e:
                await self._emergency_close(exchange, symbol, "LONG", filled_qty, avg_price, trade_id, ticker, str(e), tag=tag)
                return
            logger.info(f"[FUTURES LONG] {symbol} SL: {sl_order_id} @ {sl}, TP3: {tp_order_id} @ {tp3}")

            sl_moved = False
            while True:
                try:
                    # 1. Position check FIRST — cancel orders before they can fire
                    positions = exchange.fetch_positions([symbol])
                    active = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
                    if not active:
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [sl_order_id, tp_order_id])
                        self._close_ghost_position(exchange, exchange_name, symbol, "LONG")
                        db_update_trade(trade_id, status="closed", result="external",
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES LONG] {symbol} position closed externally")
                        await self._notify(f"{tag}📊 {ticker} LONG 포지션 외부에서 종료됨")
                        return

                    # 2. Price check for trailing SL
                    ticker_data = exchange.fetch_ticker(symbol)
                    price = ticker_data["last"]

                    if not sl_moved and price >= tp1:
                        logger.info(f"[FUTURES LONG] {symbol} TP1 reached ({price}). Moving SL to {avg_price}")
                        try:
                            self._cancel_exit_order(exchange, exchange_name, sl_order_id, symbol)
                            sl_order = self._create_sl_order(exchange, exchange_name, symbol, "LONG", filled_qty, avg_price, futures=True)
                            sl_order_id = sl_order["id"]
                            sl_moved = True
                            db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                            await self._notify(f"{tag}🔄 {ticker} TP1 도달! SL → 진입점({avg_price}) 이동")
                        except Exception as e:
                            logger.error(f"Failed to move SL: {e}")

                    # 3. Check TP/SL status (OKX: uses algo order API via _fetch_exit_order)
                    tp_status = self._fetch_exit_order(exchange, exchange_name, tp_order_id, symbol)
                    if tp_status["status"] == "closed":
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [sl_order_id])
                        self._close_ghost_position(exchange, exchange_name, symbol, "LONG")
                        pnl = round((tp3 - avg_price) / avg_price * 100, 2)
                        pnl_usdt = round((tp3 - avg_price) * filled_qty, 2)
                        self._record_pnl((tp3 - avg_price) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="tp3_hit",
                                        exit_price=tp3, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES LONG] {symbol} TP3 HIT! PnL: {pnl}%")
                        await self._notify(f"{tag}📊 {ticker} LONG 거래 완료\n결과: TP3 도달\n수익률: {pnl}%")
                        return

                    sl_status = self._fetch_exit_order(exchange, exchange_name, sl_order_id, symbol)
                    if sl_status["status"] == "closed":
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [tp_order_id])
                        self._close_ghost_position(exchange, exchange_name, symbol, "LONG")
                        sl_fill = sl_status["average"] or sl
                        pnl = round((sl_fill - avg_price) / avg_price * 100, 2)
                        pnl_usdt = round((sl_fill - avg_price) * filled_qty, 2)
                        self._record_pnl((sl_fill - avg_price) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="sl_hit",
                                        exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES LONG] {symbol} SL HIT @ {sl_fill}. PnL: {pnl}%")
                        await self._notify(f"{tag}📊 {ticker} LONG 거래 완료\n결과: SL 도달 @ {sl_fill}\n수익률: {pnl}%")
                        return

                except ccxt.NetworkError as e:
                    logger.warning(f"Network error: {e}")

                await asyncio.sleep(10)

        except Exception as e:
            if trade_id:
                db_update_trade(trade_id, status="error", result=str(e)[:200],
                                closed_at=datetime.now().isoformat())
            logger.error(f"[FUTURES LONG] {symbol} error: {e}")
            await self._notify(f"{tag}⚠️ {ticker} LONG 에러: {e}")

    async def _execute_futures_short(self, signal):
        ticker = signal["ticker"]
        exchange_name = signal.get("exchange_name", "binance")
        symbol = self._make_symbol(ticker, futures=True, exchange_name=exchange_name)
        entry = signal["entry"]
        tp1, tp3, sl = signal["tp1"], signal["tp3"], signal["sl"]
        leverage = signal.get("leverage", 1)
        trade_amount = signal.get("trade_amount", self.trade_amount)
        channel_name = signal.get("channel_name", "")
        tag = self._make_tag(channel_name, exchange_name)
        trade_id = None

        try:
            if exchange_name == "okx" and not self.config.okx_api_key:
                await self._notify(f"{tag}⚠️ OKX API 키가 설정되지 않았습니다. {ticker} 거래 불가.")
                return
            if exchange_name == "binance" and not self.config.binance_api_key:
                await self._notify(f"{tag}⚠️ Binance API 키가 설정되지 않았습니다. {ticker} 거래 불가.")
                return

            exchange = self._create_exchange(futures=True, exchange_name=exchange_name)

            # Set leverage FIRST, then fetch actual leverage for margin-based qty calc
            self._set_leverage_and_margin(exchange, exchange_name, leverage, symbol)
            actual_leverage = self._fetch_leverage(exchange, exchange_name, symbol, fallback=leverage)
            notional = trade_amount * actual_leverage
            qty = float(exchange.amount_to_precision(symbol, notional / entry))
            logger.info(f"[FUTURES SHORT] {symbol} margin={trade_amount} * {actual_leverage}x = {notional} notional, qty={qty}")

            trade_id = db_insert_trade(
                ticker, "SHORT", entry, qty, trade_amount,
                signal["tp1"], signal["tp2"], signal["tp3"], sl, channel_name,
            )

            is_market = signal.get("market_order", False)

            if is_market:
                order = exchange.create_market_sell_order(symbol, qty)
                filled_qty = order["filled"]
                avg_price = order["average"] or order.get("price") or entry
                logger.info(f"[FUTURES SHORT] {symbol} MARKET FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat(),
                                exchange_order_id=str(order["id"]), exchange_name=exchange_name)
                await self._notify(
                    f"{tag}✅ {ticker} SHORT 시장가 체결\n"
                    f"체결가: {avg_price} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {filled_qty} | 증거금: ~{trade_amount} USDT | {actual_leverage}x"
                )
            else:
                order = exchange.create_limit_sell_order(symbol, qty, entry)
                order_id = order["id"]
                db_update_trade(trade_id, exchange_order_id=str(order_id), exchange_name=exchange_name)
                logger.info(f"[FUTURES SHORT] {symbol} entry order: {order_id} qty={qty} @ {entry}")

                await self._notify(
                    f"{tag}✅ {ticker} SHORT 주문 접수\n"
                    f"진입: {entry} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {qty} | 증거금: ~{trade_amount} USDT | {actual_leverage}x"
                )

                wait_start = time.time()
                while True:
                    if time.time() - wait_start > self.entry_timeout:
                        try:
                            exchange.cancel_order(order_id, symbol)
                        except Exception:
                            pass
                        logger.info(f"[FUTURES SHORT] {symbol} entry TIMEOUT ({self.entry_timeout}s)")
                        db_update_trade(trade_id, status="timeout", result="timeout",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"{tag}⏰ {ticker} SHORT 진입 미체결 ({self.entry_timeout // 60}분). 주문 취소.")
                        return
                    o = exchange.fetch_order(order_id, symbol)
                    if o["status"] == "closed":
                        filled_qty = o["filled"]
                        avg_price = o["average"] or entry
                        logger.info(f"[FUTURES SHORT] {symbol} FILLED: {filled_qty} @ {avg_price}")
                        db_update_trade(trade_id, status="open", filled_price=avg_price,
                                        qty=filled_qty, filled_at=datetime.now().isoformat())
                        await self._notify(f"{tag}📥 {ticker} 숏 진입 체결: {filled_qty} @ {avg_price}")
                        break
                    if o["status"] == "canceled":
                        logger.info(f"[FUTURES SHORT] {symbol} entry CANCELED")
                        db_update_trade(trade_id, status="cancelled", result="cancelled",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"{tag}❌ {ticker} 진입 주문 취소됨")
                        return
                    await asyncio.sleep(5)

            try:
                sl_order_id, tp_order_id = self._place_exit_orders(
                    exchange, exchange_name, symbol, "SHORT", filled_qty, sl, tp3, futures=True)
            except Exception as e:
                await self._emergency_close(exchange, symbol, "SHORT", filled_qty, avg_price, trade_id, ticker, str(e), tag=tag)
                return
            logger.info(f"[FUTURES SHORT] {symbol} SL: {sl_order_id} @ {sl}, TP3: {tp_order_id} @ {tp3}")

            sl_moved = False
            while True:
                try:
                    # 1. Position check FIRST — cancel orders before they can fire
                    positions = exchange.fetch_positions([symbol])
                    active = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
                    if not active:
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [sl_order_id, tp_order_id])
                        self._close_ghost_position(exchange, exchange_name, symbol, "SHORT")
                        db_update_trade(trade_id, status="closed", result="external",
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES SHORT] {symbol} position closed externally")
                        await self._notify(f"{tag}📊 {ticker} SHORT 포지션 외부에서 종료됨")
                        return

                    # 2. Price check for trailing SL
                    ticker_data = exchange.fetch_ticker(symbol)
                    price = ticker_data["last"]

                    if not sl_moved and price <= tp1:
                        logger.info(f"[FUTURES SHORT] {symbol} TP1 reached ({price}). Moving SL to {avg_price}")
                        try:
                            self._cancel_exit_order(exchange, exchange_name, sl_order_id, symbol)
                            sl_order = self._create_sl_order(exchange, exchange_name, symbol, "SHORT", filled_qty, avg_price, futures=True)
                            sl_order_id = sl_order["id"]
                            sl_moved = True
                            db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                            await self._notify(f"{tag}🔄 {ticker} TP1 도달! SL → 진입점({avg_price}) 이동")
                        except Exception as e:
                            logger.error(f"Failed to move SL: {e}")

                    # 3. Check TP/SL status (OKX: uses algo order API via _fetch_exit_order)
                    tp_status = self._fetch_exit_order(exchange, exchange_name, tp_order_id, symbol)
                    if tp_status["status"] == "closed":
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [sl_order_id])
                        self._close_ghost_position(exchange, exchange_name, symbol, "SHORT")
                        pnl = round((avg_price - tp3) / avg_price * 100, 2)
                        pnl_usdt = round((avg_price - tp3) * filled_qty, 2)
                        self._record_pnl((avg_price - tp3) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="tp3_hit",
                                        exit_price=tp3, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES SHORT] {symbol} TP3 HIT! PnL: {pnl}%")
                        await self._notify(f"{tag}📊 {ticker} SHORT 거래 완료\n결과: TP3 도달\n수익률: {pnl}%")
                        return

                    sl_status = self._fetch_exit_order(exchange, exchange_name, sl_order_id, symbol)
                    if sl_status["status"] == "closed":
                        self._cancel_exit_orders_safe(exchange, exchange_name, symbol, [tp_order_id])
                        self._close_ghost_position(exchange, exchange_name, symbol, "SHORT")
                        sl_fill = sl_status["average"] or sl
                        pnl = round((avg_price - sl_fill) / avg_price * 100, 2)
                        pnl_usdt = round((avg_price - sl_fill) * filled_qty, 2)
                        self._record_pnl((avg_price - sl_fill) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="sl_hit",
                                        exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES SHORT] {symbol} SL HIT @ {sl_fill}. PnL: {pnl}%")
                        await self._notify(f"{tag}📊 {ticker} SHORT 거래 완료\n결과: SL 도달 @ {sl_fill}\n수익률: {pnl}%")
                        return

                except ccxt.NetworkError as e:
                    logger.warning(f"Network error: {e}")

                await asyncio.sleep(10)

        except Exception as e:
            if trade_id:
                db_update_trade(trade_id, status="error", result=str(e)[:200],
                                closed_at=datetime.now().isoformat())
            logger.error(f"[FUTURES SHORT] {symbol} error: {e}")
            await self._notify(f"{tag}⚠️ {ticker} SHORT 에러: {e}")

    # ── Setup ─────────────────────────────────────────────

    async def setup(self):
        """Resolve channels, register signal handler."""
        if not self.config.has_trading_config:
            logger.info("Trading config incomplete. Trader disabled.")
            return

        self.apply_settings_from_db()
        self.daily_realized_pnl = db_get_today_pnl()
        logger.info(f"Today's realized PnL: {self.daily_realized_pnl:.2f} USDT")

        # Load channel formats from DB
        channel_formats = db_get_channel_formats()
        source_entities = []
        channel_names = []

        for fmt in channel_formats:
            if not fmt.get("enabled"):
                continue
            ch = fmt["channel_id"]
            try:
                entity = await self.client.get_entity(ch)
                source_entities.append(entity)
                name = getattr(entity, "title", ch)
                compiled, fields = compile_template(fmt["template"])
                ex_name = fmt.get("exchange", "binance")
                noise_filter = fmt.get("noise_filter", "")
                marked_id = tl_utils.get_peer_id(tl_utils.get_peer(entity))
                self._channel_templates[marked_id] = {
                    "regex": compiled,
                    "fields": fields,
                    "default_side": fmt.get("default_side", "LONG"),
                    "trade_amount": float(fmt.get("trade_amount", 0)),
                    "channel_name": name,
                    "exchange_name": ex_name,
                    "noise_filter": noise_filter,
                }
                channel_names.append(f"{name}[{ex_name.upper()}]")
                logger.info(f"Monitoring (template): {name} ({ch}) [exchange={ex_name}] marked_id={marked_id}")
            except Exception as e:
                logger.error(f"Cannot resolve channel '{ch}': {e}")

        # Fallback: .env SOURCE_CHANNELS with default pattern
        for ch in self.config.source_channels:
            try:
                entity = await self.client.get_entity(ch)
                marked_id = tl_utils.get_peer_id(tl_utils.get_peer(entity))
                if marked_id not in self._channel_templates:
                    source_entities.append(entity)
                    name = getattr(entity, "title", ch)
                    channel_names.append(name)
                    logger.info(f"Monitoring (default pattern): {name} (@{ch})")
            except Exception as e:
                logger.error(f"Cannot resolve channel '{ch}': {e}")

        if not source_entities:
            logger.warning("No valid source channels for trading.")
            self.enabled = True  # Still enable so channels can be added later
            return

        trader = self  # closure reference

        @self.client.on(events.NewMessage(chats=source_entities))
        async def signal_handler(event):
            if not trader.enabled:
                return
            text = event.message.message
            if not text:
                return

            chat_id = event.chat_id
            template_info = trader._channel_templates.get(chat_id)

            # Noise filter: silently skip known non-signal messages
            if template_info and template_info.get("noise_filter"):
                keywords = [k.strip() for k in template_info["noise_filter"].split(",") if k.strip()]
                if any(kw in text for kw in keywords):
                    logger.debug(f"Noise filtered: {text[:60]}...")
                    return

            if template_info:
                signal = parse_with_template(
                    text, template_info["regex"],
                    template_info["fields"], template_info["default_side"],
                )
                if signal:
                    if template_info.get("trade_amount", 0) > 0:
                        signal["trade_amount"] = template_info["trade_amount"]
                    signal["exchange_name"] = template_info.get("exchange_name", "binance")
                    signal["channel_name"] = template_info.get("channel_name", "")
            else:
                signal = parse_signal(text)

            # Build tag for notifications using channel/exchange info
            ch_name = template_info.get("channel_name", "") if template_info else ""
            ch_exchange = template_info.get("exchange_name", "") if template_info else ""

            if not signal:
                tag = trader._make_tag(ch_name, ch_exchange)
                preview = text[:80].replace("\n", " ")
                if len(text) > 80:
                    preview += "…"
                logger.info(f"Non-signal message ignored: {preview}")
                await trader._notify(f"{tag}💬 메시지 수신 (신호 아님, 무시)\n\n\"{preview}\"")
                return

            ticker = signal["ticker"]
            sig_exchange = signal.get("exchange_name", ch_exchange or "binance")
            sig_channel = signal.get("channel_name", ch_name)
            tag = trader._make_tag(sig_channel, sig_exchange)

            # Fetch market price if entry is missing
            if "entry" not in signal:
                signal["market_order"] = True
                try:
                    price = await trader._fetch_current_price(ticker, sig_exchange)
                    signal["entry"] = price
                    logger.info(f"No entry in signal, using market price: {price}")
                except Exception as e:
                    logger.error(f"Failed to fetch price for {ticker}: {e}")
                    await trader._notify(f"{tag}⚠️ {ticker} 현재가 조회 실패: {e}")
                    return

            fill_signal_defaults(signal)
            side = signal["side"]

            # Cap leverage to MAX_LEVERAGE
            raw_leverage = signal.get("leverage", 1)
            if raw_leverage > trader.max_leverage:
                logger.info(f"Leverage capped: {raw_leverage}x → {trader.max_leverage}x (MAX_LEVERAGE)")
                signal["leverage"] = trader.max_leverage

            logger.info(f"Signal detected: #{ticker} – {side}")

            # TRADE_BLOCKED: completely blocked from all trading (LONG + SHORT)
            if ticker in trader.trade_blocked:
                logger.info(f"BLOCKED: {ticker} is trade-blocked (all directions)")
                await trader._notify(f"{tag}⛔ {ticker} 거래 금지 종목. 모든 신호 무시.")
                return

            # SELL_BLOCKED: only SHORT is blocked
            if ticker in trader.sell_blocked and side == "SHORT":
                logger.info(f"BLOCKED: {ticker} SHORT is prohibited")
                await trader._notify(f"{tag}⛔ {ticker} 매도 금지 종목. SHORT 시그널 무시.")
                return

            trader._check_daily_reset()
            if trader.daily_realized_pnl <= -trader.daily_loss_limit:
                logger.info(f"Daily loss limit reached: {trader.daily_realized_pnl:.2f} USDT")
                await trader._notify(f"{tag}⛔ 일일 손실 한도 도달 ({trader.daily_realized_pnl:.2f}/{-trader.daily_loss_limit} USDT). 신호 무시.")
                return

            if len(trader.active_trades) >= trader.max_concurrent:
                logger.info(f"Max concurrent positions reached: {len(trader.active_trades)}")
                await trader._notify(f"{tag}⛔ 동시 포지션 한도 도달 ({len(trader.active_trades)}/{trader.max_concurrent}개). 신호 무시.")
                return

            trade_key = f"{ticker}_{side}"
            if trade_key in trader.active_trades:
                logger.info(f"Already trading {trade_key}, skipping")
                await trader._notify(f"{tag}⏭️ {ticker} {side} 이미 진행 중. 스킵.")
                return

            trader.active_trades[trade_key] = signal

            async def run_trade():
                try:
                    # Sync exchange trades in background (non-blocking)
                    asyncio.create_task(trader._run_exchange_sync())
                    leverage = signal.get("leverage", 1)
                    if side == "LONG":
                        if leverage > 1:
                            await trader._execute_futures_long(signal)
                        else:
                            await trader._execute_spot_long(signal)
                    else:
                        await trader._execute_futures_short(signal)
                finally:
                    trader.active_trades.pop(trade_key, None)

            asyncio.create_task(run_trade())

        self.enabled = True
        await self._notify(
            "🟢 트레이딩 봇 시작됨\n"
            f"모니터링: {', '.join(channel_names) if channel_names else '(없음)'}\n"
            f"투입: {self.trade_amount} USDT/신호\n"
            f"거래금지: {', '.join(sorted(self.trade_blocked)) if self.trade_blocked else '없음'}"
        )
        logger.info(f"Trader module ready. Monitoring {len(source_entities)} channel(s).")

    async def _run_exchange_sync(self):
        """Run exchange trade sync in background. Never crashes the bot."""
        try:
            await sync_exchange_trades(self.config)
        except Exception as e:
            logger.warning(f"Exchange sync failed (non-fatal): {e}")

    async def shutdown(self):
        await self._notify("🔴 트레이딩 봇 종료됨")
        await self._http_client.aclose()

    async def simulate_signal(self, text, channel_id=None):
        """Process a manually entered signal text, same as if received from Telegram."""
        if not text:
            return {"error": "Message text is required"}

        # Try channel-specific template first
        signal = None
        used_template = None

        matched_info = None
        if channel_id:
            for chat_id, info in self._channel_templates.items():
                if info.get("channel_name") == channel_id or str(chat_id) == str(channel_id):
                    signal = parse_with_template(text, info["regex"], info["fields"], info["default_side"])
                    if signal:
                        used_template = info["channel_name"]
                        matched_info = info
                        break

        # Try all registered templates
        if not signal:
            for chat_id, info in self._channel_templates.items():
                signal = parse_with_template(text, info["regex"], info["fields"], info["default_side"])
                if signal:
                    used_template = info["channel_name"]
                    matched_info = info
                    break

        # Fallback to default parser
        if not signal:
            signal = parse_signal(text)
            if signal:
                used_template = "default"

        if not signal:
            return {"error": "No template matched this message"}

        # Propagate exchange, trade_amount, and channel_name from matched template
        if matched_info:
            signal["exchange_name"] = matched_info.get("exchange_name", "binance")
            signal["channel_name"] = matched_info.get("channel_name", used_template or "")
            if matched_info.get("trade_amount", 0) > 0:
                signal["trade_amount"] = matched_info["trade_amount"]

        ticker = signal["ticker"]
        sig_exchange = signal.get("exchange_name", "binance")

        # Fetch market price if entry is missing
        if "entry" not in signal:
            signal["market_order"] = True
            try:
                price = await self._fetch_current_price(ticker, sig_exchange)
                signal["entry"] = price
            except Exception as e:
                return {"error": f"Failed to fetch price for {ticker}: {e}"}

        fill_signal_defaults(signal)
        side = signal["side"]

        # Validate trade conditions
        if ticker in self.trade_blocked:
            return {"error": f"{ticker} is in trade-blocked list (all directions)"}

        if ticker in self.sell_blocked and side == "SHORT":
            return {"error": f"{ticker} is in sell-blocked list"}

        self._check_daily_reset()
        if self.daily_realized_pnl <= -self.daily_loss_limit:
            return {"error": "Daily loss limit reached"}

        if len(self.active_trades) >= self.max_concurrent:
            return {"error": f"Max concurrent positions reached ({self.max_concurrent})"}

        trade_key = f"{ticker}_{side}"
        if trade_key in self.active_trades:
            return {"error": f"{ticker} {side} already in progress"}

        # Execute trade
        self.active_trades[trade_key] = signal
        logger.info(f"[SIMULATE] Signal: #{ticker} – {side} (template: {used_template})")

        async def run_trade():
            try:
                # Sync exchange trades in background (non-blocking)
                asyncio.create_task(self._run_exchange_sync())
                leverage = signal.get("leverage", 1)
                if side == "LONG":
                    if leverage > 1:
                        await self._execute_futures_long(signal)
                    else:
                        await self._execute_spot_long(signal)
                else:
                    await self._execute_futures_short(signal)
            finally:
                self.active_trades.pop(trade_key, None)

        asyncio.create_task(run_trade())

        return {
            "ok": True,
            "signal": {
                "ticker": ticker,
                "side": side,
                "entry": signal.get("entry"),
                "tp1": signal.get("tp1"),
                "tp2": signal.get("tp2"),
                "tp3": signal.get("tp3"),
                "sl": signal.get("sl"),
                "leverage": signal.get("leverage", 1),
                "market_order": signal.get("market_order", False),
            },
            "template_used": used_template,
        }

    # ── Public API for dashboard ──────────────────────────

    def get_stats(self, channel=None):
        stats = db_get_stats(channel=channel)
        stats["active_trades"] = list(self.active_trades.keys())
        stats["daily_realized_pnl"] = round(self.daily_realized_pnl, 2)
        return stats

    def get_performance_stats(self, period='lifetime', channel=None):
        return db_get_performance_stats(period=period, channel=channel)

    def get_performance_table(self, period='lifetime'):
        return db_get_performance_table(period=period)

    def get_trades(self, limit=100, status=None, channel=None):
        return db_get_trades(limit=limit, status=status, channel=channel)

    def get_settings(self):
        return {
            "TRADE_AMOUNT": self.trade_amount,
            "SELL_BLOCKED": ",".join(sorted(self.sell_blocked)),
            "TRADE_BLOCKED": ",".join(sorted(self.trade_blocked)),
            "MAX_CONCURRENT": self.max_concurrent,
            "DAILY_LOSS_LIMIT": self.daily_loss_limit,
            "ENTRY_TIMEOUT": self.entry_timeout,
            "MAX_LEVERAGE": self.max_leverage,
        }

    async def update_settings(self, data):
        updates = {}
        if "TRADE_AMOUNT" in data:
            val = float(data["TRADE_AMOUNT"])
            if val <= 0:
                return {"error": "TRADE_AMOUNT must be > 0"}
            self.trade_amount = val
            updates["TRADE_AMOUNT"] = val
        if "SELL_BLOCKED" in data:
            raw = str(data["SELL_BLOCKED"]).strip()
            self.sell_blocked = {s.strip().upper() for s in raw.split(",") if s.strip()}
            updates["SELL_BLOCKED"] = raw.upper()
        if "TRADE_BLOCKED" in data:
            raw = str(data["TRADE_BLOCKED"]).strip()
            self.trade_blocked = {s.strip().upper() for s in raw.split(",") if s.strip()}
            updates["TRADE_BLOCKED"] = raw.upper()
        if "MAX_CONCURRENT" in data:
            val = int(data["MAX_CONCURRENT"])
            if val < 1:
                return {"error": "MAX_CONCURRENT must be >= 1"}
            self.max_concurrent = val
            updates["MAX_CONCURRENT"] = val
        if "DAILY_LOSS_LIMIT" in data:
            val = float(data["DAILY_LOSS_LIMIT"])
            if val <= 0:
                return {"error": "DAILY_LOSS_LIMIT must be > 0"}
            self.daily_loss_limit = val
            updates["DAILY_LOSS_LIMIT"] = val
        if "ENTRY_TIMEOUT" in data:
            val = int(data["ENTRY_TIMEOUT"])
            if val < 10:
                return {"error": "ENTRY_TIMEOUT must be >= 10"}
            self.entry_timeout = val
            updates["ENTRY_TIMEOUT"] = val
        if "MAX_LEVERAGE" in data:
            val = int(data["MAX_LEVERAGE"])
            if val < 1:
                return {"error": "MAX_LEVERAGE must be >= 1"}
            self.max_leverage = val
            updates["MAX_LEVERAGE"] = val

        if updates:
            db_save_settings(updates)
            logger.info(f"Settings updated via dashboard: {updates}")

        return {
            "ok": True,
            "TRADE_AMOUNT": self.trade_amount,
            "SELL_BLOCKED": ",".join(sorted(self.sell_blocked)),
            "TRADE_BLOCKED": ",".join(sorted(self.trade_blocked)),
            "MAX_CONCURRENT": self.max_concurrent,
            "DAILY_LOSS_LIMIT": self.daily_loss_limit,
            "ENTRY_TIMEOUT": self.entry_timeout,
            "MAX_LEVERAGE": self.max_leverage,
        }
