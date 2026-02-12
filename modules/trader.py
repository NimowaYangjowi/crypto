"""Trader module — monitors Telegram signals and executes trades on Binance."""

import asyncio
import logging
import re
import time
from datetime import datetime
from telethon import TelegramClient, events
import ccxt
import httpx

from core.config import AppConfig
from core.database import (
    db_insert_trade, db_update_trade, db_get_trades, db_get_stats,
    db_get_today_pnl, db_load_settings, db_save_settings,
    db_get_channel_formats,
)

logger = logging.getLogger("trader")

# ── Signal Parser ─────────────────────────────────────────

SIGNAL_PATTERN = re.compile(
    r"#(\w+)\s*[–—-]\s*(LONG|SHORT)\s*"
    r".*?진입\s*포인트[:\s]*([\d.]+)\s*"
    r".*?목표\s*수익[:\s]*([\d.,\s]+)\s*"
    r".*?손절가[:\s]*([\d.]+)",
    re.DOTALL | re.IGNORECASE,
)


def parse_signal(text):
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


# ── Template System ──────────────────────────────────────

PLACEHOLDER_RE = re.compile(r'\{(ticker|side|entry|tp1|tp2|tp3|sl)\}')

CAPTURE_MAP = {
    'ticker': r'(\w+)',
    'side': r'(LONG|SHORT|long|short)',
    'entry': r'([\d,.]+)',
    'tp1': r'([\d,.]+)',
    'tp2': r'([\d,.]+)',
    'tp3': r'([\d,.]+)',
    'sl': r'([\d,.]+)',
}

WS_MARKER = '\x00WS\x00'


def compile_template(template: str):
    """Convert a template with {placeholders} to (compiled_regex, field_list)."""
    parts = PLACEHOLDER_RE.split(template)
    fields = []
    regex_str = ''

    for i, part in enumerate(parts):
        if i % 2 == 1:  # field name
            fields.append(part)
            regex_str += CAPTURE_MAP[part]
        else:  # literal text
            cleaned = re.sub(r'\s+', WS_MARKER, part)
            escaped = re.escape(cleaned)
            escaped = escaped.replace(re.escape(WS_MARKER), r'\s+')
            regex_str += escaped

    compiled = re.compile(regex_str, re.DOTALL | re.IGNORECASE)
    return compiled, fields


def parse_with_template(text, compiled_regex, fields, default_side='LONG'):
    """Parse text using a compiled template regex."""
    match = compiled_regex.search(text)
    if not match:
        return None

    result = {}
    for i, field in enumerate(fields):
        value = match.group(i + 1).strip()
        if field == 'ticker':
            result['ticker'] = value.upper()
        elif field == 'side':
            result['side'] = value.upper()
        else:
            try:
                result[field] = float(value.replace(',', ''))
            except ValueError:
                pass

    if 'ticker' not in result:
        return None
    if 'side' not in result:
        result['side'] = default_side

    return result


def fill_signal_defaults(signal):
    """Fill missing TP/SL with defaults based on entry price and side."""
    entry = signal.get('entry')
    if entry is None:
        return signal

    side = signal.get('side', 'LONG')
    if side == 'LONG':
        signal.setdefault('sl', round(entry * 0.95, 8))
        signal.setdefault('tp1', round(entry * 1.015, 8))
        signal.setdefault('tp2', round(entry * 1.035, 8))
        signal.setdefault('tp3', round(entry * 1.10, 8))
    else:
        signal.setdefault('sl', round(entry * 1.05, 8))
        signal.setdefault('tp1', round(entry * 0.985, 8))
        signal.setdefault('tp2', round(entry * 0.965, 8))
        signal.setdefault('tp3', round(entry * 0.90, 8))

    signal.setdefault('tp4', signal['tp3'])
    return signal


class TraderModule:
    def __init__(self, client: TelegramClient, config: AppConfig):
        self.client = client
        self.config = config
        self.enabled = False

        # Mutable settings (can be changed at runtime)
        self.trade_amount = config.trade_amount
        self.sell_blocked = set(config.sell_blocked)
        self.max_concurrent = config.max_concurrent
        self.daily_loss_limit = config.daily_loss_limit
        self.entry_timeout = config.entry_timeout

        # Runtime state
        self.active_trades = {}
        self.daily_realized_pnl = 0.0
        self._daily_reset_date = datetime.now().date()
        self._http_client = httpx.AsyncClient(timeout=10)
        self._channel_templates = {}  # chat_id -> {regex, fields, default_side}

    def apply_settings_from_db(self):
        saved = db_load_settings()
        if not saved:
            return
        if "TRADE_AMOUNT" in saved:
            self.trade_amount = float(saved["TRADE_AMOUNT"])
        if "SELL_BLOCKED" in saved:
            self.sell_blocked = {s.strip().upper() for s in saved["SELL_BLOCKED"].split(",") if s.strip()}
        if "MAX_CONCURRENT" in saved:
            self.max_concurrent = int(saved["MAX_CONCURRENT"])
        if "DAILY_LOSS_LIMIT" in saved:
            self.daily_loss_limit = float(saved["DAILY_LOSS_LIMIT"])
        if "ENTRY_TIMEOUT" in saved:
            self.entry_timeout = int(saved["ENTRY_TIMEOUT"])
        logger.info(f"Settings loaded: TRADE_AMOUNT={self.trade_amount}, SELL_BLOCKED={self.sell_blocked}, "
                     f"MAX_CONCURRENT={self.max_concurrent}, DAILY_LOSS_LIMIT={self.daily_loss_limit}, "
                     f"ENTRY_TIMEOUT={self.entry_timeout}")

    def _create_exchange(self, futures=False):
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

    async def _fetch_current_price(self, ticker):
        """Fetch current price from Binance public API."""
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

    # ── Trade Execution ───────────────────────────────────

    async def _execute_spot_long(self, signal):
        ticker = signal["ticker"]
        symbol = f"{ticker}/USDT"
        entry = signal["entry"]
        tp1, tp3, sl = signal["tp1"], signal["tp3"], signal["sl"]
        trade_id = None

        try:
            exchange = self._create_exchange(futures=False)
            market = exchange.market(symbol)
            qty = round(self.trade_amount / entry, int(market["precision"]["amount"]))

            trade_id = db_insert_trade(
                ticker, "LONG", entry, qty, self.trade_amount,
                signal["tp1"], signal["tp2"], signal["tp3"], sl,
            )

            is_market = signal.get("market_order", False)

            if is_market:
                order = exchange.create_market_buy_order(symbol, qty)
                filled_qty = order["filled"]
                avg_price = order["average"] or order.get("price") or entry
                logger.info(f"[SPOT LONG] {symbol} MARKET FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat())
                await self._notify(
                    f"✅ {ticker} LONG 시장가 체결\n"
                    f"체결가: {avg_price} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {filled_qty} | 투입: ~{self.trade_amount} USDT"
                )
            else:
                order = exchange.create_limit_buy_order(symbol, qty, entry)
                order_id = order["id"]
                logger.info(f"[SPOT LONG] {symbol} entry order: {order_id} qty={qty} @ {entry}")

                await self._notify(
                    f"✅ {ticker} LONG 주문 접수\n"
                    f"진입: {entry} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {qty} | 투입: ~{self.trade_amount} USDT"
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
                        await self._notify(f"⏰ {ticker} LONG 진입 미체결 ({self.entry_timeout // 60}분). 주문 취소.")
                        return
                    o = exchange.fetch_order(order_id, symbol)
                    if o["status"] == "closed":
                        filled_qty = o["filled"]
                        avg_price = o["average"] or entry
                        logger.info(f"[SPOT LONG] {symbol} FILLED: {filled_qty} @ {avg_price}")
                        db_update_trade(trade_id, status="open", filled_price=avg_price,
                                        qty=filled_qty, filled_at=datetime.now().isoformat())
                        await self._notify(f"📥 {ticker} 진입 체결: {filled_qty} @ {avg_price}")
                        break
                    if o["status"] == "canceled":
                        logger.info(f"[SPOT LONG] {symbol} entry CANCELED")
                        db_update_trade(trade_id, status="cancelled", result="cancelled",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"❌ {ticker} 진입 주문 취소됨")
                        return
                    await asyncio.sleep(5)

            sl_order = exchange.create_order(symbol, "stop_loss_limit", "sell", filled_qty, sl, {"stopPrice": sl})
            sl_order_id = sl_order["id"]
            tp_order = exchange.create_limit_sell_order(symbol, filled_qty, tp3)
            tp_order_id = tp_order["id"]
            logger.info(f"[SPOT LONG] {symbol} SL: {sl_order_id} @ {sl}, TP3: {tp_order_id} @ {tp3}")

            sl_moved = False
            while True:
                try:
                    ticker_data = exchange.fetch_ticker(symbol)
                    price = ticker_data["last"]

                    if not sl_moved and price >= tp1:
                        logger.info(f"[SPOT LONG] {symbol} TP1 reached ({price}). Moving SL to {avg_price}")
                        try:
                            exchange.cancel_order(sl_order_id, symbol)
                            sl_order = exchange.create_order(
                                symbol, "stop_loss_limit", "sell", filled_qty, avg_price,
                                {"stopPrice": avg_price}
                            )
                            sl_order_id = sl_order["id"]
                            sl_moved = True
                            db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                            await self._notify(f"🔄 {ticker} TP1 도달! SL → 진입점({avg_price}) 이동")
                        except Exception as e:
                            logger.error(f"Failed to move SL: {e}")

                    tp_status = exchange.fetch_order(tp_order_id, symbol)
                    if tp_status["status"] == "closed":
                        try:
                            exchange.cancel_order(sl_order_id, symbol)
                        except Exception:
                            pass
                        pnl = round((tp3 - avg_price) / avg_price * 100, 2)
                        pnl_usdt = round((tp3 - avg_price) * filled_qty, 2)
                        self._record_pnl((tp3 - avg_price) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="tp3_hit",
                                        exit_price=tp3, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[SPOT LONG] {symbol} TP3 HIT! PnL: {pnl}%")
                        await self._notify(f"📊 {ticker} LONG 거래 완료\n결과: TP3 도달\n수익률: {pnl}%")
                        return

                    sl_status = exchange.fetch_order(sl_order_id, symbol)
                    if sl_status["status"] == "closed":
                        try:
                            exchange.cancel_order(tp_order_id, symbol)
                        except Exception:
                            pass
                        sl_fill = sl_status["average"] or sl
                        pnl = round((sl_fill - avg_price) / avg_price * 100, 2)
                        pnl_usdt = round((sl_fill - avg_price) * filled_qty, 2)
                        self._record_pnl((sl_fill - avg_price) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="sl_hit",
                                        exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[SPOT LONG] {symbol} SL HIT @ {sl_fill}. PnL: {pnl}%")
                        await self._notify(f"📊 {ticker} LONG 거래 완료\n결과: SL 도달 @ {sl_fill}\n수익률: {pnl}%")
                        return

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
                        await self._notify(f"📊 {ticker} LONG 포지션 외부에서 종료됨")
                        return

                except ccxt.NetworkError as e:
                    logger.warning(f"Network error: {e}")

                await asyncio.sleep(10)

        except Exception as e:
            if trade_id:
                db_update_trade(trade_id, status="error", result=str(e)[:200],
                                closed_at=datetime.now().isoformat())
            logger.error(f"[SPOT LONG] {symbol} error: {e}")
            await self._notify(f"⚠️ {ticker} LONG 에러: {e}")

    async def _execute_futures_short(self, signal):
        ticker = signal["ticker"]
        symbol = f"{ticker}/USDT"
        entry = signal["entry"]
        tp1, tp3, sl = signal["tp1"], signal["tp3"], signal["sl"]
        trade_id = None

        try:
            exchange = self._create_exchange(futures=True)
            market = exchange.market(symbol)
            qty = round(self.trade_amount / entry, int(market["precision"]["amount"]))

            trade_id = db_insert_trade(
                ticker, "SHORT", entry, qty, self.trade_amount,
                signal["tp1"], signal["tp2"], signal["tp3"], sl,
            )

            try:
                exchange.set_leverage(1, symbol)
                exchange.set_margin_mode("isolated", symbol)
            except Exception:
                pass

            is_market = signal.get("market_order", False)

            if is_market:
                order = exchange.create_market_sell_order(symbol, qty)
                filled_qty = order["filled"]
                avg_price = order["average"] or order.get("price") or entry
                logger.info(f"[FUTURES SHORT] {symbol} MARKET FILLED: {filled_qty} @ {avg_price}")
                db_update_trade(trade_id, status="open", filled_price=avg_price,
                                qty=filled_qty, filled_at=datetime.now().isoformat())
                await self._notify(
                    f"✅ {ticker} SHORT 시장가 체결\n"
                    f"체결가: {avg_price} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {filled_qty} | 투입: ~{self.trade_amount} USDT | 1x"
                )
            else:
                order = exchange.create_limit_sell_order(symbol, qty, entry)
                order_id = order["id"]
                logger.info(f"[FUTURES SHORT] {symbol} entry order: {order_id} qty={qty} @ {entry}")

                await self._notify(
                    f"✅ {ticker} SHORT 주문 접수\n"
                    f"진입: {entry} | SL: {sl} | TP3: {tp3}\n"
                    f"수량: {qty} | 투입: ~{self.trade_amount} USDT | 1x"
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
                        await self._notify(f"⏰ {ticker} SHORT 진입 미체결 ({self.entry_timeout // 60}분). 주문 취소.")
                        return
                    o = exchange.fetch_order(order_id, symbol)
                    if o["status"] == "closed":
                        filled_qty = o["filled"]
                        avg_price = o["average"] or entry
                        logger.info(f"[FUTURES SHORT] {symbol} FILLED: {filled_qty} @ {avg_price}")
                        db_update_trade(trade_id, status="open", filled_price=avg_price,
                                        qty=filled_qty, filled_at=datetime.now().isoformat())
                        await self._notify(f"📥 {ticker} 숏 진입 체결: {filled_qty} @ {avg_price}")
                        break
                    if o["status"] == "canceled":
                        logger.info(f"[FUTURES SHORT] {symbol} entry CANCELED")
                        db_update_trade(trade_id, status="cancelled", result="cancelled",
                                        closed_at=datetime.now().isoformat())
                        await self._notify(f"❌ {ticker} 진입 주문 취소됨")
                        return
                    await asyncio.sleep(5)

            sl_order = exchange.create_order(
                symbol, "stop_market", "buy", filled_qty, None,
                {"stopPrice": sl, "reduceOnly": True}
            )
            sl_order_id = sl_order["id"]
            tp_order = exchange.create_order(
                symbol, "take_profit_market", "buy", filled_qty, None,
                {"stopPrice": tp3, "reduceOnly": True}
            )
            tp_order_id = tp_order["id"]
            logger.info(f"[FUTURES SHORT] {symbol} SL: {sl_order_id} @ {sl}, TP3: {tp_order_id} @ {tp3}")

            sl_moved = False
            while True:
                try:
                    ticker_data = exchange.fetch_ticker(symbol)
                    price = ticker_data["last"]

                    if not sl_moved and price <= tp1:
                        logger.info(f"[FUTURES SHORT] {symbol} TP1 reached ({price}). Moving SL to {avg_price}")
                        try:
                            exchange.cancel_order(sl_order_id, symbol)
                            sl_order = exchange.create_order(
                                symbol, "stop_market", "buy", filled_qty, None,
                                {"stopPrice": avg_price, "reduceOnly": True}
                            )
                            sl_order_id = sl_order["id"]
                            sl_moved = True
                            db_update_trade(trade_id, tp1_hit=1, sl_moved=1)
                            await self._notify(f"🔄 {ticker} TP1 도달! SL → 진입점({avg_price}) 이동")
                        except Exception as e:
                            logger.error(f"Failed to move SL: {e}")

                    tp_status = exchange.fetch_order(tp_order_id, symbol)
                    if tp_status["status"] == "closed":
                        try:
                            exchange.cancel_order(sl_order_id, symbol)
                        except Exception:
                            pass
                        pnl = round((avg_price - tp3) / avg_price * 100, 2)
                        pnl_usdt = round((avg_price - tp3) * filled_qty, 2)
                        self._record_pnl((avg_price - tp3) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="tp3_hit",
                                        exit_price=tp3, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES SHORT] {symbol} TP3 HIT! PnL: {pnl}%")
                        await self._notify(f"📊 {ticker} SHORT 거래 완료\n결과: TP3 도달\n수익률: {pnl}%")
                        return

                    sl_status = exchange.fetch_order(sl_order_id, symbol)
                    if sl_status["status"] == "closed":
                        try:
                            exchange.cancel_order(tp_order_id, symbol)
                        except Exception:
                            pass
                        sl_fill = sl_status["average"] or sl
                        pnl = round((avg_price - sl_fill) / avg_price * 100, 2)
                        pnl_usdt = round((avg_price - sl_fill) * filled_qty, 2)
                        self._record_pnl((avg_price - sl_fill) * filled_qty)
                        db_update_trade(trade_id, status="closed", result="sl_hit",
                                        exit_price=sl_fill, pnl_pct=pnl, pnl_usdt=pnl_usdt,
                                        closed_at=datetime.now().isoformat())
                        logger.info(f"[FUTURES SHORT] {symbol} SL HIT @ {sl_fill}. PnL: {pnl}%")
                        await self._notify(f"📊 {ticker} SHORT 거래 완료\n결과: SL 도달 @ {sl_fill}\n수익률: {pnl}%")
                        return

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
                        await self._notify(f"📊 {ticker} SHORT 포지션 외부에서 종료됨")
                        return

                except ccxt.NetworkError as e:
                    logger.warning(f"Network error: {e}")

                await asyncio.sleep(10)

        except Exception as e:
            if trade_id:
                db_update_trade(trade_id, status="error", result=str(e)[:200],
                                closed_at=datetime.now().isoformat())
            logger.error(f"[FUTURES SHORT] {symbol} error: {e}")
            await self._notify(f"⚠️ {ticker} SHORT 에러: {e}")

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
                self._channel_templates[entity.id] = {
                    "regex": compiled,
                    "fields": fields,
                    "default_side": fmt.get("default_side", "LONG"),
                    "channel_name": name,
                }
                channel_names.append(name)
                logger.info(f"Monitoring (template): {name} ({ch})")
            except Exception as e:
                logger.error(f"Cannot resolve channel '{ch}': {e}")

        # Fallback: .env SOURCE_CHANNELS with default pattern
        for ch in self.config.source_channels:
            try:
                entity = await self.client.get_entity(ch)
                if entity.id not in self._channel_templates:
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
            text = event.message.message
            if not text:
                return

            chat_id = event.chat_id
            template_info = trader._channel_templates.get(chat_id)

            if template_info:
                signal = parse_with_template(
                    text, template_info["regex"],
                    template_info["fields"], template_info["default_side"],
                )
            else:
                signal = parse_signal(text)

            if not signal:
                preview = text[:80].replace("\n", " ")
                if len(text) > 80:
                    preview += "…"
                logger.info(f"Non-signal message ignored: {preview}")
                await trader._notify(f"💬 메시지 수신 (신호 아님, 무시)\n\n\"{preview}\"")
                return

            ticker = signal["ticker"]

            # Fetch market price if entry is missing
            if "entry" not in signal:
                signal["market_order"] = True
                try:
                    price = await trader._fetch_current_price(ticker)
                    signal["entry"] = price
                    logger.info(f"No entry in signal, using market price: {price}")
                except Exception as e:
                    logger.error(f"Failed to fetch price for {ticker}: {e}")
                    await trader._notify(f"⚠️ {ticker} 현재가 조회 실패: {e}")
                    return

            fill_signal_defaults(signal)
            side = signal["side"]
            logger.info(f"Signal detected: #{ticker} – {side}")

            if ticker in trader.sell_blocked and side == "SHORT":
                logger.info(f"BLOCKED: {ticker} SHORT is prohibited")
                await trader._notify(f"⛔ {ticker} 매도 금지 종목. SHORT 시그널 무시.")
                return

            trader._check_daily_reset()
            if trader.daily_realized_pnl <= -trader.daily_loss_limit:
                logger.info(f"Daily loss limit reached: {trader.daily_realized_pnl:.2f} USDT")
                await trader._notify(f"⛔ 일일 손실 한도 도달 ({trader.daily_realized_pnl:.2f}/{-trader.daily_loss_limit} USDT). 신호 무시.")
                return

            if len(trader.active_trades) >= trader.max_concurrent:
                logger.info(f"Max concurrent positions reached: {len(trader.active_trades)}")
                await trader._notify(f"⛔ 동시 포지션 한도 도달 ({len(trader.active_trades)}/{trader.max_concurrent}개). 신호 무시.")
                return

            trade_key = f"{ticker}_{side}"
            if trade_key in trader.active_trades:
                logger.info(f"Already trading {trade_key}, skipping")
                await trader._notify(f"⏭️ {ticker} {side} 이미 진행 중. 스킵.")
                return

            trader.active_trades[trade_key] = signal

            async def run_trade():
                try:
                    if side == "LONG":
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
            f"투입: {self.trade_amount} USDT/신호"
        )
        logger.info(f"Trader module ready. Monitoring {len(source_entities)} channel(s).")

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

        if channel_id:
            # Find template for specified channel
            for chat_id, info in self._channel_templates.items():
                if info.get("channel_name") == channel_id or str(chat_id) == str(channel_id):
                    signal = parse_with_template(text, info["regex"], info["fields"], info["default_side"])
                    if signal:
                        used_template = info["channel_name"]
                        break

        # Try all registered templates
        if not signal:
            for chat_id, info in self._channel_templates.items():
                signal = parse_with_template(text, info["regex"], info["fields"], info["default_side"])
                if signal:
                    used_template = info["channel_name"]
                    break

        # Fallback to default parser
        if not signal:
            signal = parse_signal(text)
            if signal:
                used_template = "default"

        if not signal:
            return {"error": "No template matched this message"}

        ticker = signal["ticker"]

        # Fetch market price if entry is missing
        if "entry" not in signal:
            signal["market_order"] = True
            try:
                price = await self._fetch_current_price(ticker)
                signal["entry"] = price
            except Exception as e:
                return {"error": f"Failed to fetch price for {ticker}: {e}"}

        fill_signal_defaults(signal)
        side = signal["side"]

        # Validate trade conditions
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
                if side == "LONG":
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
                "market_order": signal.get("market_order", False),
            },
            "template_used": used_template,
        }

    # ── Public API for dashboard ──────────────────────────

    def get_stats(self):
        stats = db_get_stats()
        stats["active_trades"] = list(self.active_trades.keys())
        stats["daily_realized_pnl"] = round(self.daily_realized_pnl, 2)
        return stats

    def get_trades(self, limit=100, status=None):
        return db_get_trades(limit=limit, status=status)

    def get_settings(self):
        return {
            "TRADE_AMOUNT": self.trade_amount,
            "SELL_BLOCKED": ",".join(sorted(self.sell_blocked)),
            "MAX_CONCURRENT": self.max_concurrent,
            "DAILY_LOSS_LIMIT": self.daily_loss_limit,
            "ENTRY_TIMEOUT": self.entry_timeout,
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

        if updates:
            db_save_settings(updates)
            logger.info(f"Settings updated via dashboard: {updates}")

        return {
            "ok": True,
            "TRADE_AMOUNT": self.trade_amount,
            "SELL_BLOCKED": ",".join(sorted(self.sell_blocked)),
            "MAX_CONCURRENT": self.max_concurrent,
            "DAILY_LOSS_LIMIT": self.daily_loss_limit,
            "ENTRY_TIMEOUT": self.entry_timeout,
        }


def test_template(template, sample, default_side='LONG'):
    """Test a template against sample text. Returns parsed result or error."""
    try:
        compiled, fields = compile_template(template)
    except Exception as e:
        return {"error": f"Template compile error: {e}"}

    signal = parse_with_template(sample, compiled, fields, default_side)
    if not signal:
        return {"match": False, "pattern": compiled.pattern}

    # Simulate defaults
    if "entry" not in signal:
        signal["market_order"] = True
        signal["entry"] = "(market price)"
    else:
        fill_signal_defaults(signal)

    return {"match": True, "signal": signal, "fields_found": fields, "pattern": compiled.pattern}
