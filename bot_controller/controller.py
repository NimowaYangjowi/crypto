"""Telegram Bot API command controller — remote control via chat commands."""

import asyncio
import logging

import ccxt
import httpx

from core.config import AppConfig
from core.database import db_get_trades

logger = logging.getLogger("bot_controller")


class BotController:
    """Polls Telegram Bot API getUpdates and dispatches /commands."""

    def __init__(self, app, config: AppConfig):
        self.app = app
        self.config = config
        self._http_client = httpx.AsyncClient(timeout=35)
        self._task: asyncio.Task | None = None
        self._offset = 0
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────

    async def start(self):
        if not self.config.bot_token or not self.config.my_chat_id:
            logger.info("BotController disabled: bot_token or my_chat_id not configured.")
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("BotController started.")

    async def shutdown(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._http_client.aclose()
        logger.info("BotController stopped.")

    # ── Polling Loop ──────────────────────────────────────

    async def _poll_loop(self):
        base_url = f"https://api.telegram.org/bot{self.config.bot_token}"
        backoff = 1

        while self._running:
            try:
                resp = await self._http_client.get(
                    f"{base_url}/getUpdates",
                    params={
                        "offset": self._offset,
                        "timeout": 30,
                        "allowed_updates": '["message"]',
                    },
                    timeout=35,
                )
                data = resp.json()
                if not data.get("ok"):
                    logger.error(f"getUpdates error: {data}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                backoff = 1

                for update in data.get("result", []):
                    self._offset = update["update_id"] + 1
                    await self._handle_update(update)

            except asyncio.CancelledError:
                break
            except httpx.TimeoutException:
                continue
            except Exception as e:
                logger.error(f"Poll error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # ── Update Dispatch ───────────────────────────────────

    async def _handle_update(self, update: dict):
        message = update.get("message")
        if not message:
            return

        chat_id = message.get("chat", {}).get("id")
        if chat_id != self.config.my_chat_id:
            logger.warning(f"Ignored message from unauthorized chat: {chat_id}")
            return

        text = (message.get("text") or "").strip()
        if not text.startswith("/"):
            return

        parts = text.split()
        command = parts[0].lower()
        if "@" in command:
            command = command.split("@")[0]
        args = parts[1:]

        handler_map = {
            "/status": self._cmd_status,
            "/on": self._cmd_on,
            "/off": self._cmd_off,
            "/trades": self._cmd_trades,
            "/settings": self._cmd_settings,
            "/set": self._cmd_set,
            "/help": self._cmd_help,
        }

        handler = handler_map.get(command)
        if handler:
            try:
                await handler(args)
            except Exception as e:
                logger.error(f"Command {command} error: {e}")
                await self._reply(f"명령 처리 중 오류: {e}")
        else:
            await self._reply(
                f"알 수 없는 명령: {command}\n"
                "/help 로 사용 가능한 명령을 확인하세요."
            )

    # ── Reply Helper ──────────────────────────────────────

    async def _reply(self, text: str):
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        try:
            resp = await self._http_client.post(url, json={
                "chat_id": self.config.my_chat_id,
                "text": text,
            })
            if not resp.json().get("ok"):
                logger.error(f"Reply failed: {resp.text}")
        except Exception as e:
            logger.error(f"Failed to send reply: {e}")

    # ── Exchange Helpers ───────────────────────────────────

    def _fetch_exchange_positions(self):
        """Fetch open positions from all configured exchanges (blocking, run in executor)."""
        positions = []

        # OKX
        if self.config.okx_api_key and self.config.okx_secret_key:
            try:
                ex = ccxt.okx({
                    "apiKey": self.config.okx_api_key,
                    "secret": self.config.okx_secret_key,
                    "password": self.config.okx_passphrase,
                    "enableRateLimit": True,
                    "hostname": "www.okx.cab",
                    "options": {"defaultType": "swap"},
                })
                for p in ex.fetch_positions():
                    contracts = abs(float(p.get("contracts", 0)))
                    if contracts > 0:
                        positions.append({
                            "exchange": "OKX",
                            "symbol": p.get("symbol", "?"),
                            "side": (p.get("side") or "?").upper(),
                            "contracts": contracts,
                            "entry": p.get("entryPrice"),
                            "mark": p.get("markPrice"),
                            "pnl": p.get("unrealizedPnl"),
                            "leverage": p.get("leverage"),
                            "notional": p.get("notional"),
                            "liq": p.get("liquidationPrice"),
                        })
            except Exception as e:
                logger.error(f"OKX position fetch failed: {e}")

        # Binance Futures
        if self.config.binance_api_key and self.config.binance_secret_key:
            try:
                ex = ccxt.binance({
                    "apiKey": self.config.binance_api_key,
                    "secret": self.config.binance_secret_key,
                    "enableRateLimit": True,
                    "options": {"defaultType": "future"},
                })
                for p in ex.fetch_positions():
                    contracts = abs(float(p.get("contracts", 0)))
                    if contracts > 0:
                        positions.append({
                            "exchange": "Binance",
                            "symbol": p.get("symbol", "?"),
                            "side": (p.get("side") or "?").upper(),
                            "contracts": contracts,
                            "entry": p.get("entryPrice"),
                            "mark": p.get("markPrice"),
                            "pnl": p.get("unrealizedPnl"),
                            "leverage": p.get("leverage"),
                            "notional": p.get("notional"),
                            "liq": p.get("liquidationPrice"),
                        })
            except Exception as e:
                logger.error(f"Binance position fetch failed: {e}")

        return positions

    # ── Command Handlers ──────────────────────────────────

    async def _cmd_help(self, args):
        await self._reply(
            "사용 가능한 명령어:\n\n"
            "/status - 전체 상태 확인\n"
            "/on - 트레이딩 활성화\n"
            "/off - 트레이딩 비활성화\n"
            "/trades - 활성 거래 목록\n"
            "/settings - 현재 설정 확인\n"
            "/set <키> <값> - 설정 변경\n"
            "/help - 이 도움말\n\n"
            "설정 가능 항목:\n"
            "TRADE_AMOUNT, MAX_CONCURRENT, DAILY_LOSS_LIMIT,\n"
            "ENTRY_TIMEOUT, MAX_LEVERAGE,\n"
            "SELL_BLOCKED, TRADE_BLOCKED"
        )

    async def _cmd_status(self, args):
        lines = ["[ 시스템 상태 ]\n"]

        # Forwarder
        fwd = self.app.forwarder
        if fwd:
            fwd_status = fwd.get_status()
            icon = "ON" if fwd_status.get("enabled") else "OFF"
            lines.append(
                f"포워더: {icon}\n"
                f"  전달: {fwd_status.get('total_messages', 0)}건 | "
                f"규칙: {fwd_status.get('rule_count', 0)}개 | "
                f"가동: {fwd_status.get('uptime', '-')}"
            )
        else:
            lines.append("포워더: 미설정")

        lines.append("")

        # Trader
        trader = self.app.trader
        if trader:
            icon = "ON" if trader.enabled else "OFF"
            stats = trader.get_stats()
            lines.append(
                f"트레이더: {icon}\n"
                f"  활성 거래: {len(trader.active_trades)}건\n"
                f"  오늘 PnL: {stats.get('today_pnl', 0):.2f} USDT\n"
                f"  총 거래: {stats.get('total_trades', 0)}건\n"
                f"  승률: {stats.get('win_rate', 0):.0f}%"
            )
        else:
            lines.append("트레이더: 미설정")

        await self._reply("\n".join(lines))

    async def _cmd_on(self, args):
        trader = self.app.trader
        if not trader:
            await self._reply("트레이더 모듈이 설정되지 않았습니다.")
            return
        if trader.enabled:
            await self._reply("트레이딩이 이미 활성화 상태입니다.")
            return
        trader.enabled = True
        logger.info("Trading enabled via bot command")
        await self._reply("트레이딩 활성화됨. 새 시그널을 수신합니다.")

    async def _cmd_off(self, args):
        trader = self.app.trader
        if not trader:
            await self._reply("트레이더 모듈이 설정되지 않았습니다.")
            return
        if not trader.enabled:
            await self._reply("트레이딩이 이미 비활성화 상태입니다.")
            return
        trader.enabled = False
        active_count = len(trader.active_trades)
        logger.info("Trading disabled via bot command")
        msg = "트레이딩 비활성화됨. 새 시그널을 무시합니다."
        if active_count > 0:
            msg += f"\n(진행 중인 거래 {active_count}건은 계속 관리됩니다)"
        await self._reply(msg)

    async def _cmd_trades(self, args):
        trader = self.app.trader
        if not trader:
            await self._reply("트레이더 모듈이 설정되지 않았습니다.")
            return

        await self._reply("거래소 포지션 조회 중...")

        lines = []

        # 1. Real exchange positions (blocking call → run in executor)
        try:
            positions = await asyncio.to_thread(self._fetch_exchange_positions)
        except Exception as e:
            logger.error(f"Exchange fetch error: {e}")
            positions = []

        if positions:
            lines.append(f"[ 거래소 포지션: {len(positions)}건 ]\n")
            for p in positions:
                symbol = p["symbol"]
                side = p["side"]
                entry = p["entry"]
                mark = p["mark"]
                pnl = p["pnl"]
                lev = p["leverage"]
                notional = p["notional"]
                pnl_str = f"+{pnl:.2f}" if pnl and pnl >= 0 else f"{pnl:.2f}" if pnl else "?"
                lines.append(
                    f"  [{p['exchange']}] {symbol} {side} {lev}x\n"
                    f"    진입: {entry} | 현재: {mark}\n"
                    f"    미실현 PnL: {pnl_str} USDT\n"
                    f"    명목가: {float(notional):.2f} USDT"
                )

        # 2. In-memory active trades (bot-managed this session)
        active = trader.active_trades
        if active:
            if lines:
                lines.append("")
            lines.append(f"[ 봇 모니터링 중: {len(active)}건 ]\n")
            for key, signal in active.items():
                ticker = signal.get("ticker", "?")
                side = signal.get("side", "?")
                entry = signal.get("entry", "?")
                sl = signal.get("sl", "?")
                tp3 = signal.get("tp3", "?")
                lines.append(
                    f"  {ticker} {side}\n"
                    f"    진입: {entry} | SL: {sl} | TP3: {tp3}"
                )

        # 3. DB open/pending trades
        db_open = db_get_trades(limit=20, status="open")
        db_pending = db_get_trades(limit=20, status="pending")
        db_trades = db_open + db_pending

        if db_trades:
            if lines:
                lines.append("")
            lines.append(f"[ DB 미결: {len(db_trades)}건 ]\n")
            for t in db_trades:
                ticker = t.get("ticker", "?")
                side = t.get("side", "?")
                status = t.get("status", "?")
                filled = t.get("filled_price") or t.get("entry_price", "?")
                created = (t.get("created_at") or "")[:16]
                lines.append(f"  {ticker} {side} [{status}] @ {filled} ({created})")

        if not lines:
            await self._reply("현재 활성 거래가 없습니다.")
            return

        await self._reply("\n".join(lines))

    async def _cmd_settings(self, args):
        trader = self.app.trader
        if not trader:
            await self._reply("트레이더 모듈이 설정되지 않았습니다.")
            return

        s = trader.get_settings()
        await self._reply(
            "[ 현재 설정 ]\n\n"
            f"TRADE_AMOUNT: {s['TRADE_AMOUNT']} USDT\n"
            f"MAX_CONCURRENT: {s['MAX_CONCURRENT']}\n"
            f"DAILY_LOSS_LIMIT: {s['DAILY_LOSS_LIMIT']} USDT\n"
            f"ENTRY_TIMEOUT: {s['ENTRY_TIMEOUT']}초\n"
            f"MAX_LEVERAGE: {s['MAX_LEVERAGE']}x\n"
            f"SELL_BLOCKED: {s['SELL_BLOCKED'] or '(없음)'}\n"
            f"TRADE_BLOCKED: {s['TRADE_BLOCKED'] or '(없음)'}"
        )

    async def _cmd_set(self, args):
        trader = self.app.trader
        if not trader:
            await self._reply("트레이더 모듈이 설정되지 않았습니다.")
            return

        if len(args) < 2:
            await self._reply(
                "사용법: /set <키> <값>\n"
                "예시: /set TRADE_AMOUNT 200"
            )
            return

        key = args[0].upper()
        value = " ".join(args[1:])

        valid_keys = {
            "TRADE_AMOUNT", "SELL_BLOCKED", "TRADE_BLOCKED",
            "MAX_CONCURRENT", "DAILY_LOSS_LIMIT", "ENTRY_TIMEOUT",
            "MAX_LEVERAGE",
        }
        if key not in valid_keys:
            await self._reply(
                f"알 수 없는 설정: {key}\n"
                f"변경 가능: {', '.join(sorted(valid_keys))}"
            )
            return

        result = await trader.update_settings({key: value})
        if "error" in result:
            await self._reply(f"설정 변경 실패: {result['error']}")
        else:
            await self._reply(f"설정 변경 완료: {key} = {result.get(key, value)}")
