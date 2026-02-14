"""Unified aiohttp dashboard server with all API routes."""

import asyncio
import logging
import sys
from pathlib import Path
from aiohttp import web

from core.database import (
    db_get_channel_formats, db_add_channel_format,
    db_update_channel_format, db_delete_channel_format,
    db_get_trade_channels,
)
from modules.trader import test_template

logger = logging.getLogger("dashboard")


def _get_template_dir() -> Path:
    """Get template directory (handles PyInstaller bundle)."""
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS) / "templates"
    return Path(__file__).parent.parent / "templates"


class DashboardServer:
    def __init__(self, app_instance):
        self.app_instance = app_instance  # Reference to main Application
        self.runner = None

    async def start(self, port: int):
        app = web.Application()

        # Auth routes
        app.router.add_get("/api/auth/status", self._auth_status)
        app.router.add_get("/api/auth/config", self._auth_get_config)
        app.router.add_post("/api/auth/save-config", self._auth_save_config)
        app.router.add_post("/api/auth/send-code", self._auth_send_code)
        app.router.add_post("/api/auth/verify", self._auth_verify)
        app.router.add_post("/api/auth/2fa", self._auth_2fa)

        # Forwarder routes
        app.router.add_get("/api/forwarder/status", self._forwarder_status)
        app.router.add_get("/api/forwarder/rules", self._forwarder_rules)
        app.router.add_get("/api/forwarder/messages", self._forwarder_messages)

        # Trading routes
        app.router.add_get("/api/trading/stats", self._trading_stats)
        app.router.add_get("/api/trading/trades", self._trading_trades)
        app.router.add_get("/api/trading/settings", self._trading_get_settings)
        app.router.add_post("/api/trading/settings", self._trading_post_settings)
        app.router.add_post("/api/trading/simulate", self._trading_simulate)
        app.router.add_get("/api/trading/trade-channels", self._trading_trade_channels)
        app.router.add_get("/api/trading/performance", self._trading_performance)

        # Channel format routes
        app.router.add_get("/api/trading/channels", self._channels_list)
        app.router.add_post("/api/trading/channels", self._channels_add)
        app.router.add_put("/api/trading/channels/{id}", self._channels_update)
        app.router.add_delete("/api/trading/channels/{id}", self._channels_delete)
        app.router.add_post("/api/trading/channels/test", self._channels_test)

        # App routes
        app.router.add_post("/api/shutdown", self._shutdown)
        app.router.add_get("/", self._serve_index)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"Dashboard at http://localhost:{port}")

    # ── Page serving ──────────────────────────────────────

    async def _serve_index(self, request):
        template_dir = _get_template_dir()
        auth = self.app_instance.auth_flow
        if auth and auth.state != "authenticated":
            return web.FileResponse(template_dir / "auth.html")
        return web.FileResponse(template_dir / "index.html")

    # ── Auth API ──────────────────────────────────────────

    async def _auth_status(self, request):
        auth = self.app_instance.auth_flow
        if not auth:
            return web.json_response({"state": "authenticated"})
        return web.json_response({"state": auth.state})

    async def _auth_get_config(self, request):
        """Return current config so the auth form can be pre-filled."""
        cfg = self.app_instance.config

        def mask(val, visible=4):
            s = str(val)
            if not s or s == "0":
                return ""
            if len(s) <= visible:
                return s
            return s[:visible] + "*" * (len(s) - visible)

        return web.json_response({
            "API_ID": str(cfg.api_id) if cfg.api_id else "",
            "API_HASH": mask(cfg.api_hash),
            "BOT_TOKEN": mask(cfg.bot_token, 6),
            "BINANCE_API_KEY": mask(cfg.binance_api_key),
            "BINANCE_SECRET_KEY": mask(cfg.binance_secret_key),
            "OKX_API_KEY": mask(cfg.okx_api_key),
            "OKX_SECRET_KEY": mask(cfg.okx_secret_key),
            "OKX_PASSPHRASE": mask(cfg.okx_passphrase),
            "SOURCE_CHANNELS": ",".join(cfg.source_channels),
            "MY_CHAT_ID": str(cfg.my_chat_id) if cfg.my_chat_id else "",
            "FORWARDING_RULES": cfg.forwarding_rules,
            "has_telegram_config": cfg.has_telegram_config,
            "has_trading_config": cfg.has_trading_config,
        })

    async def _auth_save_config(self, request):
        auth = self.app_instance.auth_flow
        if not auth:
            return web.json_response({"error": "Auth not available"}, status=400)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        result = await auth.save_config(data)
        if result.get("client_updated"):
            self.app_instance.client = auth.client
        return web.json_response(result)

    async def _auth_send_code(self, request):
        auth = self.app_instance.auth_flow
        if not auth:
            return web.json_response({"error": "Auth not available"}, status=400)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        phone = data.get("phone", "").strip()
        if not phone:
            return web.json_response({"error": "Phone number required"}, status=400)
        result = await auth.send_code(phone)
        return web.json_response(result)

    async def _auth_verify(self, request):
        auth = self.app_instance.auth_flow
        if not auth:
            return web.json_response({"error": "Auth not available"}, status=400)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        code = data.get("code", "").strip()
        if not code:
            return web.json_response({"error": "Code required"}, status=400)
        result = await auth.verify_code(code)

        if result.get("state") == "authenticated":
            asyncio.create_task(self.app_instance.on_authenticated())

        return web.json_response(result)

    async def _auth_2fa(self, request):
        auth = self.app_instance.auth_flow
        if not auth:
            return web.json_response({"error": "Auth not available"}, status=400)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        password = data.get("password", "")
        if not password:
            return web.json_response({"error": "Password required"}, status=400)
        result = await auth.verify_2fa(password)

        if result.get("state") == "authenticated":
            asyncio.create_task(self.app_instance.on_authenticated())

        return web.json_response(result)

    # ── Forwarder API ─────────────────────────────────────

    async def _forwarder_status(self, request):
        fwd = self.app_instance.forwarder
        if not fwd:
            return web.json_response({"enabled": False})
        return web.json_response(fwd.get_status())

    async def _forwarder_rules(self, request):
        fwd = self.app_instance.forwarder
        if not fwd:
            return web.json_response({"rules": []})
        return web.json_response({"rules": fwd.get_rules()})

    async def _forwarder_messages(self, request):
        fwd = self.app_instance.forwarder
        if not fwd:
            return web.json_response({"messages": []})
        return web.json_response({"messages": fwd.get_recent_messages()})

    # ── Trading API ───────────────────────────────────────

    async def _trading_stats(self, request):
        trader = self.app_instance.trader
        if not trader:
            return web.json_response({})
        channel = request.query.get("channel")
        return web.json_response(trader.get_stats(channel=channel))

    async def _trading_trades(self, request):
        trader = self.app_instance.trader
        if not trader:
            return web.json_response({"trades": []})
        status_filter = request.query.get("status")
        channel = request.query.get("channel")
        limit = min(int(request.query.get("limit", "100")), 500)
        trades = trader.get_trades(limit=limit, status=status_filter, channel=channel)
        return web.json_response({"trades": trades})

    async def _trading_trade_channels(self, request):
        channels = db_get_trade_channels()
        return web.json_response({"channels": channels})

    async def _trading_performance(self, request):
        trader = self.app_instance.trader
        if not trader:
            return web.json_response({})
        period = request.query.get("period", "lifetime")
        if period not in ('today', '7d', '30d', 'lifetime'):
            period = 'lifetime'
        return web.json_response(trader.get_performance_table(period=period))

    async def _trading_get_settings(self, request):
        trader = self.app_instance.trader
        if not trader:
            return web.json_response({})
        return web.json_response(trader.get_settings())

    async def _trading_post_settings(self, request):
        trader = self.app_instance.trader
        if not trader:
            return web.json_response({"error": "Trader not available"}, status=400)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        result = await trader.update_settings(data)
        if "error" in result:
            return web.json_response(result, status=400)
        return web.json_response(result)

    async def _trading_simulate(self, request):
        trader = self.app_instance.trader
        if not trader:
            return web.json_response({"error": "Trader not available"}, status=400)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        text = str(data.get("text", "")).strip()
        if not text:
            return web.json_response({"error": "Message text required"}, status=400)

        result = await trader.simulate_signal(text)
        if "error" in result:
            return web.json_response(result, status=400)
        return web.json_response(result)

    # ── Channel Format API ─────────────────────────────

    async def _channels_list(self, request):
        channels = db_get_channel_formats()
        return web.json_response({"channels": channels})

    async def _channels_add(self, request):
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        channel_id = str(data.get("channel_id", "")).strip()
        template = str(data.get("template", "")).strip()
        if not channel_id or not template:
            return web.json_response({"error": "channel_id and template are required"}, status=400)

        channel_name = str(data.get("channel_name", "")).strip()
        default_side = str(data.get("default_side", "LONG")).upper()
        if default_side not in ("LONG", "SHORT"):
            default_side = "LONG"

        # Validate template compiles
        result = test_template(template, "test", default_side)
        if "error" in result:
            return web.json_response(result, status=400)

        trade_amount = 0
        try:
            trade_amount = float(data.get("trade_amount", 0))
            if trade_amount < 0:
                trade_amount = 0
        except (ValueError, TypeError):
            pass

        exchange = str(data.get("exchange", "binance")).lower()
        if exchange not in ("binance", "okx"):
            exchange = "binance"

        noise_filter = str(data.get("noise_filter", "")).strip()

        fmt_id = db_add_channel_format(channel_id, channel_name, template, default_side, trade_amount, exchange, noise_filter)
        return web.json_response({"ok": True, "id": fmt_id})

    async def _channels_update(self, request):
        fmt_id = int(request.match_info["id"])
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        updates = {}
        if "channel_id" in data:
            updates["channel_id"] = str(data["channel_id"]).strip()
        if "channel_name" in data:
            updates["channel_name"] = str(data["channel_name"]).strip()
        if "template" in data:
            template = str(data["template"]).strip()
            result = test_template(template, "test", data.get("default_side", "LONG"))
            if "error" in result:
                return web.json_response(result, status=400)
            updates["template"] = template
        if "default_side" in data:
            side = str(data["default_side"]).upper()
            updates["default_side"] = side if side in ("LONG", "SHORT") else "LONG"
        if "exchange" in data:
            ex = str(data["exchange"]).lower()
            updates["exchange"] = ex if ex in ("binance", "okx") else "binance"
        if "enabled" in data:
            updates["enabled"] = 1 if data["enabled"] else 0
        if "trade_amount" in data:
            try:
                val = float(data["trade_amount"])
                updates["trade_amount"] = val if val >= 0 else 0
            except (ValueError, TypeError):
                pass
        if "noise_filter" in data:
            updates["noise_filter"] = str(data["noise_filter"]).strip()

        if updates:
            db_update_channel_format(fmt_id, **updates)

        return web.json_response({"ok": True})

    async def _channels_delete(self, request):
        fmt_id = int(request.match_info["id"])
        db_delete_channel_format(fmt_id)
        return web.json_response({"ok": True})

    async def _channels_test(self, request):
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        template = str(data.get("template", "")).strip()
        sample = str(data.get("sample", "")).strip()
        default_side = str(data.get("default_side", "LONG")).upper()

        if not template or not sample:
            return web.json_response({"error": "template and sample required"}, status=400)

        result = test_template(template, sample, default_side)
        return web.json_response(result)

    # ── App API ───────────────────────────────────────────

    async def _shutdown(self, request):
        logger.info("Shutdown requested via dashboard")
        asyncio.create_task(self.app_instance.shutdown())
        return web.json_response({"status": "shutting_down"})
