"""Web-based Telegram authentication flow for PyInstaller .app (no terminal)."""

import logging
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from core.config import AppConfig, save_env_file

logger = logging.getLogger("auth")


class TelegramAuthFlow:
    """State machine for web-based Telethon authentication.

    States: unconfigured -> need_phone -> code_sent -> need_2fa -> authenticated
    """

    def __init__(self, client: TelegramClient, config: AppConfig, data_dir: Path):
        self.client = client
        self.config = config
        self.data_dir = data_dir
        self.state = "unconfigured"
        self._phone = None
        self._phone_code_hash = None

    async def check_status(self) -> str:
        """Determine current auth state."""
        if not self.config.has_telegram_config:
            self.state = "unconfigured"
            return self.state

        await self.client.connect()
        if await self.client.is_user_authorized():
            self.state = "authenticated"
        else:
            self.state = "need_phone"
        return self.state

    async def save_config(self, data: dict) -> dict:
        """Save API credentials to .env file and reload config."""
        required = ["API_ID", "API_HASH"]
        for key in required:
            if not data.get(key):
                return {"ok": False, "error": f"{key} is required"}

        env_values = {}
        field_map = [
            "API_ID", "API_HASH", "BOT_TOKEN",
            "BINANCE_API_KEY", "BINANCE_SECRET_KEY",
            "OKX_API_KEY", "OKX_SECRET_KEY", "OKX_PASSPHRASE",
            "SOURCE_CHANNELS", "MY_CHAT_ID",
            "FORWARDING_RULES",
            "TRADE_AMOUNT", "SELL_BLOCKED", "MAX_CONCURRENT",
            "DAILY_LOSS_LIMIT", "ENTRY_TIMEOUT", "DASHBOARD_PORT",
        ]
        for key in field_map:
            if key in data and data[key]:
                env_values[key] = str(data[key])

        save_env_file(self.data_dir, env_values)

        # Update in-memory config
        self.config.api_id = int(env_values.get("API_ID", self.config.api_id))
        self.config.api_hash = env_values.get("API_HASH", self.config.api_hash)
        self.config.bot_token = env_values.get("BOT_TOKEN", self.config.bot_token)

        # Recreate client with new credentials
        session_path = str(self.data_dir / "user_session")
        self.client = TelegramClient(session_path, self.config.api_id, self.config.api_hash)

        self.state = "need_phone"
        return {"ok": True, "state": self.state, "client_updated": True}

    async def send_code(self, phone: str) -> dict:
        """Send verification code to phone number."""
        try:
            if not self.client.is_connected():
                await self.client.connect()
            result = await self.client.send_code_request(phone)
            self._phone = phone
            self._phone_code_hash = result.phone_code_hash
            self.state = "code_sent"
            return {"ok": True, "state": self.state}
        except Exception as e:
            logger.error(f"send_code error: {e}")
            return {"ok": False, "error": str(e)}

    async def verify_code(self, code: str) -> dict:
        """Verify the received code."""
        try:
            await self.client.sign_in(
                self._phone, code, phone_code_hash=self._phone_code_hash
            )
            self.state = "authenticated"
            return {"ok": True, "state": self.state}
        except SessionPasswordNeededError:
            self.state = "need_2fa"
            return {"ok": False, "need_2fa": True, "state": self.state}
        except Exception as e:
            logger.error(f"verify_code error: {e}")
            return {"ok": False, "error": str(e)}

    async def verify_2fa(self, password: str) -> dict:
        """Verify 2FA password."""
        try:
            await self.client.sign_in(password=password)
            self.state = "authenticated"
            return {"ok": True, "state": self.state}
        except Exception as e:
            logger.error(f"verify_2fa error: {e}")
            return {"ok": False, "error": str(e)}
