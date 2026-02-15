"""Centralized configuration for the unified TG Forwarder + Trading Bot app."""

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv


@dataclass
class AppConfig:
    # Telegram
    api_id: int = 0
    api_hash: str = ""
    bot_token: str = ""

    # Forwarder
    forwarding_rules: str = ""
    source_id: str = ""
    target_id: str = ""
    remove_forward_signature: bool = False

    # Trading — Binance
    binance_api_key: str = ""
    binance_secret_key: str = ""
    # Trading — OKX
    okx_api_key: str = ""
    okx_secret_key: str = ""
    okx_passphrase: str = ""
    source_channels: list = field(default_factory=list)
    my_chat_id: int = 0
    trade_amount: float = 100.0
    sell_blocked: set = field(default_factory=set)
    trade_blocked: set = field(default_factory=set)
    max_concurrent: int = 3
    daily_loss_limit: float = 500.0
    entry_timeout: int = 600
    max_leverage: int = 20

    # App
    dashboard_port: int = 8080

    @property
    def has_telegram_config(self):
        return bool(self.api_id and self.api_hash)

    @property
    def has_forwarder_config(self):
        return bool(self.forwarding_rules or (self.source_id and self.target_id))

    @property
    def has_trading_config(self):
        has_binance = bool(self.binance_api_key and self.binance_secret_key)
        has_okx = bool(self.okx_api_key and self.okx_secret_key and self.okx_passphrase)
        return (has_binance or has_okx) and bool(self.source_channels)


def load_config(data_dir: Path) -> AppConfig:
    """Load configuration from .env file in data_dir, then fall back to project .env."""
    env_path = data_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        load_dotenv(override=True)

    source_channels_raw = os.getenv("SOURCE_CHANNELS", "")
    sell_blocked_raw = os.getenv("SELL_BLOCKED", "")
    trade_blocked_raw = os.getenv("TRADE_BLOCKED", "")

    return AppConfig(
        api_id=int(os.getenv("API_ID", "0")),
        api_hash=os.getenv("API_HASH", ""),
        bot_token=os.getenv("BOT_TOKEN", ""),
        forwarding_rules=os.getenv("FORWARDING_RULES", ""),
        source_id=os.getenv("SOURCE_ID", ""),
        target_id=os.getenv("TARGET_ID", ""),
        remove_forward_signature=os.getenv("REMOVE_FORWARD_SIGNATURE", "").lower() in ("1", "true", "yes"),
        binance_api_key=os.getenv("BINANCE_API_KEY", ""),
        binance_secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
        okx_api_key=os.getenv("OKX_API_KEY", ""),
        okx_secret_key=os.getenv("OKX_SECRET_KEY", ""),
        okx_passphrase=os.getenv("OKX_PASSPHRASE", ""),
        source_channels=[c.strip() for c in source_channels_raw.split(",") if c.strip()],
        my_chat_id=int(os.getenv("MY_CHAT_ID", "0")),
        trade_amount=float(os.getenv("TRADE_AMOUNT", "100")),
        sell_blocked={s.strip().upper() for s in sell_blocked_raw.split(",") if s.strip()},
        trade_blocked={s.strip().upper() for s in trade_blocked_raw.split(",") if s.strip()},
        max_concurrent=int(os.getenv("MAX_CONCURRENT", "3")),
        daily_loss_limit=float(os.getenv("DAILY_LOSS_LIMIT", "500")),
        entry_timeout=int(os.getenv("ENTRY_TIMEOUT", "600")),
        max_leverage=int(os.getenv("MAX_LEVERAGE", "20")),
        dashboard_port=int(os.getenv("DASHBOARD_PORT", "8080")),
    )


def save_env_file(data_dir: Path, values: dict):
    """Write/update a .env file in data_dir with the given key-value pairs."""
    env_path = data_dir / ".env"
    existing = {}

    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()

    existing.update(values)

    lines = []
    for k, v in existing.items():
        lines.append(f"{k}={v}")

    env_path.write_text("\n".join(lines) + "\n")
