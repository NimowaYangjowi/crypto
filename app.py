#!/usr/bin/env python3
"""
TG Forwarder — Unified Telegram Forwarder + Trading Bot
Single process, single TelegramClient, unified dashboard.
"""

import asyncio
import logging
import sys
import webbrowser
from logging.handlers import RotatingFileHandler
from pathlib import Path

from telethon import TelegramClient

from core.config import AppConfig, load_config
from core.database import init_db
from core.auth import TelegramAuthFlow
from modules.forwarder import ForwarderModule
from modules.trader import TraderModule
from dashboard.server import DashboardServer


def get_data_dir() -> Path:
    """Get or create the user data directory."""
    data_dir = Path.home() / ".tgforwarder"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "logs").mkdir(exist_ok=True)
    return data_dir


def get_resource_dir() -> Path:
    """Get template/resource directory (handles PyInstaller bundle)."""
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS)
    return Path(__file__).parent


def setup_logging(data_dir: Path):
    """Configure logging with rotating file handler."""
    log_path = data_dir / "logs" / "app.log"
    handlers = [
        RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler(),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=handlers,
    )


class Application:
    """Main application that wires together all modules."""

    def __init__(self, config: AppConfig, data_dir: Path):
        self.config = config
        self.data_dir = data_dir
        self.client = None
        self.auth_flow = None
        self.forwarder = None
        self.trader = None
        self.dashboard = DashboardServer(self)
        self._shutdown_event = asyncio.Event()

    async def run(self):
        logger = logging.getLogger("app")

        # Initialize database
        init_db(self.data_dir)
        logger.info(f"Data directory: {self.data_dir}")

        # Create Telegram client
        session_path = str(self.data_dir / "user_session")
        self.client = TelegramClient(session_path, self.config.api_id, self.config.api_hash)

        # Create auth flow
        self.auth_flow = TelegramAuthFlow(self.client, self.config, self.data_dir)

        # Start dashboard first (needed for web-based auth)
        port = self.config.dashboard_port
        await self.dashboard.start(port)

        # Check auth status
        auth_state = await self.auth_flow.check_status()
        logger.info(f"Auth state: {auth_state}")

        # Open browser
        webbrowser.open(f"http://localhost:{port}")

        if auth_state == "authenticated":
            await self.on_authenticated()
        else:
            logger.info("Waiting for authentication via web UI...")

        # Wait for shutdown signal
        await self._shutdown_event.wait()

    async def on_authenticated(self):
        """Called after successful authentication — set up modules."""
        logger = logging.getLogger("app")
        logger.info("Authenticated. Setting up modules...")

        # Reload config in case it was updated during auth
        self.config = load_config(self.data_dir)

        # Setup forwarder
        self.forwarder = ForwarderModule(self.client, self.config)
        try:
            await self.forwarder.setup()
        except Exception as e:
            logger.error(f"Forwarder setup failed: {e}")

        # Setup trader
        self.trader = TraderModule(self.client, self.config)
        try:
            await self.trader.setup()
        except Exception as e:
            logger.error(f"Trader setup failed: {e}")

        logger.info("All modules ready. Listening for messages...")

        # Keep running via Telethon client
        asyncio.create_task(self._run_client())

    async def _run_client(self):
        """Run the Telegram client until disconnected."""
        logger = logging.getLogger("app")
        try:
            await self.client.run_until_disconnected()
        except Exception as e:
            logger.error(f"Client disconnected: {e}")
        finally:
            self._shutdown_event.set()

    async def shutdown(self):
        """Graceful shutdown."""
        logger = logging.getLogger("app")
        logger.info("Shutting down...")

        if self.trader:
            await self.trader.shutdown()

        if self.client and self.client.is_connected():
            await self.client.disconnect()

        self._shutdown_event.set()


async def main():
    data_dir = get_data_dir()
    setup_logging(data_dir)

    logger = logging.getLogger("app")
    logger.info("TG Forwarder starting...")

    # Copy project .env to data dir if none exists
    project_env = Path(__file__).parent / ".env"
    data_env = data_dir / ".env"
    if project_env.exists() and not data_env.exists():
        import shutil
        shutil.copy2(project_env, data_env)
        logger.info(f"Copied .env to {data_env}")

    config = load_config(data_dir)

    app = Application(config, data_dir)

    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
