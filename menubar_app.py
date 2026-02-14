#!/usr/bin/env python3
"""
TG Forwarder — macOS Menu Bar Application
Manages the trading bot and provides quick access to the dashboard.
"""

import os
import sys
import threading
import asyncio
import logging
import webbrowser
import urllib.request

# Prevent the bot from auto-opening browser (menu bar handles it)
os.environ["TGF_MENUBAR"] = "1"

import rumps

logger = logging.getLogger("menubar")


class TGForwarderMenuBar(rumps.App):
    """macOS menu bar app that manages the TG Forwarder trading bot."""

    def __init__(self):
        super().__init__("TG Forwarder", title="📈⏸", quit_button=None)

        self.bot_thread = None
        self.bot_loop = None
        self.bot_app = None
        self._bot_running = False
        self.port = 8080

        # Menu items
        self.status_item = rumps.MenuItem("○  Stopped")
        self.toggle_item = rumps.MenuItem("Start Bot", callback=self.toggle_bot)
        self.dashboard_item = rumps.MenuItem(
            "Open Dashboard", callback=self.open_dashboard
        )
        self.quit_item = rumps.MenuItem("Quit", callback=self.on_quit)

        self.menu = [
            self.status_item,
            None,
            self.toggle_item,
            self.dashboard_item,
            None,
            self.quit_item,
        ]

        # Disable non-clickable status item
        self.status_item.set_callback(None)

        # Health check timer (runs on main thread)
        self.health_timer = rumps.Timer(self.check_health, 3)
        self.health_timer.start()

        # Auto-start bot on launch
        self.start_bot()

    # ── Bot lifecycle ────────────────────────────────────

    def start_bot(self):
        """Start the trading bot in a background thread."""
        if self._bot_running:
            return

        self._bot_running = True
        self.title = "📈"
        self.status_item.title = "●  Running"
        self.toggle_item.title = "Stop Bot"

        self.bot_thread = threading.Thread(target=self._bot_main, daemon=True)
        self.bot_thread.start()

        # Wait for server, then open dashboard
        threading.Thread(target=self._wait_and_open_dashboard, daemon=True).start()

        rumps.notification(
            title="TG Forwarder",
            subtitle="Bot Started",
            message="Trading bot is now running.",
            sound=False,
        )

    def stop_bot(self):
        """Stop the trading bot gracefully."""
        if not self._bot_running:
            return

        # Signal the bot to shut down
        if self.bot_app and self.bot_loop and self.bot_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self.bot_app.shutdown(), self.bot_loop
            )
            try:
                future.result(timeout=10)
            except Exception:
                pass

        self._bot_running = False
        self.bot_app = None
        self.bot_loop = None
        self.title = "📈⏸"
        self.status_item.title = "○  Stopped"
        self.toggle_item.title = "Start Bot"

        rumps.notification(
            title="TG Forwarder",
            subtitle="Bot Stopped",
            message="Trading bot has been stopped.",
            sound=False,
        )

    def _bot_main(self):
        """Bot thread entry point — runs the asyncio event loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.bot_loop = loop

        try:
            loop.run_until_complete(self._bot_run())
        except Exception as e:
            logger.error(f"Bot error: {e}")
        finally:
            # Clean up aiohttp server
            if self.bot_app and self.bot_app.dashboard and self.bot_app.dashboard.runner:
                try:
                    loop.run_until_complete(self.bot_app.dashboard.runner.cleanup())
                except Exception:
                    pass
            try:
                loop.close()
            except Exception:
                pass
            self.bot_loop = None
            self._bot_running = False

    async def _bot_run(self):
        """Initialize and run the trading bot."""
        from app import Application, get_data_dir, setup_logging
        from core.config import load_config

        data_dir = get_data_dir()
        setup_logging(data_dir)
        config = load_config(data_dir)
        self.port = config.dashboard_port

        self.bot_app = Application(config, data_dir)
        await self.bot_app.run()

    def _wait_and_open_dashboard(self):
        """Wait for the dashboard server to be ready, then open browser."""
        import time

        for _ in range(30):  # Wait up to 15 seconds
            time.sleep(0.5)
            if not self._bot_running:
                return
            try:
                urllib.request.urlopen(f"http://localhost:{self.port}", timeout=1)
                webbrowser.open(f"http://localhost:{self.port}")
                return
            except Exception:
                pass

    # ── Menu callbacks ───────────────────────────────────

    def toggle_bot(self, _):
        """Start or stop the bot."""
        if self._bot_running:
            self.stop_bot()
        else:
            self.start_bot()

    def open_dashboard(self, _):
        """Open the web dashboard in the default browser."""
        if self._bot_running:
            webbrowser.open(f"http://localhost:{self.port}")
        else:
            rumps.notification(
                title="TG Forwarder",
                subtitle="Bot Not Running",
                message="Start the bot first to access the dashboard.",
                sound=False,
            )

    def on_quit(self, _):
        """Clean up and quit the application."""
        self.stop_bot()
        rumps.quit_application()

    # ── Health monitoring ────────────────────────────────

    def check_health(self, _):
        """Periodic health check (runs on main thread, safe for UI updates)."""
        if self._bot_running and (
            not self.bot_thread or not self.bot_thread.is_alive()
        ):
            self._bot_running = False
            self.bot_app = None
            self.bot_loop = None
            self.title = "📈⚠"
            self.status_item.title = "⚠  Error — Bot Crashed"
            self.toggle_item.title = "Restart Bot"


if __name__ == "__main__":
    TGForwarderMenuBar().run()
