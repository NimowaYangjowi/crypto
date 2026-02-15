"""OpenClaw Trader bridge — read-only access to OpenClaw's trade database for dashboard display."""

import logging
import os

from openclaw_trader.db import TradeDB

logger = logging.getLogger("openclaw_bridge")

# Default DB path: openclaw_trader/trades.db (alongside the scripts)
_DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db")


class OpenClawBridge:
    """Read-only bridge to OpenClaw's TradeDB for the unified dashboard."""

    def __init__(self, db_path=None):
        self.db_path = db_path or _DEFAULT_DB
        self._db = None

    def _get_db(self):
        if self._db is None:
            if not os.path.exists(self.db_path):
                logger.info(f"OpenClaw DB not found at {self.db_path}")
                return None
            self._db = TradeDB(self.db_path)
        return self._db

    def get_status(self):
        """Get OpenClaw trading status summary."""
        db = self._get_db()
        if not db:
            return {"enabled": False, "reason": "DB not found"}

        active = db.get_active_positions()
        daily = db.get_daily_pnl()
        exposure = db.get_total_exposure()

        return {
            "enabled": True,
            "active_positions": len(active),
            "total_exposure_usdt": round(exposure, 2),
            "daily_pnl": round(daily["realized_pnl"], 2) if daily else 0,
            "daily_trades": daily["trade_count"] if daily else 0,
            "daily_wins": daily["wins"] if daily else 0,
            "daily_losses": daily["losses"] if daily else 0,
            "loss_limit_hit": bool(daily.get("loss_limit_hit")) if daily else False,
        }

    def get_positions(self, active_only=True):
        """Get positions from OpenClaw DB."""
        db = self._get_db()
        if not db:
            return []

        if active_only:
            return db.get_active_positions()
        return db.get_position_history(limit=50)

    def get_daily_pnl(self):
        """Get today's PnL from OpenClaw DB."""
        db = self._get_db()
        if not db:
            return None
        return db.get_daily_pnl()

    def close(self):
        if self._db:
            self._db.close()
            self._db = None
