"""OpenClaw Trader bridge — reads from the unified tgforwarder.db (source='openclaw')."""

import logging
import sqlite3
from datetime import datetime

from core.database import (
    DB_PATH,
    db_get_active_openclaw_trades,
    db_get_today_pnl,
    db_load_settings,
)

logger = logging.getLogger("openclaw_bridge")


class OpenClawBridge:
    """Read-only bridge to OpenClaw trades in the unified database."""

    def get_status(self):
        """Get OpenClaw trading status summary."""
        if not DB_PATH:
            return {"enabled": False, "reason": "DB not initialized"}

        active = db_get_active_openclaw_trades()
        exposure = sum(t.get("amount_usdt", 0) or 0 for t in active)
        daily = self._daily_stats()
        settings = db_load_settings()
        loss_limit = float(settings.get("DAILY_LOSS_LIMIT", "500"))
        overall_pnl = db_get_today_pnl()

        return {
            "enabled": True,
            "active_positions": len(active),
            "total_exposure_usdt": round(exposure, 2),
            "daily_pnl": round(daily["realized_pnl"], 2),
            "daily_trades": daily["trade_count"],
            "daily_wins": daily["wins"],
            "daily_losses": daily["losses"],
            "loss_limit_hit": overall_pnl <= -loss_limit,
        }

    def get_positions(self, active_only=True):
        """Get positions from the unified DB (source='openclaw')."""
        if not DB_PATH:
            return []
        if active_only:
            return db_get_active_openclaw_trades()
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM trades WHERE source='openclaw' ORDER BY id DESC LIMIT 50"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_daily_pnl(self):
        """Get today's PnL stats for openclaw trades."""
        if not DB_PATH:
            return None
        return self._daily_stats()

    def close(self):
        pass  # No persistent connection needed

    @staticmethod
    def _daily_stats():
        """Return today's openclaw-specific win/loss/pnl stats."""
        today = datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT
                     COALESCE(SUM(pnl_usdt), 0) as realized_pnl,
                     COUNT(*) as trade_count,
                     COUNT(CASE WHEN pnl_usdt > 0 THEN 1 END) as wins,
                     COUNT(CASE WHEN pnl_usdt < 0 THEN 1 END) as losses
                   FROM trades
                   WHERE source='openclaw' AND status='closed' AND closed_at LIKE ?""",
                (f"{today}%",),
            ).fetchone()
            return dict(row) if row else {
                "realized_pnl": 0, "trade_count": 0, "wins": 0, "losses": 0,
            }
