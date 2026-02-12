"""Forwarder module — monitors Telegram channels and forwards messages."""

import asyncio
import logging
import re
from datetime import datetime
from telethon import TelegramClient, events, utils as tl_utils
from telethon.errors import FloodWaitError

from core.config import AppConfig
from core.database import db_insert_forwarded_message, db_get_forwarded_messages, db_get_forwarded_count

logger = logging.getLogger("forwarder")


class ForwarderModule:
    def __init__(self, client: TelegramClient, config: AppConfig):
        self.client = client
        self.config = config
        self.enabled = False
        self.start_time = None

        # State
        self.forwarding_map = {}
        self.entity_cache = {}
        self.message_history = []
        self.max_history = 200
        self.total_messages = 0

    @staticmethod
    def _parse_entity_id(value):
        value = value.strip()
        for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
            if value.lower().startswith(prefix):
                value = value[len(prefix):]
                break
        try:
            return int(value)
        except ValueError:
            return value

    def _parse_forwarding_rules(self):
        forwarding_map = {}

        if self.config.source_id and self.config.target_id:
            source_id = self._parse_entity_id(self.config.source_id)
            target_id = self._parse_entity_id(self.config.target_id)
            forwarding_map[source_id] = [target_id]
            logger.info("Using legacy single source/target configuration")
            return forwarding_map

        if self.config.forwarding_rules:
            rules_str = self.config.forwarding_rules
            rules = rules_str.split(",")
            for rule in rules:
                rule = rule.strip()
                if not rule:
                    continue
                rule = re.sub(r"https?://t\.me/", "", rule)
                parts = rule.split(":")
                if len(parts) < 2:
                    logger.warning(f"Invalid forwarding rule: {rule}")
                    continue
                source_id = self._parse_entity_id(parts[0])
                target_ids = [self._parse_entity_id(t) for t in parts[1:]]
                if source_id in forwarding_map:
                    forwarding_map[source_id].extend(target_ids)
                else:
                    forwarding_map[source_id] = target_ids

            logger.info(f"Parsed {len(forwarding_map)} forwarding rules")
            return forwarding_map

        return {}

    async def _resolve_forwarding_map(self):
        resolved_map = {}
        for source, targets in self.forwarding_map.items():
            source_entity = await self.client.get_entity(source)
            source_pid = tl_utils.get_peer_id(source_entity)
            name = getattr(source_entity, "title", None) or getattr(source_entity, "first_name", str(source))
            username = getattr(source_entity, "username", None)
            self.entity_cache[source_pid] = {
                "name": name,
                "username": f"@{username}" if username else str(source),
            }

            resolved_targets = []
            for target in targets:
                target_entity = await self.client.get_entity(target)
                target_pid = tl_utils.get_peer_id(target_entity)
                t_name = getattr(target_entity, "title", None) or getattr(target_entity, "first_name", str(target))
                t_username = getattr(target_entity, "username", None)
                self.entity_cache[target_pid] = {
                    "name": t_name,
                    "username": f"@{t_username}" if t_username else str(target),
                }
                resolved_targets.append(target_pid)

            if source_pid in resolved_map:
                resolved_map[source_pid].extend(resolved_targets)
            else:
                resolved_map[source_pid] = resolved_targets

        self.forwarding_map = resolved_map
        logger.info(f"Resolved {len(self.forwarding_map)} forwarding rules to numeric IDs")

    async def setup(self):
        """Parse rules, resolve entities, register event handler."""
        self.forwarding_map = self._parse_forwarding_rules()
        if not self.forwarding_map:
            logger.info("No forwarding rules configured. Forwarder disabled.")
            return

        await self._resolve_forwarding_map()

        for source_id, target_ids in self.forwarding_map.items():
            s = self.entity_cache[source_id]
            ts = [self.entity_cache[tid] for tid in target_ids]
            logger.info(f"  {s['name']} ({s['username']}) -> {', '.join(t['name'] for t in ts)}")

        # Load persisted message count
        self.total_messages = db_get_forwarded_count()

        # Load recent messages from DB into memory cache
        recent = db_get_forwarded_messages(limit=self.max_history)
        for msg in reversed(recent):
            t = msg["created_at"] or ""
            time_str = t[11:19] if len(t) > 19 else t
            date_str = t[:10] if len(t) >= 10 else t
            self.message_history.append({
                "time": time_str,
                "date": date_str,
                "source": msg["source_name"] or "",
                "target": msg["target_name"] or "",
                "preview": msg["preview"] or "",
                "status": msg["status"] or "success",
            })

        source_ids = list(self.forwarding_map.keys())
        remove_sig = self.config.remove_forward_signature

        @self.client.on(events.NewMessage(chats=source_ids))
        async def forward_handler(event):
            try:
                message = event.message
                source_id = event.chat_id
                target_ids = self.forwarding_map.get(source_id, [])
                if not target_ids:
                    return

                source_name = self.entity_cache.get(source_id, {}).get("name", str(source_id))

                for target_id in target_ids:
                    target_name = self.entity_cache.get(target_id, {}).get("name", str(target_id))
                    try:
                        if remove_sig:
                            await self.client.send_message(
                                entity=target_id,
                                message=message.message,
                                file=message.media,
                                parse_mode="html" if message.entities else None,
                            )
                        else:
                            await self.client.forward_messages(
                                entity=target_id,
                                messages=message.id,
                                from_peer=source_id,
                            )
                        logger.info(f"Forwarded: {source_name} -> {target_name}")
                        self._add_message(source_name, target_name, message.message)
                    except Exception as e:
                        logger.error(f"Error forwarding to {target_name}: {e}")
                        self._add_message(source_name, target_name, message.message, "error")

            except FloodWaitError as e:
                logger.warning(f"Rate limited. Waiting {e.seconds}s...")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"Error in forward handler: {e}")

        self.enabled = True
        self.start_time = datetime.now()
        logger.info("Forwarder module ready")

    def _add_message(self, source, target, text, status="success"):
        preview = ""
        if text:
            preview = text[:80] + ("..." if len(text) > 80 else "")
        else:
            preview = "[media]"

        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "source": source,
            "target": target,
            "preview": preview,
            "status": status,
        }
        self.message_history.append(entry)
        if len(self.message_history) > self.max_history:
            self.message_history.pop(0)
        self.total_messages += 1

        # Persist to DB
        db_insert_forwarded_message(source, target, preview, status)

    # ── Public API for dashboard ──────────────────────────

    def get_status(self):
        uptime = ""
        if self.start_time:
            delta = datetime.now() - self.start_time
            total_sec = int(delta.total_seconds())
            d, rem = divmod(total_sec, 86400)
            h, rem = divmod(rem, 3600)
            m, s = divmod(rem, 60)
            if d > 0:
                uptime = f"{d}d {h}h {m}m"
            elif h > 0:
                uptime = f"{h}h {m}m"
            else:
                uptime = f"{m}m {s}s"

        all_targets = set()
        for tids in self.forwarding_map.values():
            all_targets.update(tids)

        return {
            "enabled": self.enabled,
            "uptime": uptime,
            "total_messages": self.total_messages,
            "rule_count": len(self.forwarding_map),
            "source_count": len(self.forwarding_map),
            "target_count": len(all_targets),
        }

    def get_rules(self):
        rules = []
        for source_id, target_ids in self.forwarding_map.items():
            rules.append({
                "source": self.entity_cache.get(source_id, {"name": str(source_id), "username": str(source_id)}),
                "targets": [
                    self.entity_cache.get(tid, {"name": str(tid), "username": str(tid)})
                    for tid in target_ids
                ],
            })
        return rules

    def get_recent_messages(self, limit=50):
        return list(reversed(self.message_history[-limit:]))
