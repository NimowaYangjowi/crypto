import asyncio
import logging
import os
import re
import json
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils as tl_utils
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from aiohttp import web

# Load environment variables
load_dotenv()


def setup_logging(disable_console=False):
    """Configure logging based on console preference."""
    handlers = [logging.FileHandler('telegram_forwarder.log')]
    if not disable_console:
        handlers.insert(0, logging.StreamHandler())
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers,
    )


logger = logging.getLogger(__name__)


class TelegramForwarder:
    def __init__(self, remove_forward_signature=False, dashboard_port=8080):
        """Initialize the Telegram forwarder with environment variables."""
        self.api_id = os.getenv('API_ID')
        self.api_hash = os.getenv('API_HASH')
        self.bot_token = os.getenv('BOT_TOKEN')
        self.remove_forward_signature = remove_forward_signature
        self.dashboard_port = dashboard_port

        # Check for legacy single source/target configuration
        self.source_id = os.getenv('SOURCE_ID')
        self.target_id = os.getenv('TARGET_ID')
        self.forwarding_rules = os.getenv('FORWARDING_RULES')

        # Validate required environment variables
        if not all([self.api_id, self.api_hash]):
            raise ValueError("Missing API_ID or API_HASH. Check your .env file.")

        # Parse forwarding configuration
        self.forwarding_map = self._parse_forwarding_rules()

        if not self.forwarding_map:
            raise ValueError("No forwarding rules configured. Set either SOURCE_ID/TARGET_ID or FORWARDING_RULES.")

        # Initialize Telegram client
        if self.bot_token:
            self.client = TelegramClient('bot_session', self.api_id, self.api_hash)
            logger.info("Initialized in bot mode")
        else:
            self.client = TelegramClient('user_session', self.api_id, self.api_hash)
            logger.info("Initialized in user mode")

        # Dashboard state
        self.start_time = None
        self.is_connected = False
        self.entity_cache = {}
        self.message_history = []
        self.max_history = 200
        self.total_messages = 0

    @staticmethod
    def _parse_entity_id(value):
        """Parse an entity ID: numeric ID (int) or username (str)."""
        value = value.strip()
        # Strip t.me URL prefix if present
        for prefix in ('https://t.me/', 'http://t.me/', 't.me/'):
            if value.lower().startswith(prefix):
                value = value[len(prefix):]
                break
        try:
            return int(value)
        except ValueError:
            return value  # username string

    def _parse_forwarding_rules(self):
        """Parse forwarding rules from environment variables."""
        forwarding_map = {}

        # Check for legacy single source/target configuration
        if self.source_id and self.target_id:
            source_id = self._parse_entity_id(self.source_id)
            target_id = self._parse_entity_id(self.target_id)
            forwarding_map[source_id] = [target_id]
            logger.info("Using legacy single source/target configuration")
            return forwarding_map

        # Parse new multiple forwarding rules
        if self.forwarding_rules:
            try:
                rules = self.forwarding_rules.split(',')
                for rule in rules:
                    rule = rule.strip()
                    if not rule:
                        continue

                    # Strip URL prefixes before splitting by ':' to avoid conflicts
                    rule = re.sub(r'https?://t\.me/', '', rule)

                    parts = rule.split(':')
                    if len(parts) < 2:
                        raise ValueError(f"Invalid forwarding rule format: {rule}")

                    source_id = self._parse_entity_id(parts[0])
                    target_ids = [self._parse_entity_id(t) for t in parts[1:]]

                    if source_id in forwarding_map:
                        forwarding_map[source_id].extend(target_ids)
                    else:
                        forwarding_map[source_id] = target_ids

                logger.info(f"Parsed {len(forwarding_map)} forwarding rules")
                return forwarding_map

            except ValueError as e:
                raise ValueError(f"Error parsing FORWARDING_RULES: {e}")

        return {}

    # ── Telegram client ──────────────────────────────────────

    async def start_client(self):
        """Start the Telegram client and handle authentication."""
        await self.client.start(bot_token=self.bot_token if self.bot_token else None)

        if not self.bot_token:
            if not await self.client.is_user_authorized():
                phone = input("Enter your phone number: ")
                await self.client.send_code_request(phone)
                code = input("Enter the code you received: ")
                try:
                    await self.client.sign_in(phone, code)
                except SessionPasswordNeededError:
                    password = input("Enter your 2FA password: ")
                    await self.client.sign_in(password=password)

        logger.info("Client started successfully")

    async def _resolve_forwarding_map(self):
        """Resolve all usernames/IDs to numeric peer IDs for event matching."""
        resolved_map = {}

        for source, targets in self.forwarding_map.items():
            # Resolve source
            source_entity = await self.client.get_entity(source)
            source_pid = tl_utils.get_peer_id(source_entity)

            name = getattr(source_entity, 'title', None) or getattr(source_entity, 'first_name', str(source))
            username = getattr(source_entity, 'username', None)
            self.entity_cache[source_pid] = {
                'name': name,
                'username': f'@{username}' if username else str(source),
            }

            # Resolve targets
            resolved_targets = []
            for target in targets:
                target_entity = await self.client.get_entity(target)
                target_pid = tl_utils.get_peer_id(target_entity)

                t_name = getattr(target_entity, 'title', None) or getattr(target_entity, 'first_name', str(target))
                t_username = getattr(target_entity, 'username', None)
                self.entity_cache[target_pid] = {
                    'name': t_name,
                    'username': f'@{t_username}' if t_username else str(target),
                }
                resolved_targets.append(target_pid)

            if source_pid in resolved_map:
                resolved_map[source_pid].extend(resolved_targets)
            else:
                resolved_map[source_pid] = resolved_targets

        self.forwarding_map = resolved_map
        logger.info(f"Resolved {len(self.forwarding_map)} forwarding rules to numeric IDs")

    async def setup_forwarding(self):
        """Set up message forwarding from multiple sources to their respective targets."""
        await self._resolve_forwarding_map()

        # Log resolved rules
        logger.info("Forwarding rules:")
        for source_id, target_ids in self.forwarding_map.items():
            s = self.entity_cache[source_id]
            ts = [self.entity_cache[tid] for tid in target_ids]
            logger.info(f"  {s['name']} ({s['username']}) -> {', '.join(t['name'] for t in ts)}")

        source_ids = list(self.forwarding_map.keys())

        @self.client.on(events.NewMessage(chats=source_ids))
        async def forward_handler(event):
            """Handle new messages and forward them to configured targets."""
            try:
                message = event.message
                source_id = event.chat_id

                target_ids = self.forwarding_map.get(source_id, [])
                if not target_ids:
                    return

                source_name = self.entity_cache.get(source_id, {}).get('name', str(source_id))

                for target_id in target_ids:
                    target_name = self.entity_cache.get(target_id, {}).get('name', str(target_id))
                    try:
                        if self.remove_forward_signature:
                            await self.client.send_message(
                                entity=target_id,
                                message=message.message,
                                file=message.media,
                                parse_mode='html' if message.entities else None,
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
                        self._add_message(source_name, target_name, message.message, 'error')

            except FloodWaitError as e:
                logger.warning(f"Rate limited. Waiting {e.seconds}s...")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"Error in forward handler: {e}")

        logger.info("Message forwarding handlers registered")

    def _add_message(self, source, target, text, status='success'):
        """Track a forwarded message for the dashboard."""
        preview = ''
        if text:
            preview = text[:80] + ('...' if len(text) > 80 else '')
        else:
            preview = '[media]'

        self.message_history.append({
            'time': datetime.now().strftime('%H:%M:%S'),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'source': source,
            'target': target,
            'preview': preview,
            'status': status,
        })
        if len(self.message_history) > self.max_history:
            self.message_history.pop(0)
        self.total_messages += 1

    # ── Web dashboard ────────────────────────────────────────

    async def _serve_dashboard(self, request):
        html_path = Path(__file__).parent / 'templates' / 'dashboard.html'
        return web.FileResponse(html_path)

    async def _api_status(self, request):
        uptime = ''
        if self.start_time:
            delta = datetime.now() - self.start_time
            total_sec = int(delta.total_seconds())
            d, rem = divmod(total_sec, 86400)
            h, rem = divmod(rem, 3600)
            m, s = divmod(rem, 60)
            if d > 0:
                uptime = f'{d}d {h}h {m}m'
            elif h > 0:
                uptime = f'{h}h {m}m'
            else:
                uptime = f'{m}m {s}s'

        all_targets = set()
        for tids in self.forwarding_map.values():
            all_targets.update(tids)

        return web.json_response({
            'connected': self.is_connected,
            'uptime': uptime,
            'total_messages': self.total_messages,
            'rule_count': len(self.forwarding_map),
            'source_count': len(self.forwarding_map),
            'target_count': len(all_targets),
        })

    async def _api_rules(self, request):
        rules = []
        for source_id, target_ids in self.forwarding_map.items():
            rules.append({
                'source': self.entity_cache.get(source_id, {'name': str(source_id), 'username': str(source_id)}),
                'targets': [
                    self.entity_cache.get(tid, {'name': str(tid), 'username': str(tid)})
                    for tid in target_ids
                ],
            })
        return web.json_response({'rules': rules})

    async def _api_messages(self, request):
        return web.json_response({
            'messages': list(reversed(self.message_history[-50:]))
        })

    async def _api_shutdown(self, request):
        logger.info("Shutdown requested via dashboard")
        self.is_connected = False
        loop = asyncio.get_event_loop()
        loop.call_later(1, lambda: asyncio.ensure_future(self.client.disconnect()))
        return web.json_response({'status': 'shutting_down'})

    async def _start_dashboard(self):
        app = web.Application()
        app.router.add_get('/', self._serve_dashboard)
        app.router.add_get('/api/status', self._api_status)
        app.router.add_get('/api/rules', self._api_rules)
        app.router.add_get('/api/messages', self._api_messages)
        app.router.add_post('/api/shutdown', self._api_shutdown)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.dashboard_port)
        await site.start()
        logger.info(f"Dashboard running at http://localhost:{self.dashboard_port}")

    # ── Main entry ───────────────────────────────────────────

    async def run(self):
        """Main method to run the forwarder."""
        try:
            await self.start_client()
            self.is_connected = True
            self.start_time = datetime.now()

            await self.setup_forwarding()
            await self._start_dashboard()

            logger.info("Telegram forwarder is now running. Press Ctrl+C to stop.")
            await self.client.run_until_disconnected()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.is_connected = False
            await self.client.disconnect()
            logger.info("Client disconnected")


async def main():
    """Main function to run the application."""
    parser = argparse.ArgumentParser(description='Telegram Message Forwarder')
    parser.add_argument('--remove-forward-signature', '-r', action='store_true',
                        help='Remove "Forward from..." signature by sending as new messages')
    parser.add_argument('--disable-console-log', '-q', action='store_true',
                        help='Disable console logging (only log to file)')
    parser.add_argument('--port', '-p', type=int, default=8080,
                        help='Dashboard web server port (default: 8080)')

    args = parser.parse_args()
    setup_logging(disable_console=args.disable_console_log)

    try:
        forwarder = TelegramForwarder(
            remove_forward_signature=args.remove_forward_signature,
            dashboard_port=args.port,
        )
        await forwarder.run()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print("\nPlease check your .env file and ensure all required variables are set.")
        print("You can use .env.example as a template.")
    except Exception as e:
        logger.error(f"Application error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
