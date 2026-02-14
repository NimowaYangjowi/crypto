#!/usr/bin/env python3
"""가입한 텔레그램 채널/그룹 목록과 ID를 조회하는 유틸리티."""

import asyncio
from pathlib import Path
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
from core.config import load_config


async def main():
    data_dir = Path.home() / ".tgforwarder"
    if not data_dir.exists():
        data_dir = Path(__file__).parent

    config = load_config(data_dir)
    if not config.has_telegram_config:
        print("ERROR: API_ID/API_HASH가 설정되지 않았습니다.")
        return

    session_path = str(data_dir / "user_session")
    client = TelegramClient(session_path, config.api_id, config.api_hash)

    await client.start()

    print("=" * 70)
    print("  가입한 채널/그룹 목록")
    print("=" * 70)
    print(f"{'ID':<20} {'유형':<8} {'이름'}")
    print("-" * 70)

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if isinstance(entity, Channel):
            ch_type = "채널" if entity.broadcast else "그룹"
            ch_id = f"-100{entity.id}"
            username = f" (@{entity.username})" if entity.username else " (비공개)"
            print(f"{ch_id:<20} {ch_type:<8} {dialog.name}{username}")
        elif isinstance(entity, Chat):
            print(f"{entity.id:<20} {'그룹':<8} {dialog.name}")

    print("-" * 70)
    print("\n비공개 채널은 위의 ID (예: -100XXXXXXXXXX)를 채널 등록에 사용하세요.")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
