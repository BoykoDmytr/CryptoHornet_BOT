"""Minimal Telegram client wrapper."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from telegram import Bot
from telegram.constants import ParseMode

from .templates import ListingEvent, format_listing

log = logging.getLogger(__name__)


class TelegramClient:
    """Async context manager for posting updates to Telegram."""

    def __init__(self, token: str, chat_id: int, owner_chat_id: Optional[int] = None) -> None:
        self._bot = Bot(token=token)
        self._chat_id = chat_id
        self._owner_chat_id = owner_chat_id
        self._lock = asyncio.Lock()

    @property
    def bot(self) -> Bot:
        """Expose the underlying Bot instance for auxiliary operations."""
        return self._bot
    
    async def __aenter__(self) -> "TelegramClient":
        await self._bot.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        await self._bot.shutdown()

    async def post_listing(self, event: ListingEvent) -> Optional[int]:
        text = format_listing(event)
        async with self._lock:
            try:
                message = await self._bot.send_message(
                    chat_id=self._chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return message.message_id
            except Exception as exc:  # noqa: BLE001
                log.warning("failed to post listing to chat %s: %s", self._chat_id, exc)
                if self._owner_chat_id and self._owner_chat_id != self._chat_id:
                    await self._safe_notify_owner(text)
                return None

    async def send_text(self, text: str) -> None:
        async with self._lock:
            try:
                await self._bot.send_message(chat_id=self._chat_id, text=text, disable_web_page_preview=True)
            except Exception as exc:  # noqa: BLE001
                log.warning("failed to send text message: %s", exc)

    async def _safe_notify_owner(self, text: str) -> None:
        if not self._owner_chat_id:
            return
        try:
            await self._bot.send_message(chat_id=self._owner_chat_id, text=f"❗️Failed to post listing:\n{text}")
        except Exception:  # noqa: BLE001
            pass