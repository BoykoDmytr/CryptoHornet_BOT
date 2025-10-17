"""Telegram command handlers for manual bot interactions."""
from __future__ import annotations

import asyncio
import logging
from datetime import timezone
from typing import Dict, Iterable, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING, Protocol

from telegram.error import TelegramError

from .exchanges import base as exchanges_base
from .state import PostedRecord, StateStore
from .telegram import TelegramClient
from .templates import ListingEvent

if TYPE_CHECKING:
    from .config import Settings as SettingsLike
else:
    class SettingsLike(Protocol):
        owner_chat_id: Optional[int]
        target_chat_id: int

log = logging.getLogger(__name__)

LATEST_EXCHANGES: Tuple[str, ...] = ("bingx", "gate", "bitget")
LATEST_MARKETS: Tuple[str, ...] = ("spot", "futures")


def _normalize_command(text: str) -> Tuple[str, Sequence[str]]:
    parts = text.strip().split()
    if not parts:
        return "", []
    command = parts[0].split("@", 1)[0].lower()
    return command, parts[1:]


def parse_simulate_listing(text: str) -> Tuple[str, str, str]:
    """Parse the /simulate_listing command payload.

    Returns a tuple of exchange, market and pair values.
    """

    command, args = _normalize_command(text)
    if command != "/simulate_listing":
        raise ValueError("Unsupported command for simulation parsing")
    if len(args) < 3:
        raise ValueError("Usage: /simulate_listing <exchange> <market> <PAIR>")
    exchange = args[0].lower()
    market = args[1].lower()
    pair = args[2].upper()
    return exchange, market, pair


def _select_latest(records: Iterable[PostedRecord]) -> Dict[Tuple[str, str], PostedRecord]:
    latest: Dict[Tuple[str, str], PostedRecord] = {}
    for record in records:
        exchange = record.exchange.lower()
        market = record.market.lower()
        key = (exchange, market)
        if exchange not in LATEST_EXCHANGES or market not in LATEST_MARKETS:
            continue
        current = latest.get(key)
        if current is None or record.posted_at > current.posted_at:
            latest[key] = record
    return latest


def format_latest_summary(latest: Mapping[Tuple[str, str], PostedRecord]) -> str:
    """Render a human-readable summary of the latest posted listings."""

    lines = ["📊 Latest listings overview"]
    for exchange in LATEST_EXCHANGES:
        lines.append(f"{exchange.upper()}:")
        for market in LATEST_MARKETS:
            key = (exchange, market)
            record = latest.get(key)
            market_name = market.upper()
            if record:
                timestamp = record.posted_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                lines.append(f"  • {market_name}: {record.pair} ({timestamp})")
            else:
                lines.append(f"  • {market_name}: no records")
    return "\n".join(lines)


def _is_authorized(chat_id: int, settings: SettingsLike) -> bool:
    if settings.owner_chat_id is not None:
        return chat_id == settings.owner_chat_id
    return chat_id == settings.target_chat_id


async def _handle_simulate(
    telegram: TelegramClient,
    chat_id: int,
    exchange: str,
    market: str,
    pair: str,
) -> None:
    event = ListingEvent(
        exchange=exchange,
        market=market,
        pair=pair,
        url=None,
        discovered_at=exchanges_base.now_utc(),
    )
    await telegram.post_listing(event)
    await telegram.bot.send_message(chat_id=chat_id, text=f"Simulated listing for {pair} posted.")


async def _handle_latest(telegram: TelegramClient, store: StateStore, chat_id: int) -> None:
    recent = await store.recent_posts()
    latest = _select_latest(recent.values())
    summary = format_latest_summary(latest)
    await telegram.bot.send_message(chat_id=chat_id, text=summary)


async def _initial_offset(telegram: TelegramClient) -> Optional[int]:
    try:
        updates = await telegram.bot.get_updates(timeout=0, limit=1, allowed_updates=["message"])
    except TelegramError as exc:
        log.warning("Failed to fetch initial updates: %s", exc)
        return None
    if updates:
        return updates[-1].update_id + 1
    return None


async def run_command_listener(settings: SettingsLike, store: StateStore, telegram: TelegramClient) -> None:
    """Long-poll the Telegram API and respond to bot commands."""

    offset = await _initial_offset(telegram)
    while True:
        try:
            updates = await telegram.bot.get_updates(
                offset=offset,
                timeout=30,
                allowed_updates=["message"],
            )
            for update in updates:
                offset = update.update_id + 1
                message = update.message or update.effective_message
                if not message or not message.text:
                    continue
                chat_id = message.chat_id
                if chat_id is None or not _is_authorized(chat_id, settings):
                    continue
                command, _ = _normalize_command(message.text)
                if command == "/ping":
                    await telegram.bot.send_message(chat_id=chat_id, text="pong")
                elif command == "/simulate_listing":
                    try:
                        exchange, market, pair = parse_simulate_listing(message.text)
                    except ValueError as exc:
                        await telegram.bot.send_message(chat_id=chat_id, text=str(exc))
                        continue
                    await _handle_simulate(telegram, chat_id, exchange, market, pair)
                elif command == "/latest":
                    await _handle_latest(telegram, store, chat_id)
                else:
                    await telegram.bot.send_message(chat_id=chat_id, text="Unknown command")
        except asyncio.CancelledError:
            raise
        except TelegramError as exc:
            log.warning("Telegram command polling error: %s", exc)
            await asyncio.sleep(5)
        except Exception as exc:  # noqa: BLE001
            log.warning("Unexpected command listener error: %s", exc)
            await asyncio.sleep(5)