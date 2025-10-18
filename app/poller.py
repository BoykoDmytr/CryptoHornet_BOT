import asyncio
import importlib
from typing import Callable
from sqlalchemy.ext.asyncio import AsyncSession
from telegram import Bot
from app.store import SeenItem, Metric
from app.templates import spot_message, futures_message
from app.utils.time import now_utc
from app.utils.logging import logger

async def _render_and_send(bot: Bot, listing, db: AsyncSession):
    msg_text = (
        spot_message(listing.exchange, listing.symbol, listing.source_time, listing.speed_tier, listing.source_name, listing.source_url, provisional=listing.provisional)
        if listing.market_type == "SPOT"
        else futures_message(listing.exchange, listing.symbol, listing.source_time, listing.speed_tier, listing.source_name, listing.source_url, provisional=listing.provisional)
    )
    sent = await bot.send_message(chat_id=bot._default_chat_id, text=msg_text)
    return sent

async def handle_listing(bot: Bot, db: AsyncSession, listing) -> None:
    # DB idempotency
    exists = await db.execute(
        SeenItem.__table__.select().where(SeenItem.dedupe_key == listing.dedupe_key)
    )
    row = exists.first()
    if row:
        return

    # Persist first-seen (possibly without official time)
    record = SeenItem(
        dedupe_key=listing.dedupe_key,
        exchange=listing.exchange,
        market_type=listing.market_type,
        symbol=listing.symbol,
        source_time=listing.source_time,
        provisional=listing.provisional,
        source_url=listing.source_url,
        seen_at=now_utc(),
    )
    db.add(record)
    await db.commit()

    sent = await _render_and_send(bot, listing, db)

    # Save message id for future edits
    await db.execute(
        SeenItem.__table__.update()
        .where(SeenItem.dedupe_key == listing.dedupe_key)
        .values(message_id=sent.message_id)
    )
    await db.commit()

    # Latency metric: if we do have source_time, compute; else skip
    if listing.source_time:
        latency = int((sent.date - listing.source_time).total_seconds() * 1000)
        db.add(Metric(exchange=listing.exchange, latency_ms=latency, created_at=now_utc()))
        await db.commit()
    logger.info(f"Sent {listing.exchange} {listing.market_type} {listing.symbol} msg_id={sent.message_id}")


async def run_adapter(adapter_factory: Callable, poll_seconds: float, bot: Bot, db: AsyncSession, name: str):
    """Run one exchange adapter with robust logging/backoff."""
    while True:
        try:
            logger.info(f"[ADAPTER START] {name} poll_seconds={poll_seconds}")
            adapter = adapter_factory(poll_seconds=poll_seconds)
            async for listing in adapter.stream():
                try:
                    await handle_listing(bot, db, listing)
                except Exception as e:
                    logger.exception(f"[ADAPTER HANDLE ERROR] {name} symbol={getattr(listing,'symbol', '?')}: {e}")
            # If stream ends (shouldn’t), restart after short pause
            logger.warning(f"[ADAPTER STOPPED] {name} stream ended unexpectedly; restarting in 3s")
            await asyncio.sleep(3)
        except Exception as e:
            logger.exception(f"[ADAPTER CRASH] {name}: {e}")
            await asyncio.sleep(5)  # backoff and try again


async def run_all(settings, bot: Bot, db_sessionmaker):
    tasks = []
    for ex in settings.exchanges:
        if not ex.enabled:
            logger.info(f"[ADAPTER SKIP] {ex.name} ({ex.module}) disabled")
            continue
        module = importlib.import_module(ex.module)
        adapter_factory = getattr(module, "Adapter")
        # log that we're launching
        logger.info(f"[ADAPTER LAUNCH] {ex.name} ({ex.module})")
        tasks.append(asyncio.create_task(run_adapter(adapter_factory, ex.poll_seconds, bot, db_sessionmaker(), ex.name)))
    await asyncio.gather(*tasks)