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


async def run_adapter(adapter_factory: Callable, poll_seconds: float, bot: Bot, db: AsyncSession):
    adapter = adapter_factory(poll_seconds=poll_seconds)
    async for listing in adapter.stream():
        await handle_listing(bot, db, listing)


async def run_all(settings, bot: Bot, db_sessionmaker):
    tasks = []
    for ex in settings.exchanges:
        if not ex.enabled:
            continue
        module = importlib.import_module(ex.module)
        adapter_factory = getattr(module, "Adapter")
        tasks.append(asyncio.create_task(run_adapter(adapter_factory, ex.poll_seconds, bot, db_sessionmaker())))
    await asyncio.gather(*tasks)