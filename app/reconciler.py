# app/reconciler.py
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from telegram import Bot
from app.store import SeenItem
from app.templates import spot_message, futures_message
from app.utils.logging import logger

async def reconcile_and_edit(bot: Bot, db: AsyncSession, ann):
    """
    ann: Announcement(exchange, market_type, symbol, official_time, notice_url)
    Find the posted message for the pair with missing/approx time and edit it.
    """
    q = select(SeenItem).where(
        SeenItem.exchange == ann.exchange,
        SeenItem.market_type == ann.market_type,
        SeenItem.symbol == ann.symbol,
    )
    res = await db.execute(q)
    row = res.scalar_one_or_none()
    if not row or not row.message_id:
        return

    # Update DB with official time (only if we didn't have it)
    if row.source_time is None or row.provisional:
        row.source_time = ann.official_time
        row.provisional = False
        await db.commit()

        # Re-render message
        msg_text = (
            spot_message(ann.exchange, ann.symbol, row.source_time, 2, f"{ann.exchange} announcements", row.source_url, provisional=False)
            if ann.market_type == "SPOT"
            else futures_message(ann.exchange, ann.symbol, row.source_time, 2, f"{ann.exchange} announcements", row.source_url, provisional=False)
        )
        await bot.edit_message_text(
            chat_id=bot._default_chat_id,
            message_id=row.message_id,
            text=msg_text,
            disable_web_page_preview=True,
        )
        logger.info(f"[EDITED] {ann.exchange} {ann.market_type} {ann.symbol} with official time")

async def run_announcements(bot: Bot, db_sessionmaker, interval_sec: int = 600):
    """
    Runs three announcement feeds concurrently and reconciles any matches.
    """
    from app.announcements import bitget
    from app.announcements import bingx

    async def loop_feed(feed):
        async for ann in feed:
            try:
                async with db_sessionmaker() as db:
                    await reconcile_and_edit(bot, db, ann)
            except Exception as e:
                logger.exception(f"[ANN RECONCILE ERROR] {ann.exchange}:{ann.symbol}: {e}")

    tasks = [
        asyncio.create_task(loop_feed(bitget.stream(interval_sec))),
        asyncio.create_task(loop_feed(bingx.stream_spot(interval_sec))),
        asyncio.create_task(loop_feed(bingx.stream_futures(interval_sec))),
    ]
    await asyncio.gather(*tasks)
