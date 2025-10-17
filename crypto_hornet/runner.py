import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Iterable

import httpx

from .config import Settings
from .exchanges import base as exchanges_base
from .exchanges import build_feeds
from .state import PostedRecord, StateStore
from .telegram import TelegramClient
from .templates import ListingEvent

log = logging.getLogger(__name__)


@asynccontextmanager
async def http_client(settings: Settings) -> httpx.AsyncClient:
    timeout = httpx.Timeout(settings.request_timeout)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    if settings.bingx_api_key:
        headers["X-BX-APIKEY"] = settings.bingx_api_key

    async with httpx.AsyncClient(timeout=timeout, headers=headers, proxies=settings.proxies, http2=True) as client:
        yield client


async def _seed_if_needed(store: StateStore, feeds: Iterable[exchanges_base.Feed], client: httpx.AsyncClient, *, seed_only: bool) -> None:
    for feed in feeds:
        current = await feed.fetch(client)
        await store.replace_snapshot(feed.key, current)
        log.info("Seeded %s: %d pairs", feed.key, len(current))
        if not seed_only:
            await asyncio.sleep(0)


async def _run_feed(
    feed: exchanges_base.Feed,
    *,
    settings: Settings,
    store: StateStore,
    client: httpx.AsyncClient,
    telegram: TelegramClient,
) -> None:
    while True:
        try:
            snapshot = await feed.fetch(client)
            if settings.only_usdt:
                snapshot = {pair: url for pair, url in snapshot.items() if pair.endswith("/USDT")}
            previous = await store.get_snapshot(feed.key)
            if not previous:
                await store.replace_snapshot(feed.key, snapshot)
                log.info("Initial snapshot stored for %s (%d pairs)", feed.key, len(snapshot))
            else:
                diff = exchanges_base.diff_snapshot(previous, snapshot)
                if diff:
                    log.info("%s new listings detected: %s", feed.key, ", ".join(diff.keys()))
                    events: list[ListingEvent] = []
                    for pair, url in diff.items():
                        if await store.was_posted(feed.exchange, feed.market, pair):
                            continue
                        events.append(
                            ListingEvent(
                                exchange=feed.exchange,
                                market=feed.market,
                                pair=pair,
                                url=url,
                                discovered_at=exchanges_base.now_utc(),
                            )
                        )
                    posted_records: list[PostedRecord] = []
                    for event in events:
                        message_id = await telegram.post_listing(event)
                        posted_records.append(
                            PostedRecord(
                                exchange=event.exchange,
                                market=event.market,
                                pair=event.pair,
                                message_id=message_id,
                                chat_id=settings.target_chat_id,
                                posted_at=exchanges_base.now_utc(),
                            )
                        )
                    if posted_records:
                        await store.mark_posted_many(posted_records)
                await store.replace_snapshot(feed.key, snapshot)
        except Exception as exc:  # noqa: BLE001
            log.warning("Feed %s error: %s", feed.key, exc)
        await asyncio.sleep(feed.interval)


async def run(settings: Settings) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    feeds = list(build_feeds(settings.poll_interval))
    store = StateStore(settings.state_file)

    async with http_client(settings) as client:
        async with TelegramClient(settings.bot_token, settings.target_chat_id, settings.owner_chat_id) as telegram:
            if settings.seed_on_start:
                await _seed_if_needed(store, feeds, client, seed_only=True)
                log.info("Initial seeding complete")
            await telegram.send_text("✅ Crypto Hornet API watcher started")
            tasks = [
                asyncio.create_task(
                    _run_feed(feed, settings=settings, store=store, client=client, telegram=telegram)
                )
                for feed in feeds
            ]
            await asyncio.gather(*tasks)
