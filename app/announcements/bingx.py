# app/announcements/bingx.py
import os, httpx, urllib.parse, asyncio
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
from typing import AsyncIterator
from app.exchanges.base import Announcement

SPOT_URL = os.getenv(
    "BINGX_SPOT_NOTICE_URL",
    "https://bingx.com/en/support/notice-center/11257060005007",
)
FUT_URL = os.getenv(
    "BINGX_FUTURES_NOTICE_URL",
    "https://bingx.com/en/support/notice-center/11257015822991",
)
SCRAPER = os.getenv("SCRAPER_URL", "")

def _wrap(url: str) -> str:
    return f"{SCRAPER}={urllib.parse.quote(url)}" if SCRAPER else url

def _guess_symbol(title: str) -> str | None:
    import re
    # catch patterns like "List XXX/USDT", "Listing XXX", "Launch XXX ..."
    m = re.search(r"\b([A-Z0-9]{2,10})\b(?:/USDT)?", title)
    return m.group(1).upper() if m else None

async def _fetch(url: str, market_type: str, interval_sec: int) -> AsyncIterator[Announcement]:
    async with httpx.AsyncClient(timeout=20) as cx:
        while True:
            r = await cx.get(_wrap(url), headers={"Accept": "text/html"})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            # Titles are <a> entries; adjust selectors if BingX changes layout
            for a in soup.select("a[href*='/support/articles/']"):
                title = (a.get_text(strip=True) or "").strip()
                if "list" not in title.lower() and "listing" not in title.lower():
                    continue
                sym = _guess_symbol(title)
                if not sym:
                    continue
                href = a.get("href")
                link = "https://bingx.com" + href if href.startswith("/") else href

                # time near the card
                time_el = a.find_next("time")
                if not time_el:
                    continue
                published = dateparse.parse(time_el.get("datetime") or time_el.get_text(strip=True))

                yield Announcement(
                    exchange="BINGX",
                    market_type=market_type,
                    symbol=sym,
                    official_time=published,
                    notice_url=link,
                )
            await asyncio.sleep(interval_sec)

async def stream_spot(interval_sec: int = 600) -> AsyncIterator[Announcement]:
    async for a in _fetch(SPOT_URL, "SPOT", interval_sec):
        yield a

async def stream_futures(interval_sec: int = 600) -> AsyncIterator[Announcement]:
    async for a in _fetch(FUT_URL, "FUTURES", interval_sec):
        yield a
