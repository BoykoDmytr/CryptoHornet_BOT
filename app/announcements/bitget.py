# app/announcements/bitget.py
import os, httpx, urllib.parse
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
from typing import AsyncIterator
from app.exchanges.base import Announcement

SECT_URL = os.getenv("BITGET_SECTION_URL",
                     "https://www.bitget.com/support/sections/5955813039257")
SCRAPER = os.getenv("SCRAPER_URL", "")  # e.g. https://app.scrapingbee.com/api/v1/?api_key=...&url

def _fetch_url():
    return f"{SCRAPER}={urllib.parse.quote(SECT_URL)}" if SCRAPER else SECT_URL

def _guess_symbol_from_title(t: str) -> str | None:
    # very tolerant; improve as needed
    # Examples: "Bitget to List WLF", "Listing of ABC on Spot"
    import re
    m = re.search(r"\b([A-Z0-9]{2,10})\b(?=.*list|listing|spot)", t, re.I)
    return m.group(1).upper() if m else None

async def stream(interval_sec: int = 600) -> AsyncIterator[Announcement]:
    async with httpx.AsyncClient(timeout=20) as cx:
        while True:
            r = await cx.get(_fetch_url(), headers={"Accept": "text/html"})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Bitget support hub: cards with <a> titles & time labels; adjust selectors if they change
            for a in soup.select("a[href*='/support/articles/']"):
                title = (a.get_text(strip=True) or "").strip()
                url = "https://www.bitget.com" + a.get("href")
                if not ("list" in title.lower() or "listing" in title.lower()):
                    continue
                sym = _guess_symbol_from_title(title)
                if not sym:
                    continue

                # time is often in a nearby <time> or sibling; fallback: article page (omit for speed)
                time_el = a.find_next("time")
                if not time_el:
                    continue
                published = dateparse.parse(time_el.get("datetime") or time_el.get_text(strip=True))

                yield Announcement(
                    exchange="BITGET",
                    market_type="SPOT",
                    symbol=sym,
                    official_time=published,
                    notice_url=url,
                )
            await asyncio.sleep(interval_sec)
