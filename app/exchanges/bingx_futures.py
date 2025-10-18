import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc
import os
API_KEY = os.getenv("BINGX_API_KEY", "")
HEADERS = {"X-BX-APIKEY": API_KEY} if API_KEY else {}

name = "BINGX"
ENDPOINT = "https://api-swap-rest.bingx.com/api/v1/contract/symbols"  # verify in production
MARKET_URL = "https://bingx.com/en-us/futures/{base}USDT"

class BingXFutures:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            data = r.json()
            return data.get("data", [])


    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio
        while True:
            try:
                for it in await self._fetch():
                    # Common schemas: {"symbol":"RVVUSDT","status":"TRADING", ...}
                    base = (it.get("baseAsset") or it.get("symbol"," ").replace("USDT",""))
                    if not base:
                        continue
                    dedupe_key = f"BINGX:FUTURES:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)
                    start = now_utc()
                    yield Listing(
                        exchange=name,
                        market_type="FUTURES",
                        symbol=base,
                        source_time=start,
                        source_name="BingX swap contracts API",
                        source_url=MARKET_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)

Adapter = BingXFutures