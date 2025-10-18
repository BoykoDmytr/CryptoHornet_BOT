import os
import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "BINGX"

API_KEY = os.getenv("BINGX_API_KEY", "")
HEADERS = {"X-BX-APIKEY": API_KEY} if API_KEY else {}

ENDPOINT = os.getenv(
    "BINGX_FUTURES_CONTRACTS_ENDPOINT",
    "https://api-swap-rest.bingx.com/api/v1/contract/symbols",
)
TRADE_URL = os.getenv("BINGX_FUT_TRADE_URL", "https://bingx.com/en-us/futures/{base}USDT")


class BingXFutures:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()
        self.seed_on_start = os.getenv("API_SEED_ON_START", "1") == "1"
        self._seeded = False

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            data = r.json()
            return data.get("data", [])

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio

        if not self._seeded and self.seed_on_start:
            try:
                for it in await self._fetch():
                    base = (it.get("baseAsset") or it.get("symbol", "").replace("USDT", ""))
                    if not base:
                        continue
                    dedupe_key = f"BINGX:FUTURES:{base}"
                    self._known.add(dedupe_key)
                self._seeded = True
            except Exception:
                await asyncio.sleep(1)

        while True:
            try:
                for it in await self._fetch():
                    base = (it.get("baseAsset") or it.get("symbol", "").replace("USDT", ""))
                    if not base:
                        continue
                    dedupe_key = f"BINGX:FUTURES:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)

                    yield Listing(
                        exchange=name,
                        market_type="FUTURES",
                        symbol=base,
                        source_time=None,
                        provisional=True,
                        source_name="BingX swap contracts API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)


Adapter = BingXFutures
