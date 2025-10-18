import os
import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "BITGET"

ENDPOINT = os.getenv("BITGET_SYMBOLS_ENDPOINT", "https://api.bitget.com/api/v2/spot/public/symbols")
TRADE_URL = os.getenv("BITGET_TRADE_URL", "https://www.bitget.com/spot/{base}USDT")


class BitgetSpot:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()
        self.seed_on_start = os.getenv("API_SEED_ON_START", "1") == "1"
        self._seeded = False

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            data = r.json()
            return data.get("data", [])

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio

        if not self._seeded and self.seed_on_start:
            try:
                for it in await self._fetch():
                    base = (
                        it.get("baseCoin")
                        or it.get("baseCoinName")
                        or it.get("symbol", "").replace("USDT", "")
                    )
                    quote = it.get("quoteCoin") or "USDT"
                    if not base or quote != "USDT":
                        continue
                    dedupe_key = f"BITGET:SPOT:{base}"
                    self._known.add(dedupe_key)
                self._seeded = True
            except Exception:
                await asyncio.sleep(1)

        while True:
            try:
                for it in await self._fetch():
                    base = (
                        it.get("baseCoin")
                        or it.get("baseCoinName")
                        or it.get("symbol", "").replace("USDT", "")
                    )
                    quote = it.get("quoteCoin") or "USDT"
                    if not base or quote != "USDT":
                        continue
                    dedupe_key = f"BITGET:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)

                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=None,
                        provisional=True,
                        source_name="Bitget symbols API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)


Adapter = BitgetSpot
