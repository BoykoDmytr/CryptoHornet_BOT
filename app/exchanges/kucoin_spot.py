import os
import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "KUCOIN"

ENDPOINT = os.getenv("KUCOIN_SPOT_ENDPOINT", "https://api.kucoin.com/api/v1/symbols")
TRADE_URL = os.getenv("KUCOIN_TRADE_URL", "https://www.kucoin.com/trade/{base}-USDT")


class KuCoinSpot:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()
        self.seed_on_start = os.getenv("API_SEED_ON_START", "1") == "1"
        self._seeded = False

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            return r.json().get("data", [])

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio

        if not self._seeded and self.seed_on_start:
            try:
                for item in await self._fetch():
                    base = item.get("baseCurrency")
                    quote = item.get("quoteCurrency")
                    if quote != "USDT":
                        continue
                    dedupe_key = f"KUCOIN:SPOT:{base}"
                    self._known.add(dedupe_key)
                self._seeded = True
            except Exception:
                await asyncio.sleep(1)

        while True:
            try:
                for item in await self._fetch():
                    base = item.get("baseCurrency")
                    quote = item.get("quoteCurrency")
                    enable = item.get("enableTrading")
                    if quote != "USDT" or not enable:
                        continue
                    dedupe_key = f"KUCOIN:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)

                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=None,
                        provisional=True,
                        source_name="KuCoin symbols API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)


Adapter = KuCoinSpot
