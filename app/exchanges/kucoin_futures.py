import os
import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "KUCOIN"

ENDPOINT = os.getenv("KUCOIN_FUTURES_ENDPOINT", "https://api-futures.kucoin.com/api/v1/contracts/active")
TRADE_URL = os.getenv("KUCOIN_FUT_TRADE_URL", "https://futures.kucoin.com/trade/{base}USDTM")


class KuCoinFutures:
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
                for it in await self._fetch():
                    sym = it.get("symbol", "")
                    if not sym.endswith("USDTM"):
                        continue
                    base = sym.replace("USDTM", "")
                    dedupe_key = f"KUCOIN:FUTURES:{base}"
                    self._known.add(dedupe_key)
                self._seeded = True
            except Exception:
                await asyncio.sleep(1)

        while True:
            try:
                for it in await self._fetch():
                    sym = it.get("symbol", "")
                    if not sym.endswith("USDTM"):
                        continue
                    base = sym.replace("USDTM", "")
                    if it.get("status") not in {"Open", "Trading", "Listed", None}:
                        continue
                    dedupe_key = f"KUCOIN:FUTURES:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)

                    yield Listing(
                        exchange=name,
                        market_type="FUTURES",
                        symbol=base,
                        source_time=None,
                        provisional=True,
                        source_name="KuCoin Futures contracts API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)


Adapter = KuCoinFutures
