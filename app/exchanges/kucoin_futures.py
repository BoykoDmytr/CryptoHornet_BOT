import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "KUCOIN"
ENDPOINT = "https://api-futures.kucoin.com/api/v1/contracts/active"
MARKET_URL = "https://futures.kucoin.com/trade/{base}USDTM"

class KuCoinFutures:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            data = r.json()
            return data.get("data", [])

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio
        while True:
            try:
                for it in await self._fetch():
                    # Schema: {"symbol":"RVVUSDTM", "status":"Open"}
                    sym = it.get("symbol","")
                    if not sym.endswith("USDTM"):
                        continue
                    base = sym.replace("USDTM","")
                    if it.get("status") not in {"Open","Trading","Listed"}:
                        continue
                    dedupe_key = f"KUCOIN:FUTURES:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)
                    start = now_utc()
                    yield Listing(
                        exchange=name,
                        market_type="FUTURES",
                        symbol=base,
                        source_time=start,
                        source_name="KuCoin Futures contracts API",
                        source_url=MARKET_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)

Adapter = KuCoinFutures