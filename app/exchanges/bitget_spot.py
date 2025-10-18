import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "BITGET"
ENDPOINT = "https://api.bitget.com/api/spot/v1/public/symbols"
TRADE_URL = "https://www.bitget.com/spot/{base}USDT"

class BitgetSpot:
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
                    # Typical schema: {"baseCoin":"BTC","quoteCoin":"USDT","status":"online"}
                    base = it.get("baseCoin")
                    quote = it.get("quoteCoin")
                    status = (it.get("status") or "").lower()
                    if quote != "USDT" or status not in {"online","listed","trading"}:
                        continue
                    dedupe_key = f"BITGET:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)
                    start = now_utc()
                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=start,
                        source_name="Bitget symbols API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)

Adapter = BitgetSpot