import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "GATE"
ENDPOINT = "https://api.gateio.ws/api/v4/spot/currency_pairs"
TRADE_URL = "https://www.gate.io/trade/{base}_USDT"

class GateSpot:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10, headers={"Accept": "application/json"}) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            return r.json()

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio
        while True:
            try:
                for it in await self._fetch():
                    # Schema: {"id":"RVV_USDT","trade_status":"tradable", ...}
                    pair = it.get("id","")
                    if not pair.endswith("_USDT") or it.get("trade_status") != "tradable":
                        continue
                    base = pair.split("_", 1)[0]
                    dedupe_key = f"GATE:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)
                    start = now_utc()
                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=start,
                        source_name="Gate.io currency_pairs API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)

Adapter = GateSpot