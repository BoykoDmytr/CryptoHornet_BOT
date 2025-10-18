import os
import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "GATE"

ENDPOINT = os.getenv("GATE_SPOT_ENDPOINT", "https://api.gateio.ws/api/v4/spot/currency_pairs")
TRADE_URL = os.getenv("GATE_TRADE_URL", "https://www.gate.io/trade/{base}_USDT")


class GateSpot:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()
        # >>> seed toggle <<<
        self.seed_on_start = os.getenv("API_SEED_ON_START", "1") == "1"
        self._seeded = False

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10, headers={"Accept": "application/json"}) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            return r.json()

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio

        # --------- one-time seed to avoid legacy spam ----------
        if not self._seeded and self.seed_on_start:
            try:
                for it in await self._fetch():
                    pair = it.get("id", "")
                    if not pair.endswith("_USDT"):
                        continue
                    base = pair.split("_", 1)[0]
                    dedupe_key = f"GATE:SPOT:{base}"
                    self._known.add(dedupe_key)
                self._seeded = True
            except Exception:
                await asyncio.sleep(1)
        # -------------------------------------------------------

        while True:
            try:
                for it in await self._fetch():
                    pair = it.get("id", "")
                    if not pair.endswith("_USDT"):
                        continue
                    # optional: only when tradable
                    if it.get("trade_status") not in {"tradable", "trading", "open", None}:
                        continue
                    base = pair.split("_", 1)[0]
                    dedupe_key = f"GATE:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)

                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=None,
                        provisional=True,
                        source_name="Gate.io currency_pairs API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)


Adapter = GateSpot
