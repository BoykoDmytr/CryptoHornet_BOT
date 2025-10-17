"""Message formatting utilities."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ListingEvent:
    exchange: str
    market: str
    pair: str
    url: str | None
    discovered_at: datetime

    @property
    def base(self) -> str:
        return self.pair.split("/", 1)[0]

    @property
    def quote(self) -> str:
        return self.pair.split("/", 1)[1] if "/" in self.pair else ""

def _format_timestamp(moment: datetime) -> str:
    """Format timestamps in UTC without timezone suffix."""

    return moment.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M")


def format_listing(event: ListingEvent) -> str:
    exchange = event.exchange.upper()
    market = event.market.lower()
    discovered = _format_timestamp(event.discovered_at)
    url = event.url or ""

    if market == "futures":
        pair_flat = event.pair.replace("/", "")
        lines = [
            f"✅ {exchange} — futures {pair_flat} now launched for futures trading and trading bots",
            f"Пара: {event.pair}",
            f"🕒 Старт: {discovered}",
        ]
    else:
        lines = [
            f"✅ {exchange} — spot нова пара (API)",
            f"🕒 Дата: {discovered}",
            "",
            f"Пара: {event.pair}",
        ]
    if url:
        lines.append(f"🔗 Тікер: {url}")
    return "\n".join(lines)