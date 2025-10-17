"""Message formatting utilities."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ListingEvent:
    """Structure describing a detected listing."""

    exchange: str
    market: str
    pair: str
    url: str | None
    discovered_at: datetime
    source: str | None = None
    speed_tier: str | None = None

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
    """Render a high signal alert for Telegram."""

    exchange = event.exchange.upper()
    market = event.market.upper()
    discovered = _format_timestamp(event.discovered_at)
    header = f"🚀 {exchange} {market} LISTING ALERT"
    tier_line = f"⚡️ Speed tier: {event.speed_tier}" if event.speed_tier else None
    source_line = f"🛰 Source: {event.source}" if event.source else None
    url_line = f"🔗 Link: {event.url}" if event.url else None

    lines = [
        header,
        f"📈 Pair: {event.pair}",
        f"⏱ Detected: {discovered} UTC",
    ]
    if tier_line:
        lines.append(tier_line)
    if source_line:
        lines.append(source_line)
    if url_line:
        lines.append(url_line)
    return "\n".join(lines)