from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

KYIV_TZ = ZoneInfo("Europe/Kyiv")
UTC = ZoneInfo("UTC")


def fmt_times(instant: datetime) -> tuple[str, str]:
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    return (
        instant.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
        instant.astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M"),
    )


def _time_cell(start_time: Optional[datetime], provisional: bool) -> str:
    if start_time is None:
        return "Soon"
    # if we have a time but it's provisional (first-seen), show it but mark as ~approx
    start_utc, start_kyiv = fmt_times(start_time)
    return (
        f"{start_utc} UTC ({start_kyiv} Europe/Kyiv)" + (" ~" if provisional else "")
    )


def spot_message(exchange, symbol, start_time, speed_tier, source_name, url, provisional=False):
    tier_name, tier_desc = {1:("Tier 1","Webhook/push"), 2:("Tier 2","Fast API polling"), 3:("Tier 3","RSS/HTML")}[speed_tier]
    time_cell = _time_cell(start_time, provisional)
    return (
        f"🚀 {exchange} SPOT LISTING ALERT\n"
        f"📈 Pair: {symbol}/USDT\n"
        f"⏱️ Start: {time_cell}\n"
        f"⚡️ Speed tier: {tier_name} — {tier_desc}\n"
        f"🛰 Source: {source_name}\n"
        f"🔗 Link: {url}"
    )

def futures_message(exchange, symbol, start_time, speed_tier, source_name, url, provisional=False):
    tier_name, tier_desc = {1:("Tier 1","Webhook/push"), 2:("Tier 2","Fast API polling"), 3:("Tier 3","RSS/HTML")}[speed_tier]
    time_cell = _time_cell(start_time, provisional)
    return (
        f"🚀 {exchange} FUTURES LISTING ALERT\n"
        f"📈 Pair: {symbol}/USDT\n"
        f"⏱️ Start: {time_cell}\n"
        f"⚡️ Speed tier: {tier_name} — {tier_desc}\n"
        f"🛰 Source: {source_name}\n"
        f"🔗 Link: {url}"
    )