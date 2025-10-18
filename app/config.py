from pydantic import BaseModel, Field
from typing import List
import os

class ExchangeCfg(BaseModel):
    name: str               # canonical, e.g., KUCOIN, BINGX
    module: str             # python import path to adapter
    enabled: bool = True
    poll_seconds: float = 2.0

class Settings(BaseModel):
    bot_token: str = Field(..., alias="BOT_TOKEN")
    target_chat_id: str = Field(..., alias="TARGET_CHAT_ID")
    speed_tiers: dict = {
        1: ("Tier 1", "Webhook/push"),
        2: ("Tier 2", "Fast API polling"),
        3: ("Tier 3", "RSS/HTML"),
    }
    exchanges: List[ExchangeCfg] = [
        # Requested set — all enabled by default
        ExchangeCfg(name="GATE",   module="app.exchanges.gate_spot",     enabled=True,  poll_seconds=2.0),
        ExchangeCfg(name="BINGX",  module="app.exchanges.bingx_spot",    enabled=True,  poll_seconds=2.0),
        ExchangeCfg(name="BINGX",  module="app.exchanges.bingx_futures", enabled=True,  poll_seconds=2.0),
        ExchangeCfg(name="BITGET", module="app.exchanges.bitget_spot",   enabled=True,  poll_seconds=2.0),
        ExchangeCfg(name="KUCOIN", module="app.exchanges.kucoin_spot",   enabled=True,  poll_seconds=2.0),
        ExchangeCfg(name="KUCOIN", module="app.exchanges.kucoin_futures",enabled=True,  poll_seconds=2.0),
    ]
    database_url: str = Field(default=os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db"))

    class Config:
        populate_by_name = True


def load_settings() -> Settings:
    return Settings(
        BOT_TOKEN=os.getenv("BOT_TOKEN", ""),
        TARGET_CHAT_ID=os.getenv("TARGET_CHAT_ID", ""),
    )