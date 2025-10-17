"""Configuration objects for Crypto Hornet."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    bot_token: str = Field(..., alias="BOT_TOKEN", description="Telegram bot token")
    target_chat_id: int = Field(..., alias="TARGET_CHAT_ID", description="Chat/channel id to post updates")
    owner_chat_id: Optional[int] = Field(None, alias="OWNER_CHAT_ID", description="Fallback chat id for debug messages")

    poll_interval: int = Field(90, alias="POLL_INTERVAL_SEC", description="Default polling interval in seconds")
    request_timeout: int = Field(20, alias="API_TIMEOUT_SEC", description="HTTP request timeout in seconds")
    only_usdt: bool = Field(True, alias="ONLY_USDT", description="Whether to keep only USDT pairs")
    seed_on_start: bool = Field(True, alias="SEED_ON_START", description="Store initial snapshots without posting")

    state_file: Path = Field(Path("state.json"), alias="STATE_FILE", description="Path to the JSON state file")

    http_proxy: Optional[str] = Field(None, alias="HTTP_PROXY")
    https_proxy: Optional[str] = Field(None, alias="HTTPS_PROXY")

    bingx_api_key: Optional[str] = Field(None, alias="BINGX_API_KEY")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        allow_mutation = False

    @validator("poll_interval", "request_timeout")
    def _positive(cls, value: int) -> int:  # noqa: N805
        if value <= 0:
            raise ValueError("Interval values must be positive")
        return value

    @property
    def proxies(self) -> dict[str, str]:
        proxies: dict[str, str] = {}
        if self.http_proxy:
            proxies["http"] = self.http_proxy
        if self.https_proxy:
            proxies["https"] = self.https_proxy
        return proxies