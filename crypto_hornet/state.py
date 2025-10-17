"""State persistence helpers."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Mapping, MutableMapping, Optional


@dataclass(slots=True)
class PostedRecord:
    exchange: str
    market: str
    pair: str
    message_id: Optional[int]
    chat_id: Optional[int]
    posted_at: datetime

    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "market": self.market,
            "pair": self.pair,
            "message_id": self.message_id,
            "chat_id": self.chat_id,
            "posted_at": self.posted_at.replace(tzinfo=timezone.utc).isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PostedRecord":
        posted_at_raw = str(payload.get("posted_at") or "")
        try:
            posted_at = datetime.fromisoformat(posted_at_raw.replace("Z", "+00:00"))
        except ValueError:
            posted_at = datetime.now(timezone.utc)
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)
        return cls(
            exchange=str(payload.get("exchange", "")).lower(),
            market=str(payload.get("market", "")).lower(),
            pair=str(payload.get("pair", "")),
            message_id=int(payload.get("message_id")) if payload.get("message_id") is not None else None,
            chat_id=int(payload.get("chat_id")) if payload.get("chat_id") is not None else None,
            posted_at=posted_at,
        )


class StateStore:
    """In-memory representation of snapshots with JSON persistence."""

    def __init__(self, path: Path):
        self._path = path
        self._lock = asyncio.Lock()
        self._data: dict[str, object] = {"snapshots": {}, "posted": {}}
        self._load()

    # ------------------------- persistence helpers -------------------------
    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text("utf-8"))
            if isinstance(raw, dict):
                self._data["snapshots"] = raw.get("snapshots", {}) or {}
                self._data["posted"] = raw.get("posted", {}) or {}
        except Exception:
            self._data = {"snapshots": {}, "posted": {}}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    # ----------------------------- snapshots --------------------------------
    async def get_snapshot(self, key: str) -> Dict[str, str]:
        async with self._lock:
            snapshots: MutableMapping[str, Dict[str, str]] = self._data.setdefault("snapshots", {})  # type: ignore[assignment]
            return dict(snapshots.get(key, {}))

    async def replace_snapshot(self, key: str, snapshot: Mapping[str, str]) -> None:
        async with self._lock:
            snapshots: MutableMapping[str, Dict[str, str]] = self._data.setdefault("snapshots", {})  # type: ignore[assignment]
            snapshots[key] = dict(snapshot)
            self._save()

    # ------------------------------ postings --------------------------------
    async def mark_posted(self, record: PostedRecord) -> None:
        async with self._lock:
            posted: MutableMapping[str, dict] = self._data.setdefault("posted", {})  # type: ignore[assignment]
            posted[self._posted_key(record.exchange, record.market, record.pair)] = record.to_dict()
            self._save()

    async def mark_posted_many(self, records: Iterable[PostedRecord]) -> None:
        async with self._lock:
            posted: MutableMapping[str, dict] = self._data.setdefault("posted", {})  # type: ignore[assignment]
            for rec in records:
                posted[self._posted_key(rec.exchange, rec.market, rec.pair)] = rec.to_dict()
            self._save()

    async def was_posted(self, exchange: str, market: str, pair: str) -> bool:
        async with self._lock:
            posted: Mapping[str, dict] = self._data.get("posted", {})  # type: ignore[assignment]
            return self._posted_key(exchange, market, pair) in posted

    async def recent_posts(self) -> Dict[str, PostedRecord]:
        async with self._lock:
            posted_raw: Mapping[str, dict] = self._data.get("posted", {})  # type: ignore[assignment]
            return {k: PostedRecord.from_dict(v) for k, v in posted_raw.items() if isinstance(v, dict)}

    @staticmethod
    def _posted_key(exchange: str, market: str, pair: str) -> str:
        return f"{exchange.lower()}|{market.lower()}|{pair.upper()}"