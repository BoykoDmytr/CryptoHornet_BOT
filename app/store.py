# app/store.py
from datetime import datetime
from pathlib import Path

from sqlalchemy import String, Integer, DateTime, UniqueConstraint, Boolean
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SeenItem(Base):
    __tablename__ = "seen_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dedupe_key: Mapped[str] = mapped_column(String(255))
    exchange: Mapped[str] = mapped_column(String(32))
    market_type: Mapped[str] = mapped_column(String(16))  # SPOT/FUTURES
    symbol: Mapped[str] = mapped_column(String(64))
    source_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # official or None
    provisional: Mapped[bool] = mapped_column(Boolean, default=True)
    message_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # TG message id to edit later
    source_url: Mapped[str] = mapped_column(String(512))
    seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("dedupe_key", name="uq_dedupe"),)


class Metric(Base):
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32))
    latency_ms: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


async def init_db(database_url: str):
    """
    Initialize async SQLAlchemy engine & sessionmaker.
    - Auto-creates parent directory for SQLite file URLs (e.g., /data/bot.db).
    - Creates tables on first run.
    """
    url = make_url(database_url)

    # Ensure SQLite file directory exists (Railway volumes: e.g., /data/bot.db)
    if url.drivername.startswith("sqlite"):
        db_path = url.database  # absolute path or relative
        if db_path and db_path != ":memory:":
            Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)

    engine = create_async_engine(
        database_url,
        future=True,
        pool_pre_ping=True,  # helps recover from stale connections
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    return async_sessionmaker(engine, expire_on_commit=False)
