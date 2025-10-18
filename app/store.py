from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime, UniqueConstraint, Boolean

class Base(DeclarativeBase):
    pass

class SeenItem(Base):
    __tablename__ = "seen_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dedupe_key: Mapped[str] = mapped_column(String(255))
    exchange: Mapped[str] = mapped_column(String(32))
    market_type: Mapped[str] = mapped_column(String(16))  # SPOT/FUTURES
    symbol: Mapped[str] = mapped_column(String(64))
    source_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)  # official or None
    provisional: Mapped[bool] = mapped_column(Boolean, default=True)
    message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # TG message id to edit later
    source_url: Mapped[str] = mapped_column(String(512))
    seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("dedupe_key", name="uq_dedupe"),)

class Metric(Base):
    __tablename__ = "metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32))
    latency_ms: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

async def init_db(db_url: str):
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return async_sessionmaker(engine, expire_on_commit=False)