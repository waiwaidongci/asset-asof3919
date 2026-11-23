from datetime import date, datetime
from sqlalchemy import Date, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base
from src.utils.time import now


class Dataset(Base):
    """交接簿批次。大簿分批入库，完成（ready）后才供查阅与盘点。"""

    __tablename__ = 'datasets'
    __table_args__ = (UniqueConstraint('name', 'checksum'),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    filename: Mapped[str] = mapped_column(String(160))
    checksum: Mapped[str | None] = mapped_column(String(64))
    rolling_hash: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(20), default='importing')
    imported_rows: Mapped[int] = mapped_column(default=0)
    next_chunk: Mapped[int] = mapped_column(default=0)
    row_count: Mapped[int] = mapped_column(default=0)
    event_count: Mapped[int | None]
    error: Mapped[str | None] = mapped_column(String(500))
    author: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(default=now)


class LedgerChunk(Base):
    """入库分片回执：同一分片重复提交时按指纹跳过，不重复计数。"""

    __tablename__ = 'ledger_chunks'
    __table_args__ = (UniqueConstraint('dataset_id', 'chunk_index'),)

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    chunk_index: Mapped[int]
    row_count: Mapped[int]
    checksum: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(default=now)


class LedgerEvent(Base):
    """交接簿事件，保留原簿行号；replay_order 为全簿回放序号。"""

    __tablename__ = 'ledger_events'
    __table_args__ = (
        Index('ix_ledger_events_replay', 'dataset_id', 'replay_order'),
        Index('ix_ledger_events_event', 'dataset_id', 'event_id'),
        Index('ix_ledger_events_slot', 'dataset_id', 'asset_id', 'event_date', 'sequence'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    chunk_index: Mapped[int]
    source_row: Mapped[int]
    event_id: Mapped[str] = mapped_column(String(80))
    asset_id: Mapped[str] = mapped_column(String(80))
    asset_name: Mapped[str | None] = mapped_column(String(120))
    event_date: Mapped[date] = mapped_column(Date)
    sequence: Mapped[int]
    event_type: Mapped[str] = mapped_column(String(20))
    content_hash: Mapped[str] = mapped_column(String(64))
    replay_order: Mapped[int | None]
