from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid
from models.base import Base


class CryptoPrice(Base):
    """
    Stores raw crypto price data as fetched from the CoinGecko API.
    Includes enriched fields if API key is available.
    """

    __tablename__ = "crypto_prices_raw"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(20), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    current_price = Column(Float, nullable=False)
    market_cap = Column(Float)
    total_volume = Column(Float)
    price_change_24h = Column(Float)
    price_change_percentage_24h = Column(Float)
    market_cap_rank = Column(Integer)
    extracted_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Enhanced fields available with API key
    price_change_percentage_1h = Column(Float)
    price_change_percentage_7d = Column(Float)
    circulating_supply = Column(Float)
    total_supply = Column(Float)
    max_supply = Column(Float)
    ath = Column(Float)  # All-time high
    atl = Column(Float)  # All-time low
    last_updated = Column(String(50))

    __table_args__ = (
        UniqueConstraint("symbol", "extracted_at", name="uq_symbol_extracted_at"),
        Index("ix_crypto_prices_symbol_extracted_at", "symbol", "extracted_at"),
    )

    def __repr__(self):
        return f"<CryptoPrice(symbol={self.symbol}, price={self.current_price}, time={self.extracted_at})>"


class PipelineRun(Base):
    """
    Tracks the status and metrics of pipeline stages (extract, load, transform).
    """

    __tablename__ = "pipeline_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(String(100), nullable=False)
    stage = Column(String(50), nullable=False)  # extract, load, transform
    status = Column(String(20), nullable=False)  # running, success, failed
    records_processed = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("run_id", "stage", name="uq_run_id_stage"),
        Index("ix_pipeline_runs_run_id_stage", "run_id", "stage"),
    )

    def __repr__(self):
        return f"<PipelineRun(run_id={self.run_id}, stage={self.stage}, status={self.status})>"
