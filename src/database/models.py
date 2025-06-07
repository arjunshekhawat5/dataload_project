from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Numeric, BigInteger,
    ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Security(Base):
    """
    Master table for all securities (stocks, MFs, ETFs, etc.).
    """
    __tablename__ = 'securities'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False)
    security_type = Column(String, nullable=False, index=True)
    exchange = Column(String, nullable=True, index=True)
    segment = Column(String, nullable=True, index=True)
    isin = Column(String, nullable=True, unique=True)
    valid_from = Column(DateTime, nullable=False, server_default=func.now())
    valid_to = Column(DateTime, nullable=True)

    # Define the "one" side of the relationships to child tables
    equity_meta = relationship("SecuritiesEquityMeta", back_populates="security", uselist=False, cascade="all, delete-orphan")
    derivative_meta = relationship("SecuritiesDerivativeMeta", back_populates="security", uselist=False, cascade="all, delete-orphan")
    daily_history = relationship("DailyPriceHistory", back_populates="security", cascade="all, delete-orphan")
    one_minute_history = relationship("OneMinuteHistory", back_populates="security", cascade="all, delete-orphan")
    aggregated_intraday_history = relationship("AggregatedIntradayHistory", back_populates="security", cascade="all, delete-orphan")


class SecuritiesEquityMeta(Base):
    """
    Stores metadata specific to Equities.
    """
    __tablename__ = 'securities_equity_meta'
    id = Column(Integer, primary_key=True, autoincrement=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False, unique=True)
    lot_size = Column(Integer, nullable=False)
    tick_size = Column(Numeric(10, 4), nullable=False)
    company_name = Column(String, nullable=True)

    # Define the "many" (or "one" in this case) side of the relationship
    security = relationship("Security", back_populates="equity_meta")


class SecuritiesDerivativeMeta(Base):
    """
    Stores metadata specific to Futures and Options.
    """
    __tablename__ = 'securities_derivative_meta'
    id = Column(Integer, primary_key=True, autoincrement=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False, unique=True)
    underlying_symbol = Column(String, nullable=False, index=True)
    instrument_type = Column(String, nullable=False)
    expiry_date = Column(Date, nullable=False, index=True)
    strike_price = Column(Numeric(12, 4), nullable=True)
    option_type = Column(String, nullable=True)
    lot_size = Column(Integer, nullable=False)
    tick_size = Column(Numeric(10, 4), nullable=False)

    # Define the relationship to the parent Security
    security = relationship("Security", back_populates="derivative_meta")


class DailyPriceHistory(Base):
    """
    Stores daily OHLCV data for all securities. For MFs, NAV is stored in 'close'.
    """
    __tablename__ = 'daily_price_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False)
    price_date = Column(Date, nullable=False)
    open = Column(Numeric(12, 4), nullable=True)
    high = Column(Numeric(12, 4), nullable=True)
    low = Column(Numeric(12, 4), nullable=True)
    close = Column(Numeric(12, 4), nullable=False)
    volume = Column(BigInteger, nullable=True)
    __table_args__ = (UniqueConstraint('security_id', 'price_date', name='uq_daily_price'),)

    # Define the relationship to the parent Security
    security = relationship("Security", back_populates="daily_history")


class OneMinuteHistory(Base):
    """
    The source of truth for all high-resolution intraday data.
    """
    __tablename__ = 'one_minute_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False)
    price_timestamp = Column(DateTime, nullable=False)
    open = Column(Numeric(12, 4), nullable=False)
    high = Column(Numeric(12, 4), nullable=False)
    low = Column(Numeric(12, 4), nullable=False)
    close = Column(Numeric(12, 4), nullable=False)
    volume = Column(BigInteger, nullable=False)
    __table_args__ = (UniqueConstraint('security_id', 'price_timestamp', name='uq_one_minute_price'),)

    # Define the relationship to the parent Security
    security = relationship("Security", back_populates="one_minute_history")


class AggregatedIntradayHistory(Base):
    """
    Stores pre-aggregated common timeframes (5min, 15min, etc.) for fast querying.
    """
    __tablename__ = 'aggregated_intraday_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False)
    timeframe = Column(String(10), nullable=False)
    price_timestamp = Column(DateTime, nullable=False)
    open = Column(Numeric(12, 4), nullable=False)
    high = Column(Numeric(12, 4), nullable=False)
    low = Column(Numeric(12, 4), nullable=False)
    close = Column(Numeric(12, 4), nullable=False)
    volume = Column(BigInteger, nullable=False)
    __table_args__ = (UniqueConstraint('security_id', 'timeframe', 'price_timestamp', name='uq_agg_intraday_price'),)

    # Define the relationship to the parent Security
    security = relationship("Security", back_populates="aggregated_intraday_history")