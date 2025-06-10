import logging
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from .models import (
    Base,
    Security,
    DailyPriceHistory,
    OneMinuteHistory,
)  # Add OneMinuteHistory
from datetime import datetime  # Add datetime

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        logger.info("DatabaseManager initialized.")

    def create_tables(self):
        Base.metadata.create_all(self.engine)
        logger.info("Database tables checked/created successfully.")

    def get_security_by_symbol(self, symbol: str) -> Security | None:
        with self.Session() as session:
            stmt = select(Security).where(
                Security.symbol == symbol, Security.valid_to.is_(None)
            )
            return session.execute(stmt).scalar_one_or_none()

    def bulk_insert(self, model_class, data: list[dict]):
        if not data:
            return
        with self.Session() as session:
            try:
                session.bulk_insert_mappings(model_class, data)
                session.commit()
                logger.info(
                    f"Successfully inserted {len(data)} records into {model_class.__tablename__}."
                )
            except Exception as e:
                logger.error(
                    f"Error during bulk insert for {model_class.__tablename__}: {e}"
                )
                session.rollback()

    def get_last_daily_update(self, security_id: int) -> datetime | None:
        """Finds the most recent date for a given security in the daily history table."""
        with self.Session() as session:
            last_date = (
                session.query(func.max(DailyPriceHistory.price_date))
                .filter(DailyPriceHistory.security_id == security_id)
                .scalar()
            )
            return (
                datetime.combine(last_date, datetime.min.time()) if last_date else None
            )

    def get_last_intraday_update(self, security_id: int) -> datetime | None:
        """Finds the most recent timestamp for a given security in the 1-min history table."""
        with self.Session() as session:
            last_date = (
                session.query(func.max(OneMinuteHistory.price_timestamp))
                .filter(OneMinuteHistory.security_id == security_id)
                .scalar()
            )
            logger.info(
                f"get_last_intraday_update for security_id {security_id}: {last_date}"
            )
            return last_date
