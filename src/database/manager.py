import logging
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from .models import Base, Security

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Generic Database Manager for handling sessions and basic, non-specific operations.
    """
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        logger.info("DatabaseManager initialized.")

    def create_tables(self):
        """Creates all tables defined in models.py if they don't exist."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables checked/created successfully.")

    def get_security_by_symbol(self, symbol: str) -> Security | None:
        """Fetches a single security from the master list by its unique symbol."""
        with self.Session() as session:
            stmt = select(Security).where(Security.symbol == symbol, Security.valid_to.is_(None))
            return session.execute(stmt).scalar_one_or_none()

    def bulk_insert(self, model_class, data: list[dict]):
        """Generic bulk insert for any model."""
        if not data:
            return
        with self.Session() as session:
            try:
                session.bulk_insert_mappings(model_class, data)
                session.commit()
                logger.info(f"Successfully inserted {len(data)} records into {model_class.__tablename__}.")
            except Exception as e:
                logger.error(f"Error during bulk insert for {model_class.__tablename__}: {e}")
                session.rollback()