import logging
import pandas as pd
from datetime import date, datetime

from .database.manager import DatabaseManager
from .database.models import Security, DailyPriceHistory # Add other models as needed
from .mf_dataload.api_client import ApiClient as MfApiClient
from .stock_dataload.api_client import FyersApiClient
from .mf_dataload.processor import fetch_and_update_mf_history
from .stock_dataload.processor import fetch_and_store_daily_history

logger = logging.getLogger(__name__)

class QueryEngine:
    def __init__(self, db_manager: DatabaseManager, mf_api_client: MfApiClient, fyers_api_client: FyersApiClient):
        self.db_manager = db_manager
        self.mf_api_client = mf_api_client
        self.fyers_api_client = fyers_api_client
        logger.info("QueryEngine initialized.")

    def get_price_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date = None,
        timeframe: str = 'D'
    ) -> pd.DataFrame:
        """
        A unified function to get price data for any security.
        If data is missing, it attempts an on-demand fetch.
        """
        if end_date is None:
            end_date = start_date

        logger.info(f"Querying for {symbol} from {start_date} to {end_date} with timeframe {timeframe}.")

        # 1. Find the security in our database
        security = self.db_manager.get_security_by_symbol(symbol)
        if not security:
            logger.error(f"Security with symbol '{symbol}' not found in the database.")
            return pd.DataFrame()

        # For now, we will only implement the daily timeframe ('D')
        if timeframe != 'D':
            raise NotImplementedError("Intraday timeframes have not been implemented yet.")

        # 2. Query the database for existing data in the range
        with self.db_manager.Session() as session:
            stmt = session.query(DailyPriceHistory).filter(
                DailyPriceHistory.security_id == security.id,
                DailyPriceHistory.price_date.between(start_date, end_date)
            ).order_by(DailyPriceHistory.price_date)
            
            existing_data = pd.read_sql(stmt.statement, session.bind)

        # 3. Check for missing data and perform on-demand fetch if needed
        # A simple check: if the number of rows is less than expected, we might be missing data.
        # A more robust check would analyze the date range of the returned data.
        # For simplicity, we'll trigger a fetch if the data seems incomplete or empty.
        
        # Let's assume for now that if we ask for a range, we expect some data.
        # If it's empty, we trigger a full check for that security.
        if existing_data.empty:
            logger.warning(f"No data found locally for {symbol} in the requested range. Triggering on-demand fetch.")
            
            # Call the appropriate processor based on security type
            if security.security_type == 'MF':
                fetch_and_update_mf_history(security, self.db_manager, self.mf_api_client)
            elif security.security_type in ('EQUITY', 'FUTURE', 'INDEX', 'ETF'):
                fetch_and_store_daily_history(security, self.db_manager, self.fyers_api_client)
            else:
                logger.warning(f"On-demand fetch not implemented for security type: {security.security_type}")

            # 4. Re-query the database after the fetch
            with self.db_manager.Session() as session:
                stmt = session.query(DailyPriceHistory).filter(
                    DailyPriceHistory.security_id == security.id,
                    DailyPriceHistory.price_date.between(start_date, end_date)
                ).order_by(DailyPriceHistory.price_date)
                existing_data = pd.read_sql(stmt.statement, session.bind)

        if not existing_data.empty:
            # Clean up the DataFrame for the user
            existing_data.set_index('price_date', inplace=True)
            return existing_data[['open', 'high', 'low', 'close', 'volume']]
        else:
            logger.error(f"Could not retrieve any data for {symbol} in the specified range.")
            return pd.DataFrame()
