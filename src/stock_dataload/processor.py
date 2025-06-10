import logging
from datetime import datetime, date, timedelta
import time
import pandas as pd

from ..database.manager import DatabaseManager
from ..database.models import Security, DailyPriceHistory, OneMinuteHistory, AggregatedIntradayHistory
from .api_client import FyersApiClient

logger = logging.getLogger(__name__)

class PriceHistoryProcessor:
    def __init__(self, db_manager: DatabaseManager, fyers_client: FyersApiClient):
        self.db_manager = db_manager
        self.fyers_client = fyers_client
        logger.info("PriceHistoryProcessor initialized.")

    def load_history(self, security: Security, timeframe: str):
        """
        The main modular function. Handles both initial bulk loads and incremental updates
        for any timeframe by processing data in memory-efficient chunks.
        """
        logger.info(f"Starting history load for {security.symbol}, timeframe '{timeframe}'...")

        # 1. Determine target table, last sync time, and chunk size
        if timeframe == "D":
            target_model = DailyPriceHistory
            last_update = self.db_manager.get_last_daily_update(security.id)
            days_per_chunk = 365 * 2
            max_lookback_days = 365 * 20
            start_date = last_update + timedelta(days=1) if last_update else date.today() - timedelta(days=max_lookback_days)
        elif timeframe == "1":
            target_model = OneMinuteHistory
            last_update = self.db_manager.get_last_intraday_update(security.id)
            days_per_chunk = 60
            max_lookback_days = 365 * 7
            start_date = last_update + timedelta(minutes=1) if last_update else datetime.now() - timedelta(days=max_lookback_days)
        else:
            logger.error(f"Timeframe '{timeframe}' is not supported for direct fetching.")
            return

        end_date = datetime.now()
        if not isinstance(start_date, datetime):
            end_date = end_date.date()

        if start_date >= end_date:
            logger.info(f"Data for {security.symbol} ({timeframe}) is already up to date.")
            return

        # 2. Loop backward in chunks, fetching and storing each one immediately
        current_to_date = end_date
        while current_to_date > start_date:
            chunk_from_date = current_to_date - timedelta(days=days_per_chunk)
            if chunk_from_date < start_date:
                chunk_from_date = start_date

            if chunk_from_date >= current_to_date:
                break

            logger.info(f"Fetching chunk for {security.symbol} ({timeframe}) from {chunk_from_date.strftime('%Y-%m-%d')} to {current_to_date.strftime('%Y-%m-%d')}")
            
            chunk_data = self.fyers_client.fetch_history_chunk(
                security.symbol, timeframe,
                chunk_from_date.strftime('%Y-%m-%d'),
                current_to_date.strftime('%Y-%m-%d')
            )

            if not chunk_data:
                logger.info(f"No more data found before {current_to_date.strftime('%Y-%m-%d')}. Stopping.")
                break

            # Process and store this chunk immediately
            records_to_insert = []
            for candle in chunk_data:
                record = {'security_id': security.id, 'open': candle[1], 'high': candle[2], 'low': candle[3], 'close': candle[4], 'volume': candle[5]}
                if timeframe == "D":
                    record['price_date'] = datetime.fromtimestamp(candle[0]).date()
                else:
                    record['price_timestamp'] = datetime.fromtimestamp(candle[0])
                records_to_insert.append(record)
            
            self.db_manager.bulk_insert(target_model, records_to_insert)
            
            current_to_date = chunk_from_date
            time.sleep(0.5)

    def resample_history(self, security: Security, target_timeframe: str):
        """
        Implements the hybrid approach. Reads 1-min data and creates aggregated candles.
        """
        logger.info(f"Resampling 1-min data to '{target_timeframe}' for {security.symbol}...")
        # This logic can be implemented here, querying the DB for new 1-min data,
        # using pandas to resample, and bulk inserting into AggregatedIntradayHistory.
        # For now, we'll leave it as a placeholder.
        pass