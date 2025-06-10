import time
import logging
from datetime import datetime, timedelta
from .api_client import FyersApiClient

logger = logging.getLogger(__name__)

class HistoricalDataFetcher:
    def __init__(self, symbol: str, fyers_client: FyersApiClient, timeframe: str = "D", from_time: datetime = None, to_time: datetime = None):
        """
        Initialize the HistoricalDataFetcher.
        :param symbol: Symbol to fetch data for
        :param fyers_client: An instance of our FyersApiClient
        :param timeframe: Timeframe for the candles ("D", "1", "5", etc.)
        :param from_time: Start time for fetching data (datetime object)
        :param to_time: End time for fetching data (datetime object)
        """
        self.symbol = symbol
        self.fyers_client = fyers_client
        self.timeframe = timeframe
        self.from_time = from_time
        self.to_time = to_time if to_time else datetime.now()

    def _calculate_date_range(self):
        """Calculate the date range and chunk size for fetching historical data."""
        if self.timeframe == "D":
            days_per_chunk = 365 * 2 # 2 years per chunk for daily data
            max_lookback_days = 365 * 20 # 20 years max lookback
        else:
            days_per_chunk = 60 # 60 days per chunk for intraday data
            max_lookback_days = 365 * 7 # 7 years max lookback for intraday

        if self.from_time:
            from_time_dt = self.from_time
        else:
            from_time_dt = self.to_time - timedelta(days=max_lookback_days)
        
        return from_time_dt, self.to_time, days_per_chunk

    def get_historical_data(self) -> list:
        """
        Fetch historical data by walking backward in time in chunks.
        """
        historical_data = []
        from_time_dt, to_time_dt, days_per_chunk = self._calculate_date_range()

        logger.info(f"Fetching '{self.timeframe}' data for {self.symbol} from {from_time_dt.date()} to {to_time_dt.date()}")

        current_to_date = to_time_dt
        
        while current_to_date > from_time_dt:
            chunk_from_date = current_to_date - timedelta(days=days_per_chunk)
            if chunk_from_date < from_time_dt:
                chunk_from_date = from_time_dt

            if chunk_from_date >= current_to_date:
                break

            if self.timeframe == "D":
                chunk_data = self.fyers_client.get_daily_history(
                    self.symbol,
                    chunk_from_date.strftime('%Y-%m-%d'),
                    current_to_date.strftime('%Y-%m-%d')
                )
            else:
                chunk_data = self.fyers_client.get_intraday_history(
                    self.symbol,
                    self.timeframe,
                    chunk_from_date.strftime('%Y-%m-%d'),
                    current_to_date.strftime('%Y-%m-%d')
                )

            if chunk_data:
                historical_data.extend(chunk_data)
            else:
                logger.info(f"No more data found for {self.symbol} before {current_to_date.date()}. Stopping fetch.")
                break
            
            current_to_date = chunk_from_date
            time.sleep(0.4)

        unique_candles = {candle[0]: candle for candle in historical_data}
        return sorted(list(unique_candles.values()), key=lambda x: x[0])