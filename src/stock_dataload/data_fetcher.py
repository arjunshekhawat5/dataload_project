import logging
import time
from datetime import datetime, date, timedelta

from src.stock_dataload.api_client import FyersApiClient

logger = logging.getLogger(__name__)


class HistoricalDataFetcher:
    def __init__(self, fyers_client: FyersApiClient):
        self.fyers_client = fyers_client
        logger.info("HistoricalDataFetcher initialized.")

    def get_history(self, symbol: str, timeframe: str, start_date: date, end_date: date) -> list:
        """
        The main public method. Fetches complete historical data for a given range,
        handling chunking and backward iteration automatically using epoch timestamps.
        """
        all_candles = []
        days_per_chunk = 365 * 2 if timeframe == "D" else 60

        # Convert dates to epoch for processing
        start_epoch = int(datetime.combine(start_date, time.min).timestamp())
        end_epoch = int(datetime.combine(end_date, time.max).timestamp())

        current_to_epoch = end_epoch
        while current_to_epoch > start_epoch:
            chunk_from_epoch = current_to_epoch - (days_per_chunk * 24 * 60 * 60)
            if chunk_from_epoch < start_epoch:
                chunk_from_epoch = start_epoch

            if chunk_from_epoch >= current_to_epoch: break

            logger.info(
                f"Fetching chunk for {symbol} ({timeframe}) from {datetime.fromtimestamp(chunk_from_epoch).date()} to {datetime.fromtimestamp(current_to_epoch).date()}")

            chunk_data = self.fyers_client.fetch_history_chunk(
                symbol, timeframe, chunk_from_epoch, current_to_epoch
            )

            if chunk_data:
                all_candles.extend(chunk_data)
            else:
                logger.info(
                    f"No more data found for {symbol} before {datetime.fromtimestamp(current_to_epoch).date()}.")
                break

            current_to_epoch = chunk_from_epoch
            time.sleep(0.5)

        # Ensure uniqueness and sort chronologically
        unique_candles = {candle[0]: candle for candle in all_candles}
        return sorted(list(unique_candles.values()), key=lambda x: x[0])