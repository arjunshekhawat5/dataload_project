import logging
import requests
from fyers_apiv3 import fyersModel

logger = logging.getLogger(__name__)

class StockApiClient:
    """
    Client for downloading public, non-authenticated files like symbol masters.
    """
    def __init__(self):
        self.session = requests.Session()
        logger.info("StockApiClient for public files initialized.")

    def download_json_file(self, url: str) -> dict | None:
        """Downloads and parses a JSON file from a public URL."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            logger.info(f"Successfully downloaded master file from {url}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download file from {url}: {e}")
            return None


class FyersApiClient:
    """
    Client for handling authenticated API calls to the Fyers v3 API.
    """
    def __init__(self, client_id: str, access_token: str, log_path: str = "logs/"):
        if not all([client_id, access_token]):
            raise ValueError("Fyers client_id and access_token are required.")
        
        self.fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=access_token,
            log_path=log_path
        )
        logger.info("Fyers API client initialized.")
        
        response = self.fyers.get_profile()
        if response.get('s') == 'ok':
            logger.info(f"Successfully connected to Fyers API. Welcome, {response['data']['name']}.")
        else:
            raise ConnectionError(f"Failed to connect to Fyers API: {response.get('message')}")

    def get_daily_history(self, symbol: str, range_from: str, range_to: str):
        """
        Fetches daily historical data (OHLCV) for a symbol.
        """
        data = {
            "symbol": symbol,
            "resolution": "D",
            "date_format": "1",
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": "1"
        }
        try:
            response = self.fyers.history(data=data)
            if response.get("s") == "ok":
                return response.get("candles", [])
            else:
                logger.error(f"Error fetching daily history for {symbol}: {response.get('message')}")
                return None
        except Exception as e:
            logger.error(f"Exception during daily history fetch for {symbol}: {e}")
            return None