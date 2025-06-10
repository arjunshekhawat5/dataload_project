import logging
import requests
# --- CORRECT V3 IMPORT AS PER YOUR WORKING EXAMPLE ---
from fyers_apiv3 import fyersModel

logger = logging.getLogger(__name__)

class StockApiClient:
    """
    Client for downloading public, non-authenticated files like symbol masters.
    (This class is correct and remains unchanged).
    """
    def __init__(self):
        self.session = requests.Session()
        logger.info("StockApiClient for public files initialized.")

    def download_json_file(self, url: str) -> dict | None:
        """Downloads and parses a JSON file from a public URL."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download file from {url}: {e}")
            return None

class FyersApiClient:
    """
    A client for the Fyers v3 API, using the fyersModel pattern.
    """
    def __init__(self, client_id: str, access_token: str, log_path: str = "logs/"):
        if not all([client_id, access_token]):
            raise ValueError("Fyers client_id and access_token are required.")
        
        try:
            # --- USE THE CORRECT FYERSMODEL INSTANTIATION ---
            self.fyers = fyersModel.FyersModel(
                client_id=client_id,
                is_async=False,
                token=access_token,
                log_path=log_path
            )
            
            logger.info("FyersApiClient initialized with fyersModel.")
            
            response = self.fyers.get_profile()
            if response.get('s') != 'ok':
                raise ConnectionError(f"Failed to connect to Fyers API: {response.get('message')}")
            logger.info(f"Successfully connected to Fyers API. Welcome, {response['data']['name']}.")

        except Exception as e:
            logger.error(f"Failed to initialize FyersApiClient: {e}")
            raise

    def fetch_history_chunk(self, symbol: str, resolution: str, range_from: str, range_to: str) -> list | None:
        """A single, direct call to the Fyers history API for one chunk of data."""
        data = {
            "symbol": symbol, "resolution": resolution, "date_format": "1",
            "range_from": range_from, "range_to": range_to, "cont_flag": "1"
        }
        try:
            response = self.fyers.history(data=data)
            if response.get("s") == "ok":
                return response.get("candles", [])
            else:
                logger.error(f"API Error for {symbol} ({resolution}): {response.get('message')}")
                return None
        except Exception as e:
            logger.error(f"Exception during history fetch for {symbol}: {e}")
            return None