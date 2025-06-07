import logging
import requests

logger = logging.getLogger(__name__)

class StockApiClient:
    """
    Client for handling non-authenticated API calls, like downloading master files.
    """
    def __init__(self):
        self.session = requests.Session()
        logger.info("StockApiClient initialized.")

    def download_json_file(self, url: str) -> list[dict] | None:
        """Downloads and parses a JSON file from a public URL."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            logger.info(f"Successfully downloaded master file from {url}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download file from {url}: {e}")
            return None