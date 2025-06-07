import requests
import logging

logger = logging.getLogger(__name__)

class ApiClient:
    def __init__(self, list_url, data_url_template):
        self.list_url = list_url
        self.data_url_template = data_url_template
        self.session = requests.Session()

    def get_all_funds(self) -> list[dict] | None:
        try:
            response = self.session.get(self.list_url, timeout=30)
            response.raise_for_status()
            logger.info(f"Successfully fetched list of all funds from {self.list_url}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch all funds: {e}")
            return None

    def get_fund_history(self, scheme_code: int) -> dict | None:
        url = self.data_url_template.format(scheme_code=scheme_code)
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            if not response.text:
                logger.warning(f"Received empty response for scheme_code {scheme_code}")
                return None
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch history for scheme_code {scheme_code}: {e}")
            return None
