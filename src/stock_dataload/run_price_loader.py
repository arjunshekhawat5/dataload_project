import os
from dotenv import load_dotenv
from sqlalchemy import select
import configparser
from pathlib import Path

from ..common.logger import setup_logger
from ..database.manager import DatabaseManager
from ..database.models import Security
from .api_client import FyersApiClient
from .processor import PriceHistoryProcessor

load_dotenv()


# --- RENAMED THIS FUNCTION from main() to be specific and importable ---
def run_price_history_load():
    """
    Main function to run the daily price history dataload for all relevant securities.
    """
    # --- Setup ---
    project_root = Path(__file__).resolve().parent.parent.parent
    config = configparser.ConfigParser()
    config.read(project_root / "config" / "config.ini")

    log_file_path = project_root / "logs/price_history_loader.log"
    logger = setup_logger("price_loader", log_file_path)

    db_connection_string = os.path.expandvars(config["DATABASE"]["connection_string"])

    # --- Initialization ---
    db_manager = DatabaseManager(db_connection_string)
    try:
        fyers_client = FyersApiClient(
            client_id=os.getenv("FYERS_CLIENT_ID"),
            access_token=os.getenv("FYERS_ACCESS_TOKEN"),
        )
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    price_loader = PriceHistoryProcessor(db_manager, fyers_client)

    # --- Dataload Logic ---
    with db_manager.Session() as session:
        securities_to_load = (
            session.query(Security)
            .filter(
                Security.security_type.in_(["EQUITY", "FUTURE", "INDEX"]),
                Security.valid_to.is_(None),
            )
            .all()
        )

    total_securities = len(securities_to_load)
    logger.info(f"Found {total_securities} active securities to process.")

    timeframes_to_load = ["D", "1"]

    for i, security in enumerate(securities_to_load):
        logger.info(f"--- Processing {i + 1}/{total_securities}: {security.symbol} ---")
        for tf in timeframes_to_load:
            try:
                price_loader.load_history(security, tf)
            except Exception as e:
                logger.error(
                    f"Critical error processing {security.symbol} for timeframe {tf}: {e}",
                    exc_info=True,
                )

    logger.info("--- Price History Dataload Finished ---")


# This block allows the script to be run directly for debugging if needed.
if __name__ == "__main__":
    run_price_history_load()
