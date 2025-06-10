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

def main():
    # --- Setup ---
    project_root = Path(__file__).resolve().parent.parent.parent
    config = configparser.ConfigParser()
    config.read(project_root / "config" / "config.ini")
    
    log_file_path = project_root / "logs/price_history_loader.log"
    logger = setup_logger('price_loader', log_file_path)

    db_connection_string = os.path.expandvars(config['DATABASE']['connection_string'])

    # --- Initialization ---
    db_manager = DatabaseManager(db_connection_string)
    try:
        fyers_client = FyersApiClient(
            client_id=os.getenv("FYERS_CLIENT_ID"),
            access_token=os.getenv("FYERS_ACCESS_TOKEN")
        )
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    processor = PriceHistoryProcessor(db_manager, fyers_client)

    # --- Dataload Logic ---
    with db_manager.Session() as session:
        securities_to_load = session.query(Security).filter(
            Security.security_type.in_(['EQUITY', 'FUTURE', 'INDEX']),
            Security.valid_to.is_(None)
        ).all()

    logger.info(f"Found {len(securities_to_load)} active securities to process.")

    # --- STAGE 1: Fetch Source-of-Truth Data (Daily and 1-Minute) ---
    logger.info("--- STAGE 1: Fetching Daily and 1-Minute data ---")
    for i, security in enumerate(securities_to_load):
        logger.info(f"--- Processing {i + 1}/{len(securities_to_load)}: {security.symbol} ---")
        processor.load_history(security, "D")
        processor.load_history(security, "1")

    # --- STAGE 2: Implement Hybrid Approach (Pre-aggregate common timeframes) ---
    logger.info("--- STAGE 2: Resampling to create aggregated timeframes ---")
    for i, security in enumerate(securities_to_load):
        logger.info(f"--- Aggregating {i + 1}/{len(securities_to_load)}: {security.symbol} ---")
        for tf in ["5min", "15min", "60min"]:
            processor.resample_history(security, tf)

    logger.info("--- Price History Dataload Finished ---")

if __name__ == "__main__":
    main()