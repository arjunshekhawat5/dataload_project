import configparser
from pathlib import Path
import os
from dotenv import load_dotenv
from sqlalchemy import select

from src.common.logger import setup_logger
from ..database.manager import DatabaseManager
from ..database.models import Security
from .api_client import FyersApiClient
from .processor import fetch_and_store_daily_history

load_dotenv()

def run_price_history_load():
    """
    Main function to run the daily price history dataload for all relevant securities.
    """
    # --- Setup and DB Connection ---
    project_root = Path(__file__).resolve().parent.parent.parent
    config_path = project_root / "config" / "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    
    log_file_path = project_root / "logs/price_history_loader.log"
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger('price_loader', log_file_path, config['LOGGING']['log_level'])

    conn_str_template = config['DATABASE']['connection_string']
    db_connection_string = os.path.expandvars(conn_str_template)

    # --- Initialization ---
    db_manager = DatabaseManager(db_connection_string)
    fyers_client = FyersApiClient(
        client_id=os.getenv("FYERS_CLIENT_ID"),
        access_token=os.getenv("FYERS_ACCESS_TOKEN")
    )
    logger.info("--- Starting Daily Price History Dataload ---")

    # --- Dataload Logic ---
    with db_manager.Session() as session:
        # Fetch all active securities that we want price history for
        stmt = select(Security).where(
            Security.security_type.in_(['EQUITY', 'FUTURE', 'ETF', 'INDEX']),
            Security.valid_to.is_(None)
        ).order_by(Security.symbol)
        securities_to_load = session.execute(stmt).scalars().all()

    total_securities = len(securities_to_load)
    logger.info(f"Found {total_securities} active securities to update.")

    for i, security in enumerate(securities_to_load):
        logger.info(f"Processing {i + 1}/{total_securities}: {security.symbol}")
        try:
            fetch_and_store_daily_history(security, db_manager, fyers_client)
        except Exception as e:
            logger.error(f"Failed to process {security.symbol} due to an unexpected error: {e}")

    logger.info("--- Daily Price History Dataload Finished ---")

if __name__ == '__main__':
    run_price_history_load()
