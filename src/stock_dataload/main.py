import configparser
from pathlib import Path
import os
from dotenv import load_dotenv
from typing import Callable 
import logging

from ..common.logger import setup_logger
from ..database.manager import DatabaseManager
from .api_client import StockApiClient
from .processor import process_capital_market_master, process_derivative_master

load_dotenv()

def process_master_file(
    key: str,
    exchange: str,
    segment: str,
    processor_func: Callable, 
    config: configparser.ConfigParser,
    api_client: StockApiClient,
    db_manager: DatabaseManager
):
    """
    A generic function to download a master file and run a specific processor on it.
    """
    logger = logging.getLogger('stock_dataload') 
    master_files = config['FYERS_MASTER_FILES']

    if key in master_files:
        url = master_files[key]
        logger.info(f"--- Processing file for {key} ({exchange}:{segment}) ---")
        logger.info(f"URL: {url}")
        
        json_data = api_client.download_json_file(url)
        
        if json_data and isinstance(json_data, dict):
            # Call the provided processor function with the downloaded data
            processor_func(data=json_data, exchange=exchange, segment=segment, db_manager=db_manager)
        else:
            logger.error(f"Failed to get valid dictionary data from {url}")
    else:
        logger.warning(f"Master file key '{key}' not found in config.ini. Skipping.")


def run_symbol_master_sync():
    """
    Main function to run the entire symbol master synchronization process.
    This is now a clean, high-level orchestrator.
    """
    current_file_path = Path(__file__).resolve()
    project_root = current_file_path.parent.parent.parent
    config_path = project_root / "config" / "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    if not config.sections():
        raise FileNotFoundError(f"Config file not found or empty at {config_path}")

    log_file_path = project_root / "logs/stock_dataload.log"
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger('stock_dataload', log_file_path, config['LOGGING']['log_level'])

    conn_str_template = config['DATABASE']['connection_string']
    db_connection_string = os.path.expandvars(conn_str_template)
    if "${" in db_connection_string:
        raise ValueError("DB password placeholder was not replaced. Is DB_PASSWORD set in your .env file?")

    # --- Initialization ---
    db_manager = DatabaseManager(db_connection_string)
    api_client = StockApiClient()
    db_manager.create_tables()
    logger.info("--- Starting Symbol Master Synchronization ---")

    
    # Process NSE Capital Market file
    process_master_file(
        key='nse_cm',
        exchange='NSE',
        segment='CM',
        processor_func=process_capital_market_master,
        config=config,
        api_client=api_client,
        db_manager=db_manager
    )

    # Process NSE Futures & Options file
    process_master_file(
        key='nse_fo',
        exchange='NSE',
        segment='FO',
        processor_func=process_derivative_master,
        config=config,
        api_client=api_client,
        db_manager=db_manager
    )

    
    logger.info("--- Symbol Master Synchronization Finished ---")

if __name__ == '__main__':
    run_symbol_master_sync()