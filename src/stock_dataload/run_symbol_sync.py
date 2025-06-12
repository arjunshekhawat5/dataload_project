import configparser
from pathlib import Path
import os
from dotenv import load_dotenv
import logging

from src.common.logger import setup_logger
from src.database.manager import DatabaseManager
from src.stock_dataload.api_client import StockApiClient
from src.stock_dataload.processor import SymbolMasterLoader

load_dotenv()


def process_master_file(key, exchange, segment, config, api_client, loader):
    logger = logging.getLogger("stock_dataload")
    url = config["FYERS_MASTER_FILES"].get(key)
    if not url:
        logger.warning(f"Master file key '{key}' not found in config.ini. Skipping.")
        return

    logger.info(f"--- Processing file for {key} ({exchange}:{segment}) ---")
    json_data = api_client.download_json_file(url)

    if json_data and isinstance(json_data, dict):
        if segment == "FO":
            loader.process_derivative_master(
                data=json_data, exchange=exchange, segment=segment
            )
        else:
            loader.process_capital_market_master(
                data=json_data, exchange=exchange, segment=segment
            )
    else:
        logger.error(f"Failed to get valid dictionary data from {url}")


def run_symbol_master_sync():
    project_root = Path(__file__).resolve().parent.parent.parent
    config = configparser.ConfigParser()
    config.read(project_root / "config" / "config.ini")

    log_file_path = project_root / "logs/stock_dataload.log"
    logger = setup_logger("stock_dataload", log_file_path)

    conn_str_template = config["DATABASE"]["connection_string"]
    db_connection_string = os.path.expandvars(conn_str_template)

    db_manager = DatabaseManager(db_connection_string)
    api_client = StockApiClient()
    db_manager.create_tables()
    symbol_loader = SymbolMasterLoader(db_manager)

    logger.info("--- Starting Symbol Master Synchronization ---")

    process_master_file("nse_cm", "NSE", "CM", config, api_client, symbol_loader)
    process_master_file("nse_fo", "NSE", "FO", config, api_client, symbol_loader)

    logger.info("--- Symbol Master Synchronization Finished ---")


if __name__ == "__main__":
    run_symbol_master_sync()
