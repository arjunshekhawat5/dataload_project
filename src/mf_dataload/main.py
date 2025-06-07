import configparser
from pathlib import Path
from sqlalchemy import select
import os
from dotenv import load_dotenv

from ..common.logger import setup_logger
from ..database.manager import DatabaseManager
from ..database.models import Security
from .api_client import ApiClient
from .processor import sync_mf_master_list, fetch_and_update_mf_history

# Load environment variables from .env file so os.path.expandvars can find them
load_dotenv()

def run_mf_dataload():
    # --- Setup ---
    current_file_path = Path(__file__).resolve()
    project_root = current_file_path.parent.parent.parent
    config_path = project_root / "config" / "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    if not config.sections():
        raise FileNotFoundError(f"Config file not found or empty at {config_path}")

    log_file_path = project_root / config['LOGGING']['log_file']
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger('mf_dataload', log_file_path, config['LOGGING']['log_level'])

    # --- Get DB connection string and expand any environment variables ---
    conn_str_template = config['DATABASE']['connection_string']
    
    # os.path.expandvars will find ${DB_PASSWORD} and replace it with the env variable
    db_connection_string = os.path.expandvars(conn_str_template)

    # A check to ensure the substitution happened correctly
    if "${" in db_connection_string:
        raise ValueError("DB password placeholder was not replaced. Is DB_PASSWORD set in your .env file?")
    
    # Handle the local SQLite case separately
    if db_connection_string.startswith("sqlite"):
        db_path = project_root / db_connection_string.split('///')[1]
        db_connection_string = f"sqlite:///{db_path}"
        logger.info(f"Connecting to local database: {db_connection_string}")
    else:
        logger.info("Connecting to cloud database.")


    logger.info("--- Starting MF Dataload Process ---")

    # --- Initialization ---
    db_manager = DatabaseManager(db_connection_string)
    api_client = ApiClient(config['API']['list_all_funds_url'], config['API']['fund_data_url_template'])
    db_manager.create_tables()

    # --- Dataload Logic ---
    sync_mf_master_list(db_manager, api_client)

    with db_manager.Session() as session:
        stmt = select(Security).where(Security.security_type == 'MF', Security.valid_to.is_(None))
        all_mfs = session.execute(stmt).scalars().all()

    logger.info(f"Found {len(all_mfs)} active MFs to update.")
    for mf in all_mfs:
        logger.info(f"Processing history for {mf.symbol}: {mf.name}")
        fetch_and_update_mf_history(mf, db_manager, api_client)

    logger.info("--- MF Dataload Process Finished ---")

if __name__ == '__main__':
    run_mf_dataload()