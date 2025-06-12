import logging
from src.common.logger import setup_logger
from pathlib import Path

from src.stock_dataload.run_symbol_sync import run_symbol_master_sync
from src.stock_dataload.run_price_loader import run_price_history_load

log_file_path = Path("logs/stock_dataload_orchestrator.log")
log_file_path.parent.mkdir(parents=True, exist_ok=True)
setup_logger("stock_orchestrator", log_file_path)
logger = logging.getLogger("stock_orchestrator")


def main():
    """
    The main entry point for the daily stock and derivative dataload process.
    It runs the required jobs in the correct sequence.
    """
    logger.info("========================================================")
    logger.info("=== STARTING DAILY STOCK & DERIVATIVE DATALOAD JOB ===")
    logger.info("========================================================")

    try:
        # --- STAGE 1: Sync all instrument metadata first ---
        # This is critical to discover new instruments and handle F&O expiries.
        logger.info("\n--- STAGE 1: Running Symbol Master Synchronization ---")
        run_symbol_master_sync()
        logger.info("--- STAGE 1: Symbol Master Synchronization Finished ---\n")

        # --- STAGE 2: Run the Price History Dataload ---
        # This fetches daily and intraday prices for the instruments synced in Stage 1.
        logger.info("\n--- STAGE 2: Running Stock Price History Dataload ---")
        run_price_history_load()
        logger.info("--- STAGE 2: Stock Price History Dataload Finished ---\n")

    except Exception as e:
        logger.critical(
            f"A critical error occurred in the main stock orchestrator: {e}",
            exc_info=True,
        )
        logger.info("========================================================")
        logger.info("=== DAILY STOCK DATALOAD JOB FAILED ===")
        logger.info("========================================================")
        return

    logger.info("========================================================")
    logger.info("=== DAILY STOCK & DERIVATIVE DATALOAD JOB COMPLETED SUCCESSFULLY ===")
    logger.info("========================================================")


if __name__ == "__main__":
    main()
