import logging
from datetime import datetime
from sqlalchemy import select, func
from src.database.models import Security, DailyPriceHistory
from src.database.manager import DatabaseManager
from src.mf_dataload.api_client import ApiClient

logger = logging.getLogger(__name__)

def sync_mf_master_list(db_manager: DatabaseManager, api_client: ApiClient):
    """
    Fetches all MFs from the API and syncs them with the 'securities' table.
    """
    logger.info("Syncing MF master list...")
    api_funds = api_client.get_all_funds()
    if not api_funds:
        logger.error("Could not fetch MF list from API.")
        return

    with db_manager.Session() as session:
        stmt = select(Security).where(Security.security_type == 'MF', Security.valid_to.is_(None))
        db_mfs_raw = session.execute(stmt).scalars().all()
        db_mf_map = {mf.symbol: mf for mf in db_mfs_raw}

        api_mf_map = {str(f['schemeCode']): f for f in api_funds}
        
        db_symbols = set(db_mf_map.keys())
        api_symbols = set(api_mf_map.keys())

        new_symbols = api_symbols - db_symbols
        deactivated_symbols = db_symbols - api_symbols
        existing_symbols = db_symbols.intersection(api_symbols)
        now = datetime.utcnow()

        for symbol in new_symbols:
            fund_details = api_mf_map[symbol]
            new_sec = Security(
                symbol=symbol,
                name=fund_details['schemeName'],
                security_type='MF',
                exchange='AMFI',
                isin=fund_details.get('isinGrowth') or fund_details.get('isinDivReinvestment'),
                valid_from=now
            )
            session.add(new_sec)
        if new_symbols: logger.info(f"Added {len(new_symbols)} new MFs to securities table.")

        for symbol in deactivated_symbols:
            mf_to_deactivate = db_mf_map[symbol]
            mf_to_deactivate.valid_to = now
        if deactivated_symbols: logger.info(f"Deactivated {len(deactivated_symbols)} MFs.")

        for symbol in existing_symbols:
            db_mf = db_mf_map[symbol]
            api_mf = api_mf_map[symbol]
            if db_mf.name != api_mf['schemeName']:
                db_mf.valid_to = now
                new_version = Security(
                    symbol=symbol, name=api_mf['schemeName'], security_type='MF',
                    exchange='AMFI', isin=api_mf.get('isinGrowth') or api_mf.get('isinDivReinvestment'),
                    valid_from=now
                )
                session.add(new_version)
                logger.info(f"Updated details for MF {symbol}.")
        
        session.commit()

def fetch_and_update_mf_history(security: Security, db_manager: DatabaseManager, api_client: ApiClient):
    """Fetches and updates the daily price history for a single MF."""
    with db_manager.Session() as session:
        stmt = select(func.max(DailyPriceHistory.price_date)).where(DailyPriceHistory.security_id == security.id)
        last_sync_date = session.execute(stmt).scalar_one_or_none()

    fund_history = api_client.get_fund_history(int(security.symbol))
    if not fund_history or 'data' not in fund_history:
        return

    records_to_insert = []
    for nav_entry in fund_history['data']:
        try:
            nav_date = datetime.strptime(nav_entry['date'], '%d-%m-%Y').date()
            if last_sync_date is None or nav_date > last_sync_date:
                records_to_insert.append({
                    'security_id': security.id,
                    'price_date': nav_date,
                    'close': float(nav_entry['nav']),
                })
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse NAV entry for {security.symbol}: {nav_entry}. Error: {e}")
    
    if records_to_insert:
        db_manager.bulk_insert(DailyPriceHistory, records_to_insert)