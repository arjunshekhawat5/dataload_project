import logging
from datetime import datetime
from sqlalchemy import select

from ..database.models import Security, SecuritiesEquityMeta, SecuritiesDerivativeMeta
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)

def classify_security(symbol: str, isin: str | None) -> str:
    """
    Classifies a security type based on its symbol suffix or ISIN.
    This version now correctly identifies INDEX as a valid type.
    """
    # Highest priority: ISIN prefix for MFs is definitive.
    if isin and isin.startswith('INF'):
        return 'MF'

    # Split the symbol to analyze the suffix
    parts = symbol.split('-')
    suffix = parts[-1] if len(parts) > 1 else ''

    # Identify indices first, as they are a primary asset class.
    if suffix == 'INDEX':
        return 'INDEX'

    # Equity and related types
    if suffix in ('EQ', 'SM', 'ST', 'BZ', 'E1'):
        return 'EQUITY'
    
    # Exchange Traded Funds and Trusts
    if suffix == 'BE':
        return 'ETF'
    if suffix == 'IV':
        return 'INVIT'
    if suffix == 'RE':
        return 'REIT'

    # Government Bonds
    if suffix in ('SG', 'GB'):
        return 'SGB'
    if suffix == 'GS':
        return 'GSEC'

    # Corporate Bonds / Debentures
    if suffix.startswith(('N', 'Y', 'Z', 'M', 'D')):
        return 'BOND'

    # Other specific instrument types
    if suffix.startswith('P'):
        return 'PREFERENCE_SHARE'
    if suffix == 'RR':
        return 'RIGHTS'
    if suffix.startswith('W'):
        return 'WARRANT'

    # If it's a mutual fund symbol from the CM segment
    if suffix == 'MF':
        return 'MF'

    # If no specific rule matches, mark as unknown
    return 'UNKNOWN'


def process_capital_market_master(data: dict, exchange: str, segment: str, db_manager: DatabaseManager):
    """
    Processes a dictionary of symbols from a Capital Market master file and syncs with the DB.
    This version now correctly handles and inserts INDEX symbols.
    """
    logger.info(f"Processing {len(data)} symbols for {exchange}:{segment}...")
    
    with db_manager.Session() as session:
        stmt = select(Security.symbol).where(
            Security.exchange == exchange,
            Security.segment == segment
        )
        db_symbols_set = set(session.execute(stmt).scalars().all())
        
        new_records_to_add = {s: i for s, i in data.items() if s not in db_symbols_set}

        if not new_records_to_add:
            logger.info("No new symbols to add for this segment.")
            return

        logger.info(f"Identified {len(new_records_to_add)} new instruments to add.")
        
        for symbol, item in new_records_to_add.items():
            isin_value = item.get('isin') or None
            
            security_type = classify_security(symbol, isin_value)

            if security_type in ('MF', 'UNKNOWN'):
                logger.info(f"Skipping symbol {symbol} (type: {security_type}).")
                continue

            new_security = Security(
                symbol=symbol,
                name=item['symbolDetails'],
                security_type=security_type,
                exchange=exchange,
                segment=segment,
                isin=isin_value,
                valid_from=datetime.utcnow()
            )
            session.add(new_security)
            
            if security_type == 'EQUITY':
                session.flush()
                equity_meta = SecuritiesEquityMeta(
                    security_id=new_security.id,
                    lot_size=item['minLotSize'],
                    tick_size=item['tickSize'],
                    company_name=item['symbolDetails']
                )
                session.add(equity_meta)
        
        session.commit()
    logger.info(f"Finished processing symbols for {exchange}:{segment}.")


def process_derivative_master(data: dict, exchange: str, segment: str, db_manager: DatabaseManager):
    """
    Processes a dictionary of derivative symbols (Futures & Options) and syncs with the DB.
    This version uses the correct SQLAlchemy pattern for creating related objects.
    """
    logger.info(f"Processing {len(data)} derivative symbols for {exchange}:{segment}...")

    with db_manager.Session() as session:
        logger.info("Fetching existing derivative symbols from DB for comparison...")
        stmt = select(Security.symbol).where(Security.exchange == exchange, Security.segment == segment)
        db_symbols_set = set(session.execute(stmt).scalars().all())
        logger.info(f"Found {len(db_symbols_set)} existing symbols. Identifying new ones...")

        new_records_to_process = {s: i for s, i in data.items() if s not in db_symbols_set}

        if not new_records_to_process:
            logger.info("No new derivative symbols to add.")
            return

        total_new = len(new_records_to_process)
        logger.info(f"Identified {total_new} new derivatives to add. Starting batch insert...")
        
        for i, (symbol, item) in enumerate(new_records_to_process.items()):
            if (i + 1) % 10000 == 0:
                logger.info(f"Processed {i + 1} / {total_new} new derivatives...")

            option_type = item.get('optType')
            if option_type == 'XX':
                security_type = 'FUTURE'
            elif option_type in ('CE', 'PE'):
                security_type = 'OPTION'
            else:
                continue

            isin_value = item.get('isin') or None

            new_security = Security(
                symbol=symbol,
                name=item['symbolDetails'],
                security_type=security_type,
                exchange=exchange,
                segment=segment,
                isin=isin_value,
                valid_from=datetime.utcnow()
            )

            try:
                expiry_ts = int(item['expiryDate'])
                expiry_date_obj = datetime.fromtimestamp(expiry_ts).date()
            except (ValueError, TypeError):
                logger.error(f"Could not parse expiryDate for {symbol}. Skipping.")
                continue

            # 2. Create the child object WITHOUT the 'security' keyword
            derivative_meta = SecuritiesDerivativeMeta(
                underlying_symbol=item['underSym'],
                instrument_type='FUT' if security_type == 'FUTURE' else 'OPT',
                expiry_date=expiry_date_obj,
                strike_price=item['strikePrice'] if security_type == 'OPTION' else None,
                option_type=option_type if security_type == 'OPTION' else None,
                lot_size=item['minLotSize'],
                tick_size=item['tickSize']
            )

            new_security.derivative_meta = derivative_meta

            session.add(new_security)

        logger.info("All new derivatives processed in memory. Committing to database...")
        session.commit()
        logger.info(f"Successfully committed {total_new} new derivatives to the database.")