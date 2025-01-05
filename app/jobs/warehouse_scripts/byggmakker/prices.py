# app/jobs/warehouse_scripts/byggmakker/prices.py

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from app.jobs.common.base_processor import BaseProcessor
from app.jobs.utils.logging_config import setup_script_logging


@dataclass
class PriceData:
    """Data class for store price information"""
    ean: str
    store_id: str
    price: Decimal
    comparison_price: Optional[Decimal] = None


class StorePriceProcessor(BaseProcessor):
    """Processes store price data from raw database to svenn products database"""

    def __init__(self, db_manager):
        """
        Initialize the processor with a database manager

        Args:
            db_manager: DatabaseManager instance
        """
        super().__init__(db_manager)
        self.processed_count = 0
        self.error_count = 0

    def _fetch_raw_data(self) -> List[Dict[str, Any]]:
        """Fetch price data from raw_data database"""
        query = """
            SELECT ean, store_id, price, comparison_price
            FROM byggmakker_store_prices
            WHERE ean IS NOT NULL 
            AND store_id IS NOT NULL 
            AND price IS NOT NULL
        """
        return self.db_manager.execute_query('raw_data', query)

    def _get_product_id_by_ean(self, cursor, ean_code: str) -> Optional[int]:
        """Get product_id from ean_codes table"""
        try:
            cursor.execute(
                "SELECT product_id FROM ean_codes WHERE ean_code = %s",
                (ean_code,)
            )
            result = cursor.fetchone()
            return result['product_id'] if result else None
        except Exception as e:
            self.logger.error(f"Error fetching product_id for EAN {ean_code}: {e}")
            return None

    def validate_price(self, price: Any) -> Optional[Decimal]:
        """
        Validate and convert price values

        Args:
            price: The price value to validate

        Returns:
            Optional[Decimal]: Validated price or None if invalid
        """
        try:
            if price is None:
                return None
            price_decimal = Decimal(str(price))
            if price_decimal < 0:
                self.logger.warning(f"Negative price value: {price}")
                return None
            return price_decimal
        except (InvalidOperation, TypeError, ValueError) as e:
            self.logger.warning(f"Invalid price value: {price}, error: {e}")
            return None

    def process_record(self, raw_price: Dict[str, Any]) -> None:
        """Process a single price record"""
        try:
            # Validate price values
            price = self.validate_price(raw_price.get('price'))
            comparison_price = self.validate_price(raw_price.get('comparison_price'))

            if not price:
                self.logger.warning(f"Skipping record: Invalid price for EAN {raw_price.get('ean')}")
                self.error_count += 1
                return

            price_data = PriceData(
                ean=raw_price['ean'],
                store_id=raw_price['store_id'],
                price=price,
                comparison_price=comparison_price
            )

            self._process_price_record(price_data)

        except Exception as e:
            self.logger.error(f"Error processing price record: {e}")
            self.error_count += 1
            raise

    def _process_price_record(self, price_data: PriceData) -> None:
        """
        Process a single price record

        Args:
            price_data: Price data to process
        """
        try:
            with self.db_manager.transaction('svenn_products') as cursor:
                # Get product_id for the EAN
                product_id = self._get_product_id_by_ean(cursor, price_data.ean)
                if not product_id:
                    self.logger.warning(f"No product_id found for EAN: {price_data.ean}")
                    self.error_count += 1
                    return

                # Upsert price record
                cursor.execute("""
                    INSERT INTO store_prices 
                        (store_id, product_id, price, comparison_price, created, updated)
                    VALUES 
                        (%s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE 
                        price = VALUES(price),
                        comparison_price = VALUES(comparison_price),
                        updated = NOW()
                """, (
                    price_data.store_id,
                    product_id,
                    price_data.price,
                    price_data.comparison_price
                ))
                self.processed_count += 1
                self.logger.debug(f"Processed price for EAN {price_data.ean} in store {price_data.store_id}")

        except Exception as e:
            self.logger.error(f"Error processing price record for EAN {price_data.ean}: {e}")
            self.error_count += 1
            raise


def main():
    """Main execution function"""
    import os
    import flask
    from pathlib import Path
    from app.jobs.common.database_manager import DatabaseManager

    logger = setup_script_logging("store_prices")
    logger.info("Starting store_prices script")

    # Check if running in Flask context
    if flask.has_app_context():
        logger.info("Running in Flask context")
        env_path = Path(os.getcwd()) / '.env'
    else:
        import argparse
        logger.info("Running in standalone mode")
        parser = argparse.ArgumentParser(description="Process Byggmakker store prices.")
        parser.add_argument(
            "--env",
            type=str,
            default=str(Path(os.getcwd()) / '.env'),
            help="Path to the environment file"
        )
        args = parser.parse_args()
        env_path = Path(args.env)

    db_manager = None
    try:
        logger.info(f"Using env file at: {env_path.absolute()}")
        if not env_path.exists():
            logger.error(f"Environment file not found at: {env_path.absolute()}")
            raise FileNotFoundError(f"Environment file not found at: {env_path.absolute()}")

        db_manager = DatabaseManager(env_path)
        logger.info("DatabaseManager initialized successfully")

        # Initialize and run processor
        logger.info("Initializing StorePriceProcessor")
        processor = StorePriceProcessor(db_manager)
        logger.info("Starting process_all")
        processor.process_all()

    except Exception as e:
        logger.error(f"Script execution failed: {e}", exc_info=True)
        raise
    finally:
        if db_manager:
            logger.info("Closing database connections")
            db_manager.close_all_connections()


if __name__ == "__main__":
    main()