# app/jobs/warehouse_scripts/byggmakker/store_data.py

from typing import Dict, Any, List
from dataclasses import dataclass
from app.jobs.common.base_processor import BaseProcessor
from app.jobs.utils.logging_config import setup_script_logging


@dataclass
class StoreData:
    """Data class for store information"""
    store_id: str
    store_name: str


class StoreDataProcessor(BaseProcessor):
    """Processes store data from raw database to svenn products database"""

    RETAILER_ID = 1  # Hardcoded retailer_id for Byggmakker

    def __init__(self, db_manager):
        """
        Initialize the processor with a database manager

        Args:
            db_manager: DatabaseManager instance
        """
        super().__init__(db_manager)
        self.inserted_count = 0
        self.updated_count = 0
        self.skipped_count = 0

    def _fetch_raw_data(self) -> List[Dict[str, Any]]:
        """Fetch store data from raw_data database"""
        query = """
            SELECT store_id, store_name 
            FROM byggmakker_store_ids 
            ORDER BY store_id
        """
        return self.db_manager.execute_query('raw_data', query)

    def process_record(self, raw_store: Dict[str, Any]) -> None:
        """Process a single store record"""
        try:
            store = StoreData(
                store_id=raw_store['store_id'],
                store_name=raw_store['store_name']
            )

            if not all([store.store_id, store.store_name]):
                self.logger.warning(f"Skipping store: Missing required data")
                return

            self._process_store(store)

        except Exception as e:
            self.logger.error(f"Error processing store {raw_store.get('store_id')}: {e}")
            raise

    def _process_store(self, store: StoreData) -> None:
        """
        Process a single store

        Args:
            store: Store data to process
        """
        try:
            with self.db_manager.transaction('svenn_products') as cursor:
                # Check if store exists
                cursor.execute(
                    "SELECT store_id, store_name FROM stores WHERE store_id = %s",
                    (store.store_id,)
                )
                existing_store = cursor.fetchone()

                if existing_store:
                    # Update if store name has changed
                    if existing_store['store_name'] != store.store_name:
                        cursor.execute("""
                            UPDATE stores 
                            SET store_name = %s 
                            WHERE store_id = %s
                        """, (store.store_name, store.store_id))
                        self.updated_count += 1
                        self.logger.info(f"Updated store: {store.store_name} (ID: {store.store_id})")
                    else:
                        self.skipped_count += 1
                        self.logger.debug(f"Skipped existing store: {store.store_name} (ID: {store.store_id})")
                else:
                    # Insert new store
                    cursor.execute("""
                        INSERT INTO stores (store_id, retailer_id, store_name)
                        VALUES (%s, %s, %s)
                    """, (store.store_id, self.RETAILER_ID, store.store_name))
                    self.inserted_count += 1
                    self.logger.info(f"Inserted new store: {store.store_name} (ID: {store.store_id})")

        except Exception as e:
            self.logger.error(f"Error processing store {store.store_id}: {e}")
            raise

    def _log_summary(self) -> None:
        """Override base class log_summary to include store-specific stats"""
        super()._log_summary()
        self.logger.info("\nStore Processing Details:")
        self.logger.info(f"New stores inserted: {self.inserted_count}")
        self.logger.info(f"Existing stores updated: {self.updated_count}")
        self.logger.info(f"Stores skipped (no changes): {self.skipped_count}")


def main():
    """Main execution function"""
    import os
    import flask
    from pathlib import Path
    from app.jobs.common.database_manager import DatabaseManager

    logger = setup_script_logging("store_byggmakker")
    logger.info("Starting store_byggmakker script")

    # Check if running in Flask context
    if flask.has_app_context():
        logger.info("Running in Flask context")
        env_path = Path(os.getcwd()) / '.env'
    else:
        import argparse
        logger.info("Running in standalone mode")
        parser = argparse.ArgumentParser(description="Process Byggmakker store data.")
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
        logger.info("Initializing StoreDataProcessor")
        processor = StoreDataProcessor(db_manager)
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