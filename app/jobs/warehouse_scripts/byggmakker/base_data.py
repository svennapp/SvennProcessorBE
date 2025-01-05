# app/jobs/warehouse_scripts/byggmakker/base_data.py

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path
import json
import argparse
import os
import flask
from app.jobs.common.base_processor import BaseProcessor
from app.jobs.common.database_manager import DatabaseManager
from app.jobs.utils.logging_config import setup_script_logging

@dataclass
class ProductData:
    """Data class for product information"""
    ean: str
    name: str
    unit: str
    price_unit: str
    nobb: str
    images: List[str]

class BaseByggmakkerProcessor(BaseProcessor):
    """Processes Byggmakker base data from raw database to svenn products database"""

    def __init__(self, db_manager: DatabaseManager, batch_size: int = 100):
        """
        Initialize the processor with a database manager

        Args:
            db_manager (DatabaseManager): Initialized database manager instance
            batch_size (int): Number of records to process in each batch
        """
        super().__init__(db_manager, batch_size)
        self.updated_count = 0
        self.created_count = 0

    def _fetch_raw_data(self) -> List[Dict[str, Any]]:
        """Fetch data from raw_data database"""
        try:
            query = """
                SELECT b.name, b.ean, b.product_id, b.images, 
                       COALESCE(s.sales_unit, e.sales_unit) as sales_unit,
                       COALESCE(s.comparison_price_unit, e.comparison_price_unit) as comparison_price_unit
                FROM byggmakker_base_data b
                LEFT JOIN byggmakker_retailer_store_unit s ON b.ean = s.ean
                LEFT JOIN byggmakker_retailer_ecom_unit e ON b.ean = e.ean
                WHERE b.ean IS NOT NULL
            """
            return self.db_manager.execute_query('raw_data', query)
        except Exception as e:
            self.logger.error(f"Failed to fetch raw data: {e}")
            raise

    def validate_ean(self, ean_code: Any) -> Optional[str]:
        """
        Validates an EAN code

        Args:
            ean_code: The code to validate (str or int)

        Returns:
            Optional[str]: Validated EAN code or None if invalid
        """
        try:
            ean_str = str(ean_code).strip()
            if not ean_str.isdigit():
                self.logger.warning(f"Invalid EAN (non-digits): {ean_str}")
                return None
            if len(ean_str) not in [12, 13, 14]:
                self.logger.warning(f"Invalid EAN length: {ean_str}")
                return None
            return ean_str
        except (TypeError, ValueError) as e:
            self.logger.warning(f"EAN validation failed: {e}")
            return None

    def process_record(self, raw_product: Dict[str, Any]) -> None:
        """Process a single product record"""
        try:
            ean = self.validate_ean(raw_product.get('ean'))
            if not ean:
                return

            # Parse images from JSON string if needed
            images = raw_product.get('images', '[]')
            if isinstance(images, str):
                try:
                    images = json.loads(images)
                except json.JSONDecodeError:
                    images = []

            product_data = ProductData(
                ean=ean,
                name=raw_product.get('name', ''),
                unit=raw_product.get('sales_unit', ''),
                price_unit=raw_product.get('comparison_price_unit', ''),
                nobb=raw_product.get('product_id', ''),
                images=images if isinstance(images, list) else []
            )

            if not all([product_data.name, product_data.unit, product_data.price_unit]):
                self.logger.warning(f"Skipping product with EAN {ean}: Missing required data")
                return

            self._process_product(product_data)

        except Exception as e:
            self.logger.error(f"Error processing product with EAN {raw_product.get('ean')}: {e}")
            raise

    def _process_product(self, product_data: ProductData) -> None:
        """
        Process a single product

        Args:
            product_data (ProductData): Product data to process
        """
        try:
            with self.db_manager.transaction('svenn_products') as cursor:
                # Check if product exists
                cursor.execute(
                    "SELECT product_id FROM ean_codes WHERE ean_code = %s",
                    (product_data.ean,)
                )
                result = cursor.fetchone()

                if result:
                    self._update_product(cursor, product_data, result['product_id'])
                else:
                    self._insert_product(cursor, product_data)

        except Exception as e:
            self.logger.error(f"Error processing product {product_data.ean}: {e}")
            raise

    def _update_product(self, cursor, product_data: ProductData, product_id: int) -> None:
        """
        Update existing product

        Args:
            cursor: Database cursor
            product_data (ProductData): Product data to update
            product_id (int): ID of the product to update
        """
        cursor.execute("""
            UPDATE products 
            SET base_name = %s, base_unit = %s, base_price_unit = %s, updated = NOW()
            WHERE product_id = %s
        """, (product_data.name, product_data.unit, product_data.price_unit, product_id))
        self.updated_count += 1
        self.logger.info(f"Updated product: {product_data.ean}")

    def _insert_product(self, cursor, product_data: ProductData) -> None:
        """
        Insert new product

        Args:
            cursor: Database cursor
            product_data (ProductData): Product data to insert
        """
        # Insert into products table
        cursor.execute("""
            INSERT INTO products (base_name, base_unit, base_price_unit)
            VALUES (%s, %s, %s)
        """, (product_data.name, product_data.unit, product_data.price_unit))
        product_id = cursor.lastrowid

        # Insert EAN code
        cursor.execute("""
            INSERT INTO ean_codes (ean_code, product_id)
            VALUES (%s, %s)
        """, (product_data.ean, product_id))

        # Insert NOBB code if present
        if product_data.nobb:
            cursor.execute("""
                INSERT INTO nobb_codes (nobb_code, product_id)
                VALUES (%s, %s)
            """, (product_data.nobb, product_id))

        # Insert images
        for image_url in product_data.images:
            cursor.execute("""
                INSERT INTO product_images (product_id, image_url)
                VALUES (%s, %s)
            """, (product_id, image_url))

        self.created_count += 1
        self.logger.info(f"Inserted new product: {product_data.ean}")

def main():
    """Main execution function"""
    logger = setup_script_logging("base_byggmakker")
    logger.info("Starting base_byggmakker script")

    # Check if running in Flask context
    if flask.has_app_context():
        logger.info("Running in Flask context")
        env_path = Path(os.getcwd()) / '.env'
        batch_size = 100
    else:
        logger.info("Running in standalone mode")
        parser = argparse.ArgumentParser(description="Process Byggmakker data.")
        parser.add_argument(
            "--env",
            type=str,
            default=str(Path(os.getcwd()) / '.env'),
            help="Path to the environment file"
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of products to process in each batch"
        )
        args = parser.parse_args()
        env_path = Path(args.env)
        batch_size = args.batch_size

    db_manager = None
    try:
        logger.info(f"Using env file at: {env_path.absolute()}")
        if not env_path.exists():
            logger.error(f"Environment file not found at: {env_path.absolute()}")
            raise FileNotFoundError(f"Environment file not found at: {env_path.absolute()}")

        db_manager = DatabaseManager(env_path)
        logger.info("DatabaseManager initialized successfully")

        # Initialize and run processor
        logger.info("Initializing BaseByggmakkerProcessor")
        processor = BaseByggmakkerProcessor(db_manager, batch_size)
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