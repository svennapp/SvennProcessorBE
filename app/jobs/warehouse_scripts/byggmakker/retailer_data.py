# app/jobs/warehouse_scripts/byggmakker/retailer_data.py

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from app.jobs.common.base_processor import BaseProcessor
from app.jobs.utils.logging_config import setup_script_logging


@dataclass
class RetailerProduct:
    """Data class for retailer product information"""
    ean: str
    name: str
    brand: str
    category: str
    sales_unit: Optional[str] = None
    price_comparison_unit: Optional[str] = None


class RetailerByggmakkerProcessor(BaseProcessor):
    """Processes Byggmakker retailer data from raw database to svenn products database"""

    RETAILER_ID = 1  # Hardcoded retailer_id for Byggmakker
    BASE_URL = "https://www.byggmakker.no/produkt/"

    def __init__(self, db_manager):
        """
        Initialize the processor with a database manager

        Args:
            db_manager: Database manager instance
        """
        super().__init__(db_manager)
        self.updated_count = 0
        self.created_count = 0

    def _fetch_raw_data(self) -> List[Dict[str, Any]]:
        """Fetch base product data from raw_data database"""
        query = """
            SELECT 
                b.name, b.ean, b.brand, b.category,
                COALESCE(s.sales_unit, e.sales_unit) as sales_unit,
                COALESCE(s.comparison_price_unit, e.comparison_price_unit) as comparison_price_unit
            FROM byggmakker_base_data b
            LEFT JOIN byggmakker_retailer_store_unit s ON b.ean = s.ean
            LEFT JOIN byggmakker_retailer_ecom_unit e ON b.ean = e.ean
            WHERE b.ean IS NOT NULL
        """
        return self.db_manager.execute_query('raw_data', query)

    def _get_product_id_by_ean(self, cursor, ean_code: str) -> Optional[int]:
        """Get product_id from ean_codes table"""
        cursor.execute(
            "SELECT product_id FROM ean_codes WHERE ean_code = %s",
            (ean_code,)
        )
        result = cursor.fetchone()
        return result['product_id'] if result else None

    def _insert_or_update_category(self, cursor, category_name: str) -> Optional[int]:
        """Insert or update category and return category_id"""
        try:
            # Check if category exists
            cursor.execute(
                "SELECT category_id FROM categories WHERE category_name = %s",
                (category_name,)
            )
            result = cursor.fetchone()

            if result:
                return result['category_id']

            # Insert new category
            cursor.execute(
                "INSERT INTO categories (category_name) VALUES (%s)",
                (category_name,)
            )
            return cursor.lastrowid

        except Exception as e:
            self.logger.error(f"Error managing category {category_name}: {e}")
            return None

    def _generate_product_url(self, product_name: str, ean: str) -> str:
        """Generate product URL from name and EAN"""
        import re
        url_name = product_name.strip()
        url_name = re.sub(r'[^a-zA-Z0-9]+', '-', url_name)
        url_name = re.sub(r'-+', '-', url_name)
        url_name = url_name.strip('-')
        return f"{self.BASE_URL}{url_name}/{ean}"

    def validate_ean(self, ean_code: Any) -> Optional[str]:
        """Validate EAN code format"""
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

            product = RetailerProduct(
                ean=ean,
                name=raw_product.get('name', ''),
                brand=raw_product.get('brand', ''),
                category=raw_product.get('category', ''),
                sales_unit=raw_product.get('sales_unit'),
                price_comparison_unit=raw_product.get('comparison_price_unit')
            )

            if not all([product.name, product.category]):
                self.logger.warning(f"Skipping product with EAN {ean}: Missing required data")
                return

            self._process_retailer_product(product)

        except Exception as e:
            self.logger.error(f"Error processing product with EAN {raw_product.get('ean')}: {e}")
            raise

    def _process_retailer_product(self, product: RetailerProduct) -> None:
        """Process a single retailer product"""
        try:
            with self.db_manager.transaction('svenn_products') as cursor:
                # Get product_id
                product_id = self._get_product_id_by_ean(cursor, product.ean)
                if not product_id:
                    self.logger.warning(f"No product_id found for EAN: {product.ean}")
                    return

                # Get or create category
                category_id = self._insert_or_update_category(cursor, product.category)
                if not category_id:
                    self.logger.warning(f"Failed to process category for EAN: {product.ean}")
                    return

                # Generate product URL
                url_product = self._generate_product_url(product.name, product.ean)

                # Check if retailer product exists
                cursor.execute("""
                    SELECT product_id 
                    FROM retailers_products 
                    WHERE product_id = %s AND retailer_id = %s
                """, (product_id, self.RETAILER_ID))

                if cursor.fetchone():
                    # Update existing product
                    cursor.execute("""
                        UPDATE retailers_products
                        SET variant_name = %s, brand = %s, category_id = %s,
                            retail_unit = %s, retail_price_comparison_unit = %s,
                            url_product = %s, updated = NOW()
                        WHERE product_id = %s AND retailer_id = %s
                    """, (
                        product.name, product.brand, category_id,
                        product.sales_unit, product.price_comparison_unit,
                        url_product, product_id, self.RETAILER_ID
                    ))
                    self.updated_count += 1
                    self.logger.info(f"Updated retailer product: {product.ean}")
                else:
                    # Insert new product
                    cursor.execute("""
                        INSERT INTO retailers_products (
                            retailer_id, product_id, variant_name, brand,
                            category_id, retail_unit, retail_price_comparison_unit,
                            url_product, created, updated
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """, (
                        self.RETAILER_ID, product_id, product.name, product.brand,
                        category_id, product.sales_unit, product.price_comparison_unit,
                        url_product
                    ))
                    self.created_count += 1
                    self.logger.info(f"Inserted new retailer product: {product.ean}")

        except Exception as e:
            self.logger.error(f"Error processing retailer product {product.ean}: {e}")
            raise


def main():
    """Main execution function"""
    import os
    import flask
    from pathlib import Path
    from app.jobs.common.database_manager import DatabaseManager

    logger = setup_script_logging("retailer_byggmakker")
    logger.info("Starting retailer_byggmakker script")

    # Check if running in Flask context
    if flask.has_app_context():
        logger.info("Running in Flask context")
        env_path = Path(os.getcwd()) / '.env'
    else:
        import argparse
        logger.info("Running in standalone mode")
        parser = argparse.ArgumentParser(description="Process Byggmakker retailer data.")
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
        logger.info("Initializing RetailerByggmakkerProcessor")
        processor = RetailerByggmakkerProcessor(db_manager)
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