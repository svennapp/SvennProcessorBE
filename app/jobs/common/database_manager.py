import os
import logging
from contextlib import contextmanager
import pymysql
from typing import Optional, Any
from dotenv import load_dotenv
from pathlib import Path
from pymysql.cursors import DictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations for multiple databases"""

    def __init__(self, env_file: Optional[Path] = None):
        """
        Initialize the database manager

        Args:
            env_file (Optional[Path]): Path to the .env file. If None, looks in current directory
        """
        self.connections = {}
        # Load environment variables
        if env_file:
            if not env_file.exists():
                raise FileNotFoundError(f"Environment file not found: {env_file}")
            load_dotenv(env_file)
        else:
            load_dotenv()

        self._initialize_configs()

    # database_manager.py
    def _initialize_configs(self):
        """Initialize database configurations from environment variables"""
        try:
            self.db_configs = {
                'raw_data': {
                    'host': os.getenv('RAW_DB_HOST'),
                    'user': os.getenv('RAW_DB_USER'),
                    'password': os.getenv('RAW_DB_PASSWORD'),
                    'database': os.getenv('RAW_DB_NAME'),
                    'port': int(os.getenv('RAW_DB_PORT')),
                    'cursorclass': pymysql.cursors.DictCursor,
                    'charset': 'utf8mb4'
                },
                'svenn_products': {
                    'host': os.getenv('SVENN_DB_HOST'),
                    'user': os.getenv('SVENN_DB_USER'),
                    'password': os.getenv('SVENN_DB_PASSWORD'),
                    'database': os.getenv('SVENN_DB_NAME'),
                    'port': int(os.getenv('SVENN_DB_PORT')),
                    'cursorclass': pymysql.cursors.DictCursor,
                    'charset': 'utf8mb4'
                }
            }

            # Validate all required environment variables are present
            self._validate_configs()

        except Exception as e:
            logger.error(f"Failed to initialize database configurations: {e}")
            raise

    def _validate_configs(self):
        """Validate that all required configuration values are present"""
        required_fields = ['host', 'user', 'database', 'port']

        for db_name, config in self.db_configs.items():
            missing_fields = [field for field in required_fields if not config.get(field)]
            if missing_fields:
                raise ValueError(
                    f"Missing required configuration fields for {db_name}: {', '.join(missing_fields)}"
                )

    def get_connection(self, db_name: str) -> pymysql.Connection:
        """
        Get a database connection for the specified database

        Args:
            db_name (str): Name of the database configuration to use

        Returns:
            pymysql.Connection: Database connection

        Raises:
            KeyError: If database configuration not found
            pymysql.Error: If connection fails
        """
        if db_name not in self.db_configs:
            raise KeyError(f"No configuration found for database: {db_name}")

        if db_name not in self.connections or not self.connections[db_name].open:
            try:
                self.connections[db_name] = pymysql.connect(**self.db_configs[db_name])
            except pymysql.Error as e:
                logger.error(f"Failed to connect to {db_name}: {e}")
                raise

        return self.connections[db_name]

    @contextmanager
    def get_cursor(self, db_name: str):
        """Context manager for database cursor"""
        conn = self.get_connection(db_name)
        cursor = None
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed for {db_name}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def close_all_connections(self):
        """Close all database connections"""
        for db_name, conn in self.connections.items():
            if conn and conn.open:
                conn.close()
        self.connections.clear()

    @contextmanager
    def transaction(self, db_name: str):
        """Context manager for database transactions"""
        with self.get_cursor(db_name) as cursor:
            try:
                cursor.execute("START TRANSACTION")
                yield cursor
                cursor.execute("COMMIT")
            except Exception as e:
                cursor.execute("ROLLBACK")
                logger.error(f"Transaction failed for {db_name}: {e}")
                raise

    def execute_query(self, db_name: str, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query and return results"""
        with self.get_cursor(db_name) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()