# app/jobs/common/base_processor.py
import logging
from typing import Dict, Any, List
from pathlib import Path
from abc import ABC, abstractmethod
from datetime import datetime


class BaseProcessor(ABC):
    """Base class for all data processors"""

    def __init__(self, db_manager, batch_size: int = 100):
        """
        Initialize the base processor

        Args:
            db_manager: Database manager instance
            batch_size: Size of batches for processing
        """
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.processed_count = 0
        self.error_count = 0
        self.start_time = None

        # Setup logging
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the processor"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        # Remove any existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create formatters and handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                      '%Y-%m-%d %H:%M:%S')  # Removed microseconds from format

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File Handler
        try:
            log_file = Path(__file__).parent.parent.parent / 'logs' / 'script_executions.log'
            file_handler = logging.FileHandler(str(log_file))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to set up file logging: {e}")

        return logger

    @abstractmethod
    def _fetch_raw_data(self) -> List[Dict[str, Any]]:
        """
        Fetch raw data from source database
        Must be implemented by child classes
        """
        pass

    @abstractmethod
    def process_record(self, record: Dict[str, Any]) -> None:
        """
        Process a single record
        Must be implemented by child classes
        """
        pass

    def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of records"""
        for record in batch:
            try:
                self.process_record(record)
                self.processed_count += 1
            except Exception as e:
                self.logger.error(f"Error processing record: {e}")
                self.error_count += 1

    def process_all(self) -> None:
        """Process all records in batches"""
        self.start_time = datetime.utcnow()
        try:
            self.logger.info(f"Starting {self.__class__.__name__} processing")
            raw_data = self._fetch_raw_data()

            if not raw_data:
                self.logger.warning("No data found to process")
                return

            total_batches = (len(raw_data) + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Found {len(raw_data)} records to process in {total_batches} batches")

            for i in range(0, len(raw_data), self.batch_size):
                batch = raw_data[i:i + self.batch_size]
                current_batch = (i // self.batch_size) + 1
                self.logger.info(f"Processing batch {current_batch}/{total_batches}")

                try:
                    self.process_batch(batch)
                except Exception as e:
                    self.logger.error(f"Error processing batch {current_batch}: {e}")
                    raise

            self._log_summary()

        except Exception as e:
            self.logger.error(f"Error during batch processing: {e}")
            raise

    def _log_summary(self) -> None:
        """Log processing summary"""
        end_time = datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        self.logger.info("\nProcessing Summary:")
        self.logger.info(f"Total records processed: {self.processed_count}")
        self.logger.info(f"Total errors: {self.error_count}")
        self.logger.info(f"Processing completed in {duration:.2f} seconds")