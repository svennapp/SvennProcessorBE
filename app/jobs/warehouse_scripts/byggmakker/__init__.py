# app/jobs/warehouse_scripts/byggmakker/__init__.py

from .base_data import BaseByggmakkerProcessor
from .store_data import StoreDataProcessor
from .prices import StorePriceProcessor
from .retailer_data import RetailerByggmakkerProcessor

# Constants
WAREHOUSE_NAME = "Byggmakker"
RETAILER_ID = 1

# Define what should be publicly available when importing from this package
__all__ = [
    'BaseByggmakkerProcessor',
    'StoreDataProcessor',
    'StorePriceProcessor',
    'RetailerByggmakkerProcessor',
    'WAREHOUSE_NAME',
    'RETAILER_ID',
    'run_all_processors'
]


def run_all_processors(db_manager):
    """
    Run all processors for Byggmakker warehouse in the correct order

    Args:
        db_manager: DatabaseManager instance for database connections
    """
    processors = [
        BaseByggmakkerProcessor(db_manager),
        StoreDataProcessor(db_manager),
        RetailerByggmakkerProcessor(db_manager),
        StorePriceProcessor(db_manager)
    ]

    for processor in processors:
        try:
            processor.process_all()
        except Exception as e:
            # Log error but continue with next processor
            processor.logger.error(f"Error running {processor.__class__.__name__}: {e}")
            continue


# Optional: Add any warehouse-specific utility functions here
def get_processor_by_name(processor_name: str, db_manager):
    """
    Get a processor instance by its name

    Args:
        processor_name: Name of the processor to instantiate
        db_manager: DatabaseManager instance for database connections

    Returns:
        Instance of the requested processor

    Raises:
        ValueError: If processor name is not found
    """
    processors = {
        'base': BaseByggmakkerProcessor,
        'store': StoreDataProcessor,
        'price': StorePriceProcessor,
        'retailer': RetailerByggmakkerProcessor
    }

    processor_class = processors.get(processor_name.lower())
    if not processor_class:
        raise ValueError(f"Unknown processor: {processor_name}")

    return processor_class(db_manager)