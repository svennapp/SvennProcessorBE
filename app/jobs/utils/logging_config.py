# In app/jobs/utils/logging_config.py

import logging
import os
from pathlib import Path


def setup_script_logging(script_name: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Configure logging for a script

    Args:
        script_name: Name of the script (used for logger name)
        log_level: Logging level (default: INFO)

    Returns:
        configured logger instance
    """
    logger = logging.getLogger(script_name)
    logger.setLevel(log_level)

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create formatter with standardized timestamp format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                  '%Y-%m-%d %H:%M:%S')

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler
    try:
        # Ensure logs directory exists
        log_dir = Path(__file__).parent.parent.parent / 'logs'
        os.makedirs(log_dir, exist_ok=True)

        log_file = log_dir / 'script_executions.log'
        file_handler = logging.FileHandler(str(log_file), encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.info(f"Logging initialized for {script_name}")

    except Exception as e:
        logger.error(f"Failed to set up file logging: {e}")
        raise

    return logger