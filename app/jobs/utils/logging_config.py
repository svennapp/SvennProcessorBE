# app/jobs/utils/logging_config.py
import logging
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

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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