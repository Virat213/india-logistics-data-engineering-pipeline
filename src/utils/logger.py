"""
Logging utility module providing standardized console and file logging.
"""

import logging
import sys
from datetime import datetime

def get_logger(name: str = "IndiaLogisticsPipeline") -> logging.Logger:
    """
    Returns a configured Python logger with consistent formatting.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger
