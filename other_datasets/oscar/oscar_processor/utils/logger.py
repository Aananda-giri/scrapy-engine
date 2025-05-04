"""
Logging configuration for OSCAR data processor.
"""
import logging
import sys
from pathlib import Path
from typing import Optional, Literal

def setup_logging(
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
    log_file: Optional[str] = "oscar_processor.log",
    console_output: bool = True
) -> logging.Logger:
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Path to log file, None for no file logging
        console_output: Whether to output logs to console
        
    Returns:
        Logger instance
    """
    # Convert string log level to numeric value
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Format for log messages
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Add file handler if log_file is specified
    if log_file:
        # Create directory for log file if it doesn't exist
        log_path = Path(log_file)
        if log_path.parent and not log_path.parent.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add console handler if enabled
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger