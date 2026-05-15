import logging

from src.storage.paths import PROJECT_ROOT


# Folder where all log files will be stored.
LOG_DIR = PROJECT_ROOT / "logs"

# Main log file for pipeline runs.
LOG_FILE = LOG_DIR / "pipeline.log"


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.

    This logger writes messages to:
    1. The terminal
    2. logs/pipeline.log

    The name usually comes from __name__, so each file gets its own logger name.
    Example: src.pipeline.eia_pipeline
    """

    # Create the logs/ folder if it does not already exist.
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Get a logger with the given name.
    logger = logging.getLogger(name)

    # INFO means we track normal pipeline events, warnings, and errors.
    logger.setLevel(logging.INFO)

    # Prevent messages from being passed to the root logger,
    # which can cause duplicate log messages.
    logger.propagate = False

    # If this logger already has handlers, reuse it.
    # This prevents duplicate logs if get_logger() is called multiple times.
    if logger.handlers:
        return logger

    # Define how each log message should look.
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Handler for printing log messages to the terminal.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Handler for saving log messages to logs/pipeline.log.
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)

    # Attach both handlers to the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Return the fully configured logger.
    return logger