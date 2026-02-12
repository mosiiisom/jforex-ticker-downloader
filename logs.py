# logs.py
import logging
import logging.handlers
from pathlib import Path

from config import load_config

cfg = load_config()
LOGS_DIR = Path(cfg["LOGS_PATH"])
LOGS_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "logs.log"

# Single formatter used everywhere
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)5s] %(name)-18s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def get_logger(
    name: str = "jforex_downloader",
    level: str = cfg.get("LOG_LEVEL", "WARNING"),
    console_level: str = cfg.get("CONSOLE_LOG_LEVEL", "WARNING"),
) -> logging.Logger:
    """
    Returns the shared application logger.
    All modules should use this same logger (or child loggers).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())

    # Prevent duplicate handlers
    if logger.hasHandlers():
        return logger

    # Rotating file handler ── single file for everything
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,     # 10 MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(level.upper())
    file_handler.setFormatter(formatter)

    # Console
    console = logging.StreamHandler()
    console.setLevel(console_level.upper())
    console.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console)

    logger.propagate = False
    return logger


# The main logger everyone imports
log = get_logger(
    name="jforex_downloader",
    level=cfg.get("LOG_LEVEL", "WARNING"),              # file log level
    console_level=cfg.get("CONSOLE_LOG_LEVEL", "WARNING")  # console level
)

# Optional: child loggers (if you want more granularity later)
app_log = logging.getLogger("jforex_downloader.app")
dl_log  = logging.getLogger("jforex_downloader.downloader")