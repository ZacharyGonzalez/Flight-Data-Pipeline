import logging
import os
import time
from datetime import datetime


def make_logger():
    """Makes a logger that will append to the daily log file, or else make it"""
    os.environ["TZ"] = "America/New_York"
    os.makedirs("./logs", exist_ok=True)
    FILE_MODE = "w"
    now = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"./logs/etl_{now}.log"
    if os.path.exists(log_filename):
        FILE_MODE = "a"
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]: %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        filename=log_filename,
        filemode=FILE_MODE,
        encoding="utf-8",
        level=logging.INFO,
    )
    logger.info("Logger Initialization success.")
    return logger


def log_function_call(func):
    def wrapper(*args, **kwargs):
        logging.info("Calling {%s}.", func.__name__)
        result = func(*args, **kwargs)
        logging.info("{%s} completed.", func.__name__)
        return result

    return wrapper


def log_function_call_with_params(func):
    def wrapper(*args, **kwargs):
        logging.info(
            "Calling {%s} with args {%s} and kwargs {%s}", func.__name__, args, kwargs
        )
        result = func(*args, **kwargs)
        logging.info("{%s} completed", func.__name__)
        return result

    return wrapper
