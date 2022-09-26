import logging
import sys


def logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    log_handler.setLevel(logging.DEBUG)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    return logger
