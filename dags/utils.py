import logging
from logging import Logger
from sys import stdout


def get_logger(logger_name: str) -> Logger:

    logger = logging.getLogger(name=logger_name)

    logger.setLevel(level=logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger_handler = logging.StreamHandler(stream=stdout)
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)

    return logger


if __name__ == "__main__":
    get_logger()
