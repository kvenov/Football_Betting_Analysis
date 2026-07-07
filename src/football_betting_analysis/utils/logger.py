import logging
import sys
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """
    Optional console formatter with colors.
    """

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
        "RESET": "\033[0m",
    }

    def format(self, record):

        color = self.COLORS.get(
            record.levelname,
            self.COLORS["RESET"],
        )

        message = super().format(record)

        return (
            f"{color}"
            f"{message}"
            f"{self.COLORS['RESET']}"
        )


def setup_logger(
    logging_config: dict,
    mode: str,
):
    """
    Central logging configuration.
    Creates separated log files depending on workflow.

    Example:
        mode="data"
            logs/data.log
        mode="train"
            logs/training.log

        mode="predict"
            logs/prediction.log


    Config example:
    logging:
        - level: INFO
        - directory: logs
        - train_file: training.log
        - predict_file: prediction.log
        - console: true

    """

    log_level = getattr(
        logging,
        logging_config.get(
            "level",
            "INFO",
        ),
    )

    log_directory = Path(
        logging_config.get(
            "directory",
            "logs",
        )
    )

    log_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Select log file based on functionality
    if mode == "data":
        log_file = (
            logging_config.get(
                "data_file",
                "data.log",
            )
        )
    elif mode == "train":
        log_file = (
            logging_config.get(
                "train_file",
                "training.log",
            )
        )
    elif mode == "predict":
        log_file = (
            logging_config.get(
                "predict_file",
                "prediction.log",
            )
        )
    elif mode == "evaluate":
        log_file = (
            logging_config.get(
                "evaluate_file",
                "evaluation.log",
            )
        )
    else:
        log_file = (
            logging_config.get(
                "default_file",
                "application.log",
            )
        )


    log_path = (
        log_directory
        /
        log_file
    )

    # Formatter
    file_formatter = logging.Formatter(
        fmt=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # COnsole formatter
    console_formatter = ColoredFormatter(
        fmt=(
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        )
    )

    # Root logger
    logger = logging.getLogger()

    logger.setLevel(
        log_level
    )

    # Remove previous handlers
    # prevents duplicate logs when rerunning experiments
    if logger.handlers:
        logger.handlers.clear()


    # File Handler
    file_handler = logging.FileHandler(
        log_path,
        mode="a",
        encoding="utf-8",
    )

    file_handler.setLevel(
        log_level
    )

    file_handler.setFormatter(
        file_formatter
    )

    logger.addHandler(
        file_handler
    )

    # Console Handler
    if logging_config.get(
        "console",
        True,
    ):
        console_handler = logging.StreamHandler(
            sys.stdout
        )
        
        console_handler.setLevel(
            log_level
        )

        console_handler.setFormatter(
            console_formatter
        )

        logger.addHandler(
            console_handler
        )


    logger.info(
        "Logger initialized."
    )

    logger.info(
        "Log file: %s",
        log_path
    )

    return logger