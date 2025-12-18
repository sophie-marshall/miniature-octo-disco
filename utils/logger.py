import logging
import sys
import json


def parse_log_level(level_str: str, default=logging.INFO) -> int:
    level = logging.getLevelName(level_str.upper())
    return level if isinstance(level, int) else default


def get_logger(
    name: str = "pos_pipeline_logger",
    config_path: str = "config.json",
) -> logging.Logger:
    with open(config_path, "r") as f:
        config = json.load(f)

    logger = logging.getLogger(name)

    log_level = parse_log_level(config["logging"].get("level", "INFO"))
    logger.setLevel(log_level)

    logger.propagate = config["logging"].get("propagate", False)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)

        formatter = logging.Formatter(
            "[%(levelname)s] %(asctime)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
