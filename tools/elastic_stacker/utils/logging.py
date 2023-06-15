import enum
import logging

import ecs_logging

logger = logging.getLogger("elastic_stacker")


def configure_logger(level: int = logging.WARN, ecs: bool = False):
    if ecs:
        formatter = ecs_logging.StdlibFormatter()
    else:
        formatter = logging.Formatter(logging.BASIC_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
