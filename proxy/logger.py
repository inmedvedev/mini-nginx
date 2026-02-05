import logging
import logging.config
import logging.handlers
import queue
from typing import Optional


def setup_logging(config_path: str = "logging.conf") -> Optional[logging.handlers.QueueListener]:
    logging.config.fileConfig(config_path, disable_existing_loggers=False)

    log_queue: queue.Queue = queue.Queue(-1)
    root = logging.getLogger()
    handlers = root.handlers[:]

    if not handlers:
        return None

    listener = logging.handlers.QueueListener(
        log_queue,
        *handlers,
        respect_handler_level=True
    )

    root.handlers = [logging.handlers.QueueHandler(log_queue)]
    listener.start()
    return listener