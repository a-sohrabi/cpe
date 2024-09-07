import logging
from typing import Optional

from logging_loki import LokiHandler

from .config import settings


class LogManager:
    def __init__(self,
                 logger_name: Optional[str] = None,
                 loki_url: str = settings.LOKI_URL,
                 application_name: str = "cpe_scrapper") -> None:
        self.loki_url = loki_url
        self.application_name = application_name
        self.logger = logging.getLogger(logger_name or __name__)

        loki_handler = LokiHandler(
            url=self.loki_url,
            tags={"application": self.application_name},
            version="1"
        )

        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(loki_handler)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def critical(self, message: str) -> None:
        self.logger.critical(message)
