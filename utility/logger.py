import logging
import os
import sys

import httpx
from pythonjsonlogger import jsonlogger


class LokiHandler(logging.Handler):
    def __init__(self, url, tags=None):
        super().__init__()
        self.url = url
        self.tags = tags or {}

    def emit(self, record):
        try:
            log_entry = self.format(record)
            self.send_log(log_entry, record)
        except Exception as e:
            print(f"An error occurred in emit: {e}")

    def send_log(self, log_entry, record):
        headers = {'Content-Type': 'application/json'}
        combined_tags = {**self.tags, **getattr(record, "tags", {})}
        payload = {
            "streams": [
                {
                    "stream": combined_tags,
                    "values": [[str(int(record.created * 1e9)), log_entry]]
                }
            ]
        }
        try:
            logging.getLogger("httpx").disabled = True
            response = httpx.post(self.url, headers=headers, json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred: {e.response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            logging.getLogger("httpx").disabled = False


def setup_logging(loki_url, app_name):
    formatter = jsonlogger.JsonFormatter('')

    loki_handler = LokiHandler(
        url=loki_url,
        tags={"application": app_name}
    )
    loki_handler.setLevel(logging.INFO)
    loki_handler.setFormatter(formatter)

    logger_ = logging.getLogger()
    logger_.setLevel(logging.INFO)
    logger_.handlers = []  # Clear existing handlers
    logger_.addHandler(loki_handler)
    return logger_


loki_url = os.getenv("LOKI_URL")
app_name = "cpe_scrapper"
logger = setup_logging(loki_url, app_name)


def log_error(e: Exception, extra_context: dict = None):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    line_number = exc_tb.tb_lineno

    log_data = {
        "file_name": file_name,
        "line": line_number,
        "error_message": str(e),
    }

    if extra_context:
        log_data.update(extra_context)

    logger.error(
        f"An error occurred: ",
        extra={"tags": log_data}
    )

