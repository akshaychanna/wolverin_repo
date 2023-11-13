from logging import DEBUG
from logging import Formatter
from logging import getLogger
from logging import INFO
from logging import StreamHandler

ENV = "DEV"


class AsyncScraperLogger:
    def __init__(self, name: str):
        self.logger = getLogger(name)
        if ENV == "DEV":
            self.logger.setLevel(DEBUG)
        else:
            self.logger.setLevel(INFO)
        handler = StreamHandler()
        formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def error(self, message):
        self.logger.error(message)
