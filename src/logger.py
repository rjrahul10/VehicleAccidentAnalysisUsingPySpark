import logging
from datetime import datetime


class Logger:

    def __init__(self, log_file):
        self.logger = logging.getLogger('Vehicle_Accident_logger')
        self.logger.setLevel(logging.INFO)  # defaulting logger mode to INFO
        today_dt = datetime.now().strftime("%Y%m%d")
        log_file = log_file + str(today_dt) + ".log"
        file_handler = logging.FileHandler(log_file)
        self.logger.addHandler(file_handler)

    def getLogger(self):
        return self.logger

    def info(self, msg):
        self.logger.info(msg)

    def warn(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)
