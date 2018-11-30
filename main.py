from configparser import ConfigParser
from log_manager import sys_logger
config = ConfigParser()
config.read('logger.conf')
log_manager = sys_logger(config)