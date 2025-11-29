import logging
import os
import sys
from pythonjsonlogger import json
from src.utils.config import CONFIG
from logging.handlers import RotatingFileHandler

class CleanNoneJsonFormatter(json.JsonFormatter):
    def process_log_record(self, log_record):
        return {k: v for k, v in log_record.items() if v is not None}

def setup_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    file_formatter = CleanNoneJsonFormatter(
            fmt='%(asctime)s %(levelname)s %(name)s %(message)s %(migration_id)s %(entity)s %(source_id)s %(target_id)s %(error_source)s %(context)s',
            rename_fields={
                'asctime': 'timestamp',
                'levelname': 'level',
                'message': 'msg'
            }
        )
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f'{CONFIG["EXEC_ENVIRONMENT"]}.log')

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes = 10 * 1024 * 1024,
        backupCount = 20
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    std_types = ['out', 'err']

    for type in std_types:
        sys_type = sys.stderr if type == 'err' else sys.stdout
        logger_handler = logging.StreamHandler(sys_type)
        logger_handler.setLevel(logging.WARNING if type == 'err' else logging.DEBUG)
        if type == 'out':
            logger_handler.addFilter(lambda record: record.levelno < logging.WARNING)
        
        logger_handler.setFormatter(file_formatter)
        logger.addHandler(logger_handler)

    return logger
