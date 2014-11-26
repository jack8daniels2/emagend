import logging

def init_logger(name, level = logging.ERROR):
    logger = logging.getLogger(name)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(threadName)s] {%(funcName)s@%(lineno)s}: %(levelname)s %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(level)
    return logger
