import logging

def init_logger(name, level = logging.DEBUG):
    logger = logging.getLogger(name)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(funcName)s@%(lineno)s: %(levelname)s %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(level)
    return logger
