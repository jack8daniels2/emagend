import ConfigParser
import logging

CONFIG_FILE = 'config.txt'
config = ConfigParser.RawConfigParser()
config.read(CONFIG_FILE)
config.get('ingest','epoch')

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(funcName)s@%(lineno)s: %(levelname)s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
