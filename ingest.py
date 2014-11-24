import ConfigParser
import utils.logging_helper

CONFIG_FILE = 'config.txt'
config = ConfigParser.RawConfigParser()
config.read(CONFIG_FILE)
config.get('ingest','epoch')

logger = utils.logging_helper.init_logger(__name__)
