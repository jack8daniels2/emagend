"""
The application crawl config['ingest']['log_dir'] looking for directories that
can be parsed into date using the format config['ingest']['date_format'],
containing access log files that match regex config['ingest']['access_log_pattern'].
The complete path looks like - log_dir/date/[access_log,]
For each of access_log, ingest the IP contained in it for date given by its parent directory

This must be run periodically, or must use a library like watchdog to continously look for new files.
A better approach would be to push these logs into a message broker and consume them on the other end.

"""

import ConfigParser
from utils import logging_helper
import parser
import db

CONFIG_FILE = 'config.cfg'
logger = logging_helper.init_logger(__name__)

class Ingester(object):
    def __init__(self, config):
        self.db = db.riak_db(config)
        self.config = config

    def start(self):
        ''' Ingest all the IPs produced from crawling config.log_dir'''
        #TODO: Use threading to parallelize this. Plenty of I/O blocking here
        for dt, ip in parser.parse_directories(config):
            self.db.put(ip, dt)

if __name__ == '__main__':
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    Ingester(config).start()
