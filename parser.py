from datetime import datetime
import ConfigParser
import os
import re
import utils.logging_helper
import logging

logger = utils.logging_helper.init_logger(__name__, logging.ERROR)

def parse_directories(config):
    ''' Look for all files matching log_dir/<date>/access_file_regex
        parse IPv4 from each file
        yield date, ip'''

    log_dir = config.get('ingest','log_dir')
    access_file_regex = config.get('ingest','access_file_regex')
    date_format = config.get('ingest','date_format')
    access_file_pattern = re.compile(access_file_regex)
    for root, dirs, files in os.walk(log_dir):
        sub_dir = root.rsplit('/',1)[-1]
        if not sub_dir:
            continue
        try:
            dt = datetime.strptime(sub_dir, date_format).date()
        except ValueError:
            logger.error('unable to parse {}'.format(sub_dir))
            continue
        for access_file in files:
            if access_file_pattern.match(access_file):
                filepath = os.path.join(root, access_file)
                for ip in parse_file(filepath):
                    yield ip, dt
        logger.info('Ingested directory {}'.format(sub_dir))

def parse_file(log_file):
    ''' Parse a file containing an IP per line, yields one at a time'''

    ipv4_regex = '(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3,3}$'
    pattern = re.compile(ipv4_regex)

    with open(log_file, 'r') as fp:
        for line in fp:
            if pattern.match(line):
                yield line.rstrip('\n')
    logger.info('Ingested file {}'.format(log_file))

if __name__ == '__main__':
    CONFIG_FILE = 'config.cfg'
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    for ip in parse_directories(config):
        print ip
