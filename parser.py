from datetime import datetime
import ConfigParser
import os
import re
import utils.logging_helper

CONFIG_FILE = 'config.txt'
logger = utils.logging_helper.init_logger(__name__)

def parse_directories(top_dir, access_file_regex):
    ''' Look for all files matching top_dir/<date>/access_file_regex
        parse IPv4 from each file
        yield date, ip'''

    access_file_pattern = re.compile(access_file_regex)
    for root, dirs, files in os.walk(top_dir):
        if not root:
            continue
        sub_dir = root.rsplit('/',1)[-1]
        try:
            dt = datetime.strptime(sub_dir, '%Y-%m-%d').date()
        except ValueError:
            logger.error('unable to parse {}'.format(sub_dir))
            continue
        for access_file in files:
            if access_file_pattern.match(access_file):
                for ip in parse_file(os.path.join(root, access_file)):
                    yield dt, ip

def parse_file(log_file):
    ''' Parse a file containing an IP per line, yields one at a time'''

    ipv4_regex = '(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3,3}$'
    pattern = re.compile(ipv4_regex)

    with open(log_file, 'r') as fp:
        for line in fp:
            if pattern.match(line):
                yield line.rstrip('\n')

if __name__ == '__main__':
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    log_dir = config.get('ingest','log_dir')
    access_file_regex = config.get('ingest','access_file_regex')
    for ip in parse_directories(log_dir, access_file_regex):
        print ip
