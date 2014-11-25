import ConfigParser
import db
from datetime import datetime

CONFIG_FILE = 'config.cfg'

class AccessQuery(object):
    def __init__(self, config):
        self.db = db.riak_db(config)
        self.date_format = config.get('ingest','date_format')
        #TODO read access bloom filter off disk
    def query(self, ip, start_date = None, end_date = None):
        ''' Check if the ip has accessed the website in the given date_range '''
        date_range = None
        if start_date and end_date:
            date_range = []
            for dt in (start_date, end_date):
                try:
                    date_range.append(datetime.strptime(dt, self.date_format).date())
                except ValueError:
                    raise Exception('Unable to parse {} as a date'.format(dt))
        print 'Searching for {} in date_range {}'.format(ip, date_range)
        return self.db.isset(ip, date_range)

if __name__ == '__main__':
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    query_hndl = AccessQuery(config)
    while True:
        try:
            user_input = raw_input('Enter IP start_date end_date: ').split()
        except KeyboardInterrupt:
            break
        if not len(user_input):
            break
        try:
            res = query_hndl.query(*user_input[:3])
        except Exception as e:
            print 'Error:',e
            continue
        print res
