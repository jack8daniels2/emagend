import riak
import ConfigParser
import logging
from bitarray import bitarray
from datetime import datetime, date
from abc import ABCMeta, abstractmethod

CONFIG_FILE = 'config.txt'

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(funcName)s@%(lineno)s: %(levelname)s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class kl_db(object):
    __metaclass__ = ABCMeta
    ''' an Abstraction over the db, in case we want to swap it/change schema later'''
    @abstractmethod
    def put(self, ip, access_date):
        '''stores the IP and the access_date in the db'''
        pass
    @abstractmethod
    def get(self, ip, date_range=None):
        '''returns a list of access dates when the IP contacted us'''
        pass
    @abstractmethod
    def isset(self, ip, date_range=None):
        '''returns True if any item exists in the given date_range'''
        pass

class riak_db(kl_db):
    ''' Riak DB implementation of db_abstract'''
    def __init__(self, bucket = None):
        config = ConfigParser.RawConfigParser()
        config.read(CONFIG_FILE)
        if not bucket:
            bucket = 'ip_access'
        self.epoch = datetime.strptime(config.get('ingest','epoch'), '%Y-%m-%d').date()
        self.pb_port = config.get('db','pb_port')
        self.proto = config.get('db','proto')
        self._client = riak.RiakClient(pb_port=self.pb_port, protocol=self.proto)
        self._bucket = self._client.bucket(bucket)

    def put(self, ip, access_date):
        if access_date < self.epoch:
            raise Exception('access_date {} before epoch {}'.format(access_date, self.epoch))
        index = (access_date - self.epoch).days + 1
        print 'putting it at index {}'.format(index)
        # Riak requires a get before put if the object exists to maintain its vector_clock I suppose
        obj = self._bucket.get(ip)
        if not obj.data:
            access_list = bitarray(index)
            access_list.setall(False)
        else:
            access_list = bitarray()
            access_list.pack(bytes(obj.data))
            if len(access_list) < index:
                update = bitarray(index - len(access_list))
                update.setall(0)
                access_list.extend(update)
        access_list[-1] = 1
        print access_list.tolist()
        obj.data = access_list.unpack(one=b'\x01')
        obj.store()

    def get(self, ip, date_range = None):
        obj = self._bucket.get(ip)
        if obj.data:
            access_list = bitarray()
            access_list.pack(bytes(obj.data))
            if not date_range:
                return access_list
            if len(date_range) != 2 or date_range[1] < date_range[0]:
                raise Exception('Invalid date range {}'.format(date_range))
            start_index = (date_range[0] - self.epoch).days
            end_index = (date_range[1] - self.epoch).days + 1
            print 'reading {}-{}'.format(start_index, end_index)
            print access_list[start_index:end_index].tolist()
            return access_list[start_index:end_index]

    def isset(self, ip, date_range = None):
        return any(self.get(ip, date_range))

if __name__ == '__main__':
    db = kl_db('test')
    db.put('j', date.today().replace(month=1, day=1))
    print db.get('j')
    db.put('k', date.today().replace(month=1, day=1))
    print db.get('k')
    db.put('k', date.today().replace(month=1, day=5))
    print db.get('k')
    print db.isset('k', (date.today().replace(month=1, day=5), date.today().replace(month=1, day=6)))
