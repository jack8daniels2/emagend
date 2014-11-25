"""
An abc for database that maintains a <key, list<date>> and an implementation using
Riak implementing the list using an bitarray
"""
import riak
import ConfigParser
import utils.logging_helper
from bitarray import bitarray
from datetime import datetime, date
from abc import ABCMeta, abstractmethod
import logging

logger = utils.logging_helper.init_logger(__name__, logging.ERROR)

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
    def __init__(self, config, bucket = None):
        if not bucket:
            bucket = config.get('db','bucket')
        epoch = config.get('ingest','epoch')
        date_format = config.get('ingest','date_format')
        self.epoch = datetime.strptime(epoch, date_format).date()
        pb_port = config.get('db','pb_port')
        proto = config.get('db','proto')
        self._client = riak.RiakClient(pb_port=pb_port, protocol=proto)
        self._bucket = self._client.bucket(bucket)

    def put(self, ip, access_date):
        if access_date < self.epoch:
            raise Exception('access_date {} before epoch {}'.format(access_date, self.epoch))
        index = (access_date - self.epoch).days + 1
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
        logger.info('key {} value {} at index {}'.format(ip, access_list, index-1))
        obj.data = access_list.unpack(one=b'\x01')
        obj.store()

    def get(self, ip, date_range = None):
        obj = self._bucket.get(ip)
        if not obj.data:
            logger.debug('Key {} not found'.format(ip))
            return
        access_list = bitarray()
        access_list.pack(bytes(obj.data))
        if not date_range:
            return access_list
        if len(date_range) != 2 or date_range[1] < date_range[0]:
            raise Exception('Invalid date range {}'.format(date_range))
        start_index = (date_range[0] - self.epoch).days
        end_index = (date_range[1] - self.epoch).days + 1
        logger.info('Accessing {} {}'.format(start_index, end_index))
        return access_list[start_index:end_index]

    def isset(self, ip, date_range = None):
        access = self.get(ip, date_range)
        return (bool(access) and any(access))

    def delete_bucket(self):
        for keys in self._bucket.stream_keys():
            map(self._bucket.delete, keys)

if __name__ == '__main__':
    CONFIG_FILE = 'config.cfg'
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    db = riak_db(config, 'test')
    db.put('j', date.today().replace(day=1))
    print db.get('j')
    db.put('k', date.today().replace(day=1))
    print db.get('k')
    db.put('k', date.today().replace(day=19))
    print db.get('k')
    print db.isset('k', (date.today().replace(day=19), date.today().replace(day=19)))
    print db.isset('j', (date.today().replace(day=19), date.today().replace(day=19)))
    db.delete_bucket()
