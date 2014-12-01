"""
The application crawls config['ingest']['log_dir'] looking for directories that
can be parsed into date using the format config['ingest']['date_format'],
containing access log files that match regex config['ingest']['access_log_pattern'].
The complete path looks like - log_dir/date/[access_log,]
For each of access_log, ingest the IP contained in it for date given by its parent directory

This must be run periodically, or must use a library like watchdog to continously look for new files.
A better approach would be to push these logs as they come, into a message broker
and consume them on the other end.

"""

import ConfigParser
from utils import logging_helper
import parser
import db
import threading
import Queue
import itertools
from datetime import datetime
import logging
import math
import pybloomfilter

CONFIG_FILE = 'config.cfg'
logger = logging_helper.init_logger(__name__, logging.INFO)

class Ingester(object):
    def __init__(self, config, iterable, num_threads = 4,
                 chunk_size = 50, queue_size = 4):
        self.config = config
        self.num_threads = num_threads
        self.chunk_size = chunk_size
        self.tasks = Queue.Queue(queue_size*num_threads)
        self.refill_tasks = threading.Condition()
        self.iterable = iterable
        #TODO: BloomFilter must be attached to a date_range, say one for each month
        self.bf = pybloomfilter.BloomFilter(math.pow(2,29), 0.01,
                                            config.get('ingest','bloom_filter_location'))
        self.bf_lock = threading.Lock()
        #http://bugs.python.org/issue7980 need to call strptime before the threads call it!
        epoch = config.get('ingest','epoch')
        date_format = config.get('ingest','date_format')
        epoch = datetime.strptime(epoch, date_format).date()

    def _worker(self):
        db_hndl = db.riak_db(self.config)
        while True:
            task = self.tasks.get()
            #1. Insert into the db
            for data in task:
                db_hndl.put(*data)
            #2. Insert into the bloom filter
            self.bf_lock.acquire()
            self.bf.update((data[0] for data in task))
            self.bf_lock.release()
            #3. Mark done
            self.tasks.task_done()
            #4. Check if task queue is running out, if so notify main thread
            if self.tasks.qsize() < self.tasks.maxsize / 2:
                logger.debug('Queue getting empty')
                self.refill_tasks.acquire()
                self.refill_tasks.notify()
                self.refill_tasks.release()
                logger.debug('Notified main thread that tasks queue is running out of tasks')
        logging.debug('Exiting')

    def start(self):
        '''
        Ingest all the IPs produced from crawling config.log_dir.
        The key logic in this function is to
            a) use threads each with a separate connection to riak, handling a chunk of log data
            b) use a fixed size queue to limit the amount of logs read and kept in memory.
                Any time the queue size falls below the half full (or half empty), we
                read the disk and refill it.
        '''
        self.threads = [threading.Thread(target = self._worker) \
                            for t in xrange(self.num_threads)]
        for t in self.threads:
            t.daemon = True
            t.start()

        data_stream = iter(self.iterable)
        task = set(itertools.islice(data_stream, self.chunk_size))
        total_num_records = 0
        while task:
            try:
                self.tasks.put_nowait(task)
                total_num_records += len(task)
                task = set(itertools.islice(data_stream, self.chunk_size))
            except Queue.Full:
                logger.debug('Task queue full')
                self.refill_tasks.acquire()
                while self.tasks.qsize() >= self.tasks.maxsize / 2:
                    logger.debug('Waiting for tasks queue to be empty')
                    self.refill_tasks.wait()
                self.refill_tasks.release()
        self.tasks.join()
        self.bf.sync()
        logger.info('Processed {} records'.format(total_num_records))

if __name__ == '__main__':
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    Ingester(config, parser.parse_directories(config)).start()
