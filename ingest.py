"""
The application crawls config['ingest']['log_dir'] looking for directories that
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
import threading
import Queue
import itertools
from datetime import datetime
import logging

CONFIG_FILE = 'config.cfg'
logger = logging_helper.init_logger(__name__, logging.ERROR)

class Ingester(object):
    def __init__(self, config, iterable, num_threads = 4, chunk_size = 50):
        self.config = config
        self.num_threads = num_threads
        self.chunk_size = chunk_size
        self.tasks = Queue.Queue(2*num_threads)
        self.refill_tasks = threading.Event()
        self.iterable = iterable
        #http://bugs.python.org/issue7980 need to call strptime before the threads call it!
        epoch = config.get('ingest','epoch')
        date_format = config.get('ingest','date_format')
        epoch = datetime.strptime(epoch, date_format).date()

    def _worker(self):
        db_hndl = db.riak_db(self.config)
        while True:
            task = self.tasks.get()
            for data in task:
                db_hndl.put(*data)
            self.tasks.task_done()
            if self.tasks.qsize() < self.tasks.maxsize /  2:
                self.refill_tasks.set()

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
                self.refill_tasks.clear()
                self.refill_tasks.wait()
        self.tasks.join()
        logger.info('Processed {} records'.format(total_num_records))

if __name__ == '__main__':
    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_FILE)
    Ingester(config, parser.parse_directories(config)).start()
