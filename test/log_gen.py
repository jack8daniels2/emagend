"""
Generates log files for a configurable number of days,
each with rand(configurable_number_of_files) containing
rand(configurable_min, configurable_max) IPs made of rand(configurable_octet_value) to
allow duplicate IPs easily.
"""
from datetime import date
import os
import random
from dateutil.relativedelta import relativedelta
import ConfigParser


CONFIG_FILE = '../config.cfg'
config = ConfigParser.RawConfigParser()
config.read(CONFIG_FILE)
log_dir = config.get('ingest', 'log_dir')
num_days = config.getint('test', 'num_days')
max_files_per_day = config.getint('test', 'max_files_per_day')
records_per_file = (config.getint('test', 'min_records_per_file'),
                    config.getint('test', 'max_records_per_file'))
date_format = config.get('ingest', 'date_format')
max_octet_value = config.getint('test', 'max_octet_value')

def gen_rand_ips(count):
    for x in xrange(count):
        first_number = random.randint(0, max_octet_value)
        second_number = random.randint(0, max_octet_value)
        third_number = random.randint(0, max_octet_value)
        fourth_number = random.randint(0, max_octet_value)
        yield "%d.%d.%d.%d\n" % (first_number, second_number, third_number, fourth_number)

try:
    os.mkdir(log_dir)
except Exception as e:
    print e
today = date.today()
for i in xrange(num_days):
    dt = today - relativedelta(days=i)
    sub_dir = os.path.join(log_dir, dt.strftime(date_format))
    try:
        os.mkdir(sub_dir)
    except Exception as e:
        print e
        continue
    for i in range(0, random.randint(1, max_files_per_day)):
        log_file = 'access_%d.log'%i
        with open(os.path.join(sub_dir,log_file),'w') as fp:
            map(fp.write, gen_rand_ips(random.randint(*records_per_file)))
