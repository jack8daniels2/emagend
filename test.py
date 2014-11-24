from datetime import date
import os
import random
import log_gen

top_dir = '/tmp/emagend/'

try:
    os.mkdir(top_dir)
except Exception as e:
    print e
today = date.today()
for day in range(1, today.day):
    dt = today.replace(day=day)
    sub_dir = os.path.join(top_dir, dt.strftime('%Y-%m-%d'))
    try:
        os.mkdir(sub_dir)
    except Exception as e:
        print e
        continue
    for i in range(0, random.randint(1,10)):
        log_file = 'access_%d.log'%i
        with open(os.path.join(sub_dir,log_file),'w') as fp:
            map(fp.write, log_gen.main(random.randint(100,200)))
