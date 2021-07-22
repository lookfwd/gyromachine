import sys
import gzip
from random import random
import boto3
import re


MAX_RUN = 255


s3_access = '*'
s3_secret = '*'


def upload(target_file, object_name):
    client = boto3.client('s3',
                          aws_access_key_id=s3_access,
                          aws_secret_access_key=s3_secret)
    client.upload_file(target_file, 'enginestream', object_name)


taget_batch = int(sys.argv[2])

target_file = f'batch_{taget_batch:02d}_{sys.argv[3]}.log.gz'
object_file = f'batch_{taget_batch:02d}.log.gz'
line_no = 0

with open(sys.argv[1], 'rt') as f:
    with gzip.open(target_file, 'wt') as fo:
        for line in f.readlines():
            try: 
                time, cycle, conf, run, x, y, z = re.split('[:,]', line.strip())
                cycle, conf, run = map(int, (cycle, conf, run))
                time, x, y, z = map(float, (time, x, y, z))

                msg = "%f:%d:%d:%d:%f,%f,%f\n" % (time, cycle, conf,
                                                  run + (taget_batch * MAX_RUN),
                                                  x,y,z)
                                                  #x + 0.1 * random(),
                                                  #y + 0.1 * random(),
                                                  #z + 0.01 * random())
                fo.write(msg)
            except:
                pass

upload(target_file, f'configs/{sys.argv[3]}/{object_file}')

