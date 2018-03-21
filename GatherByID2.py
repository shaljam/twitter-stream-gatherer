import time
import ujson
from twitter import *
import asyncio
import gzip
import json
import glob
import shutil
from os import remove
from pathlib import Path
import itertools

base_path = './data/t.'
processed_count_path = 'processed_count'
file_number = 1
count = 0
processed_count = 0

config_path = 'config.json'
config = None


def load_config():
    global config

    with open(config_path) as config_file:
        config = json.load(config_file)

    print('config loaded from {}'.format(config_path))


load_config()

ids_path = config['ids_path']

files = glob.glob('{}*.gz'.format(base_path))

if len(files) > 0:
    last = max(files)
    file_number = int(last[last.find('t.') + 2:last.rfind('.')]) + 1

    path = '{}{}'.format(base_path, str(file_number).zfill(10))

    if Path(path).is_file():
        count = sum(1 for line in open(path, 'rt', encoding='utf-8'))
    else:
        count = 0
else:
    file_number = 1
    count = 0


with open(processed_count_path) as f:
    try:
        processed_count = int(f.read())
    except:
        pass

processed_count = 90 * 1000 * 1000

auth = OAuth(
    consumer_key=config['consumer_key'],
    consumer_secret=config['consumer_secret'],
    token=config['access_token_key'],
    token_secret=config['access_token_secret']
)

t = Twitter(
    auth=auth, retry=True)


while True:
    ids = ''
    batch_count = 0
    with open(ids_path) as f:
        for line in itertools.islice(f, processed_count, processed_count + 10):
            # status = api.get_status(line[:-1])
            ids += line[:-1] + ','
            batch_count += 1

    if len(ids) == 0:
        break

    ids = ids[:-1]

    print('from {}, count: {}'.format(processed_count, batch_count))

    results = t.statuses.lookup(_id="12345", _map=True)
    print('results {}'.format(len(results)))
    for item in results:
        with open('{}{}'.format(base_path, str(file_number).zfill(10)), 'at', encoding='utf-8') as f:
            f.write(ujson.dumps(item._json) + '\n')
            f.close()

        count += 1

        if count == 10000:
            count = 0

            file_path = '{}{}'.format(base_path, str(file_number).zfill(10))

            with open(file_path, 'rb') as f_in:
                with gzip.open('{}.gz'.format(file_path), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

                f_in.close()

            remove(file_path)

            file_number += 1

    processed_count += batch_count
    with open(processed_count_path, mode='wt') as f:
        f.write(str(processed_count))

    print('batch completed!')
