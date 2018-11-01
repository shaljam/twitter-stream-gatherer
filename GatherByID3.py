import datetime
import glob
import gzip
import itertools
import json
import shutil
import signal
import time
import traceback
import ujson
from os import remove
from pathlib import Path
from threading import Thread

import tweepy

from utils import beautiful_now

base_path = './data/i.'
processed_count_path = 'processed_count'
file_number = 1
count = 0
processed_count = 0

config_path = 'config.json'
config = None


def load_config():
    global config

    with open(config_path, 'rb') as config_file:
        config = json.load(config_file)

    print('config loaded from {}'.format(config_path))


load_config()

ids_path = config['ids_path']

# !!! base_path already contains t.
files = glob.glob('{}*'.format(base_path))

if len(files) > 0:
    last = max(files)
    add_to_file_number = 0
    if last.endswith('.gz'):
        last = last[:-3]
        add_to_file_number = 1

    file_number = int(last[last.find('i.') + 2:]) + add_to_file_number

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
        print(f'failed to read processed_count file {processed_count_path}')
        exit(1)

# processed_count = 70 * 1000 * 1000

auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])

if not config['user_login']:
    auth.set_access_token(config['access_token_key'], config['access_token_secret'])
else:
    print(f'trying user login, please wait for to input verifier...')

    try:
        redirect_url = auth.get_authorization_url()
    except tweepy.TweepError:
        print('Error! Failed to get request token.')
        exit(1)

    verifier = input(f'open the url\n{redirect_url}\nand enter the verifier:\n')

    try:
        auth.get_access_token(verifier)
    except tweepy.TweepError:
        print('Error! Failed to get access token.')
        exit(1)

print('got access token, starting ...')

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)

running = True
received_batch_count = 0
last_received_batch_count = 0
rate_calc_time = time.time()
start_time = time.time()


def timer():
    global received_batch_count, last_received_batch_count, rate_calc_time, start_time, running

    while running:
        now = time.time()
        current_received = received_batch_count
        rate = (current_received - last_received_batch_count) / (now - rate_calc_time) * 15 * 60
        overall_rate = received_batch_count / (now - start_time) * 15 * 60

        running_time = datetime.timedelta(seconds=int(now - start_time))

        print(f'{beautiful_now()}: '
              f'{rate:.0f} batch/15m\t'
              f'overall {overall_rate:.0f} batch/15m\t'
              f'running for {running_time}\t'
              f'total {current_received}'
              )

        rate_calc_time = now
        last_received_batch_count = current_received

        if not running:
            return

        time.sleep(60)


def start_timer():
    t = Thread(target=timer)
    t.setDaemon(True)
    t.start()


start_timer()


def save_results_to_file(results, bc):
    global file_number, count, processed_count

    with open('{}{}'.format(base_path, str(file_number).zfill(10)), 'at', encoding='utf-8') as res_file:
        for item in results:
            res_file.write(ujson.dumps(item._json) + '\n')
            count += 1

    if count >= 10000:

        file_path = '{}{}'.format(base_path, str(file_number).zfill(10))

        print(f'compressing file {file_path} with count {count} ...')

        count = 0

        with open(file_path, 'rb') as f_in:
            with gzip.open('{}.gz'.format(file_path), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            f_in.close()

        remove(file_path)

        file_number += 1

    processed_count += batch_count
    with open(processed_count_path, mode='wt') as f:
        f.write(str(processed_count))


def exit_gracefully(signum, frame):
    global running

    print('exiting gracefully ...')
    running = False


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)


with open(ids_path) as f:
    ids = []
    batch_count = 0
    bst = time.time()

    # skip processed count and continue to the end
    pc = processed_count
    for line in itertools.islice(f, pc, None):
        if not running:
            break

        ids.append(line[:-1])
        batch_count += 1

        if batch_count < 100:
            continue

        time_to_wait = 1.0001 - (time.time() - bst)

        if time_to_wait > 0:
            # print(f'sleeping {time_to_wait}')
            time.sleep(time_to_wait)

        error_count = 0
        while error_count < 5:
            bst = time.time()

            try:
                results = api.statuses_lookup(id_=ids, include_entities=True, map_=False)
                save_results_to_file(results, batch_count)
                break
            except tweepy.TweepError:
                error_count += 1
                print(f'TweepError happend\n{traceback.format_exc()}')
                continue
        else:
            print(f'many errors happened, will exit')
            exit(1)

        ids.clear()
        batch_count = 0

        received_batch_count += 1

    if running and len(ids):
        results = api.statuses_lookup(id_=ids, include_entities=True, map_=False)
        save_results_to_file(results, batch_count)

        ids.clear()
        batch_count = 0

    running = False
