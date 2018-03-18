import tweepy
import asyncio
import gzip
import json
import glob
import shutil
import datetime
import time
from threading import Thread
from os import remove
from pathlib import Path
from utils import beautiful_now


base_path = './data/t.'


class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.running = True

        # !!! base_path already contains t.
        files = glob.glob('{}*'.format(base_path))

        if len(files) > 0:
            last = max(files)
            add_to_file_number = 0
            if last.endswith('.gz'):
                last = last[:-3]
                add_to_file_number = 1

            self.file_number = int(last[last.find('t.') + 2:]) + add_to_file_number

            path = '{}{}'.format(base_path, str(self.file_number).zfill(10))

            if Path(path).is_file():
                self.count = sum(1 for line in open(path, 'rt', encoding='utf-8'))
            else:
                self.count = 0
        else:
            self.file_number = 1
            self.count = 0

        self.received_count = 0
        self.last_received_count = 0
        self.rate_calc_time = time.time()
        self.start_time = time.time()
        self.start_timer()

    def on_status(self, status):
        self.save(json.dumps(status._json))

        return self.running

    def on_error(self, status_code):
        print('RECEIVED ERROR STATUS CODE: {}'.format(status_code))

        return self.running

    def on_limit(self, track):
        print('LIMIT REACHED WITH TRACK: {} on {}'
              .format(track, datetime.datetime.now().strftime('%A, %b %d, %Y %H:%M:%S')))

        return self.running

    def save(self, tweet):
        self.loop.run_until_complete(self.do_insert(tweet))

    async def do_insert(self, document):
        # result = await self.db.stream.insert_one(document)
        # print('result %s' % repr(result.inserted_id))

        with open('{}{}'.format(base_path, str(self.file_number).zfill(10)), 'at', encoding='utf-8') as f:
            f.write(document + '\n')
            f.close()

        self.count += 1

        if self.count == 10000:
            self.count = 0

            file_path = '{}{}'.format(base_path, str(self.file_number).zfill(10))

            with open(file_path, 'rb') as f_in:
                with gzip.open('{}.gz'.format(file_path), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

                f_in.close()

            remove(file_path)

            self.file_number += 1

        self.received_count += 1

    def timer(self):
        while self.running:
            now = time.time()
            current_received = self.received_count
            rate = (current_received - self.last_received_count) / (now - self.rate_calc_time)
            overall_rate = self.received_count / (now - self.start_time)

            running_time = datetime.timedelta(seconds=int(now - self.start_time))

            print('{}: {:.2f} t/s\t overall {:.2f} t/s\t running for {}\t total {}'
                  .format(beautiful_now(), rate, overall_rate, running_time, current_received))

            self.rate_calc_time = now
            self.last_received_count = current_received

            if not self.running:
                return

            time.sleep(60)

    def start_timer(self):
        t = Thread(target=self.timer)
        t.setDaemon(True)
        t.start()

