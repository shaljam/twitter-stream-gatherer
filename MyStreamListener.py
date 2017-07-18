import tweepy
# import motor.motor_asyncio
import asyncio
import gzip
import json
import glob
import shutil
from os import remove
from pathlib import Path

base_path = './data/t.'


# override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://twitter:kjashzi1SKzajzXI!33@localhost:52912/twitter', io_loop=self.loop)
        # self.db = self.client.twitter

        files = glob.glob('{}*.gz'.format(base_path))

        if len(files) > 0:
            last = max(files)
            self.file_number = int(last[last.find('t.') + 2:last.rfind('.')]) + 1

            path = '{}{}'.format(base_path, str(self.file_number).zfill(10))

            if Path(path).is_file():
                self.count = sum(1 for line in open(path, 'rt', encoding='utf-8'))
            else:
                self.count = 0
        else:
            self.file_number = 1
            self.count = 0

    def on_status(self, status):
        print(status.id)
        self.save(json.dumps(status._json))

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            print('RECEIVED ERROR STATUS CODE: {}'.format(status_code))
            return False

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
