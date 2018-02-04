import tweepy
import json
import urllib3
import traceback
import time
import asyncio
import uvloop
import signal
import functools
import MyStreamListener
from utils import beautiful_now


print('{}\thi there!'.format(beautiful_now()))

config_path = 'config.json'
config = None
stopped = False
my_stream_listener = None


def load_config():
    global config

    with open(config_path) as config_file:
        config = json.load(config_file)

    print('config loaded from {}'.format(config_path))


def gather():
    global my_stream_listener
    while True:
        try:
            load_config()

            auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
            auth.set_access_token(config['access_token_key'], config['access_token_secret'])

            api = tweepy.API(auth, timeout=10, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                             compression=True)

            my_stream_listener = MyStreamListener.MyStreamListener()
            my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)

            if config['filter_or_sample'] == 'filter':
                track_str = config['track']
                print(len(track_str.split()))
                track = list({item for item in track_str.split()})
                print(len(track))
                print(track)
                my_stream.filter(track=track, languages=['fa'])
            else:
                my_stream.sample(languages=['fa'])

            break
        except urllib3.exceptions.ReadTimeoutError:
            print('ReadTimeoutError occurred on {}. Will print stacktrace...'.format(beautiful_now()))
            print(traceback.format_exc())

            time.sleep(10)


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()


async def gather_async():
    await loop.run_in_executor(None, gather)

gather_task = loop.create_task(gather_async())


def ask_exit(sig_name):
    print("got signal %s: exit" % sig_name)
    if my_stream_listener is not None:
        my_stream_listener.running = False


for sig in (signal.SIGINT, signal.SIGTERM):
    loop.add_signal_handler(sig, functools.partial(ask_exit, sig))

try:
    loop.run_until_complete(gather_task)
    print('task completed!')
finally:
    loop.close()

print('{}\tsee you soon.'.format(beautiful_now()))
