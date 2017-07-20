import tweepy
import MyStreamListener
import json


config_path = 'config.json'
config = None


def load_config():
    global config

    with open(config_path) as config_file:
        config = json.load(config_file)

    print('config loaded from {}'.format(config_path))


load_config()

auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
auth.set_access_token(config['access_token_key'], config['access_token_secret'])

api = tweepy.API(auth)

myStreamListener = MyStreamListener.MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

if config['filter_or_sample'] == 'filter':
    track_str = config['track']
    print(len(track_str.split()))
    track = list({item for item in track_str.split()})
    print(len(track))
    print(track)
    myStream.filter(track=track, languages=['fa'], async=True)
else:
    myStream.sample(languages=['fa'], async=True)
