import tweepy
import time
import ujson

file_number = 1
base_path = './data/t.'

auth = tweepy.OAuthHandler('kIpQuw1LxS4DgatGEB0NwZfAm', 'a8r7hgRMNdqb2xxmhcWanzYg1FN6DgTpS4iANiA0lyos47fdQZ')
auth.set_access_token('2380084197-h1uU4twPIGIIrB3NqzQxW8yIhNswwKJwwsfsYzy', 'dKc5cZavlqBKqGmnA9kJM0WbWZfNuqFtwz8yPTyBvSgQX')

api = tweepy.API(auth)

query = 'maria OR hurricane'


def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)


for tweet in limit_handled(tweepy.Cursor(api.search, q=query, result_type="recent", lang="en").items(5000)):
    with open('{}{}'.format(base_path, str(file_number).zfill(10)), 'at', encoding='utf-8') as f:
        f.write(ujson.dumps(tweet._json) + '\n')
        f.close()
