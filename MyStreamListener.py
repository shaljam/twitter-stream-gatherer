import tweepy
import motor.motor_asyncio
import asyncio


#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://twitter:kjashzi1SKzajzXI!33@localhost:52912/twitter', io_loop=self.loop)
        self.db = self.client.twitter

    def on_status(self, status):
        print(status.id)
        self.save(status._json)

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

    def save(self, tweet):
        self.loop.run_until_complete(self.do_insert(tweet))

    async def do_insert(self, document):
        result = await self.db.stream.insert_one(document)
        # print('result %s' % repr(result.inserted_id))