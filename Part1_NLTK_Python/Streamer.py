#From Tweepy
import tweepy
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


class MyListener(tweepy.streaming.StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True) #wait on rate limit-against 402
    stream = tweepy.Stream(auth = api.auth, listener = MyListener())

    stream.filter(track=['mercedes', 'bmw', 'audi'], async=True)
