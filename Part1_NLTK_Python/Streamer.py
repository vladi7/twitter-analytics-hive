#From Tweepy
import tweepy
access_token = "2822717651-AuHq6xAFkU56Cy2qKhtNTtkm1oERL2DTkLywlYM"
access_token_secret = "6Lbx5l0MLY3WpP75xDiv31KOAOtum3Wnxgr7tGdahef7E"
consumer_key = "SBwjKORZPuflTIyfXYkxlug9e"
consumer_secret = "d5hMfDWOPrzwtfoQUEUIJXun19Ie5rkK0s2egSoy2c5CFPfrw8"


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