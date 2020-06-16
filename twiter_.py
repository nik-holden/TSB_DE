import tweepy
import pandas as pd


def tweety_module():
    auth = tweepy.OAuthHandler('N1VGVEsuxkRxjecJkCZESfyNT', 'bUs1mT0TeiuXSmqh0SjPKzpM09vEDl9x4uAWm5xwFtUN6vdiAv',)
    auth.set_access_token('2377854260-eQg0hqbz5S4Los8irTwchKFPl8SNTtOylM5sIl0', 'uRharA83P8BYD3bYt4MFxu58wSwXBTPtcruK634bVJs3K')

    api = tweepy.API(auth)

    public_tweets = api.user_timeline(screen_name='NPDCouncil', count=1, tweet_mode="extended")
    for tweet in public_tweets:
        print(tweet.full_text)

def tweety_stream_module():
    auth = tweepy.OAuthHandler('N1VGVEsuxkRxjecJkCZESfyNT', 'bUs1mT0TeiuXSmqh0SjPKzpM09vEDl9x4uAWm5xwFtUN6vdiAv',)
    auth.set_access_token('2377854260-eQg0hqbz5S4Los8irTwchKFPl8SNTtOylM5sIl0', 'uRharA83P8BYD3bYt4MFxu58wSwXBTPtcruK634bVJs3K')

    api = tweepy.API(auth)

    class StreamListener(tweepy.StreamListener):
        def on_status(self, status):
            print(status.text)
        def on_error(self, status_code):
            if status_code == 420:
                return False

    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(follow=['428333','55507370']) #track=["trump", "clinton", "hillary clinton", "donald trump"])


#twitter_module()
tweety_stream_module()
