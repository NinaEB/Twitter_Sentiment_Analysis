import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
import credentials
import settings

# Pull API credentials
consumer_key = credentials.API_KEY
consumer_secret = credentials.API_SECRET_KEY
access_token = credentials.ACCESS_TOKEN
access_secret = credentials.ACCESS_TOKEN_SECRET

# Define the Twitter tag to search for
TAG = 'cryptocurrencies'


# Extend the StreamListener class to use it for Twitter
class TweetsListener(StreamListener):
    # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads( data )
            print("new message")
            # if tweet is longer than 140 characters
            if "extended_tweet" in msg:
                # add at the end of each tweet "t_end"
                self.client_socket\
                    .send(str(msg['extended_tweet']['full_text']+"t_end")\
                    .encode('utf-8'))
                print(msg['extended_tweet']['full_text'])
            else:
                # add at the end of each tweet "t_end"
                self.client_socket\
                    .send(str(msg['text']+"t_end")\
                    .encode('utf-8'))
                print(msg['text'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(c_socket, keyword):
    print('start sending data from Twitter to socket')
    # authentication based on the credentials
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # start sending data from the Streaming API
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track = keyword, languages=["en"])


# server (local machine) creates listening socket
s = socket.socket()
host = settings.SOCKET_HOST
port = settings.SOCKET_PORT
s.bind((host, port))
print('socket is ready')
# server (local machine) listens for connections
s.listen(4)
print('socket is listening')
# return the socket and the address on the other side of the connection (client side)
c_socket, addr = s.accept()
print("Received request from: " + str(addr))
# select here the keyword for the tweet data
send_data(c_socket, keyword=[TAG])