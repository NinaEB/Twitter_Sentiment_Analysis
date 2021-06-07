# package imports
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
from threading import Thread
from http.client import IncompleteRead

# local imports
import credentials
import settings


# Debug option to include print output
_DEBUG = False

# Get API credentials from credentials.py
consumer_key = credentials.API_KEY
consumer_secret = credentials.API_SECRET_KEY
access_token = credentials.ACCESS_TOKEN
access_secret = credentials.ACCESS_TOKEN_SECRET

# Grab the keyword from the settings.py file
keywords = settings.KEYWORDS
print('Keywords for Twitter streaming server: ', keywords)


# Define a StreamListener
class TweetsListener(StreamListener):
    # Constructor
    def __init__(self, csocket):
        self.client_socket = csocket

    # When data is received, extract the Tweet text from the JSON response and send 
    # it to the client socket
    def on_data(self, data):
        try:
            json_message_from_stream = json.loads(data)

            # if tweet is longer than 140 characters, grab the extended_tweet value
            if "extended_tweet" in json_message_from_stream:
                tweet_text = str(json_message_from_stream['extended_tweet']['full_text'])
            else:
                tweet_text = str(json_message_from_stream['text'])

            if _DEBUG:
                print("\n===================== New tweet from stream =========================\n")
                print(tweet_text)
                print("\n=====================================================================")
            
            if tweet_text != '':
                # add at the end of each tweet "t_end"
                message_to_send = tweet_text + 't_end'
                self.client_socket.send(message_to_send.encode('utf-8'))
            return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    # When an error occurs, print the error
    def on_error(self, status):
        print(status)
        return True


# Function to send data from Twitter
def send_data(server_socket, filter_keyword):
    client_socket, addr = server_socket.accept()
    print('Start sending data from Twitter to socket for keyword: ', filter_keyword)
    # authentication based on the credentials
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # start sending data from the Streaming API
    twitter_stream = Stream(auth, TweetsListener(client_socket))
    try:
        twitter_stream.filter(track = filter_keyword, is_async=True, languages=["en"])
    except IncompleteRead:
        print("Empty stream, continuing...")


def generate_server_sockets():
    # Create streaming socket(s), one for each keyword
    for index, keyword in enumerate(keywords):
        s = socket.socket()
        host = settings.SOCKET_HOST
        port = settings.SOCKET_PORT + index
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        print('Socket ' + str(index) + ' is ready.')
        s.listen(4)
        print('Socket ' + str(index) + ' is listening.')
        Thread(target=send_data, args=[s, [keyword]]).start()

    return True


if __name__ == '__main__':
    generate_server_sockets()