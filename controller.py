# package imports
from threading import Thread
import time

# local imports
import TwitterServer
import ClientGenerator


# Set up the Twitter streaming server
thread_twitter_server = Thread(target=TwitterServer.generate_server_sockets)
# Start up the sentiment analysis clients
thread_client_generator = Thread(target=ClientGenerator.generate_clients)


def run():
    thread_twitter_server.start()

    # wait two seconds to give time for the sockets to be established
    time.sleep(2)

    thread_client_generator.start()


if __name__ == '__main__':
    run()






