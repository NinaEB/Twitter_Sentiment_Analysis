# package imports
import subprocess

# local imports
import settings

# This is the script that will be called to initialize a sentiment analyzer client process
client_script_name = "SentimentAnalysis_Script.py"

# Pull the keywords and socket settings from settings.py
keywords = settings.KEYWORDS

print('Keywords for sentiment analysis clients: ', keywords)


def generate_clients():
    # For each keyword, run a subprocess of SentimentAnalysis_Script.py
    for index, keyword in enumerate(keywords):
        command = 'python ' + client_script_name + ' ' + str(index)

        print('Starting a sentiment analyzer client for keyword "' + keyword + '"')
        print('--> Running command: ' + command)
        cp = subprocess.Popen([command], shell=True, universal_newlines=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    return True


if __name__ == '__main__':
    generate_clients()