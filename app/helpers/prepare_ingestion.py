import urllib
import json
import requests

from app.utils.constants import ALL_SUBREDDITS_URL


class PrepareIngestion:

    def __init__(self):
        self.url = ALL_SUBREDDITS_URL
        self.contents = None

    def fetch_all_subreddits_list(self):
        # req = urllib.request.Request(self.url)
        # response = urllib.request.urlopen(req)
        # data = response.read()
        # self.contents = json.loads(data)

        # The following needs to be removed and the above needs to be uncommented
        with open('/Users/s.sathyakumari/Downloads/projects/pipeline-projects/trending-subreddits/listofsubreddits.json', 'r') as f:
            self.contents = json.load(f)

        unprocessed_string = self.contents['data']['content_md']
        return self.extract_subreddit_names(unprocessed_string)

    @staticmethod
    def extract_subreddit_names(input_string):
        all_subreddits_list = []

        # split string to generate words
        words = input_string.split(' ')
        # print("words", words)

        # select only the subreddits names from the file starting with "/r/"
        all_subreddit_handles = [word for word in words if word.startswith("/r/") or word.startswith("\r\n/r/")]
        # print("subreddits", len(all_subreddits))

        for subreddit in all_subreddit_handles:
            if '.' in subreddit:
                subreddit = subreddit.rstrip(".")

            subreddit = subreddit.split("/r/")[1]
            all_subreddits_list.append(subreddit)

        return all_subreddits_list

