import os
import glob
import json
from collections import OrderedDict
from operator import itemgetter


class SubredditsRanking:
    """
    A helper class for the primary task RankSubreddits, that performs all task specific operations
    """

    def __init__(self, start_date, dir_path):
        self.start_date = start_date
        self.dir_path = dir_path
        self.unsorted_ranking_index = {}
        self.sorted_ranking_index = OrderedDict()

    def get_ranking_index(self):
        """
        Generates the subreddit ranking index, an OrderedDict() in the form {"subreddit_name1": "subreddit_score1,..}
        """
        all_files = os.path.join(self.dir_path, f"*")
        saved_files_list = glob.glob(all_files)
        for subreddit_file in saved_files_list:
            with open(subreddit_file, "r") as input_file:
                subreddit_data = json.load(input_file)

                # Populate unordered ranking index dict with type {subreddit_name: subreddit_score}
                update_item = {subreddit_data['subreddit']: subreddit_data['subreddit_score']}
                self.unsorted_ranking_index.update(update_item)

        # Create sorted ranking index by sorting unordered ranking index by dict values (subreddit_score)
        # in decreasing order to obtain best to worst rankings
        self.sorted_ranking_index = OrderedDict(sorted(self.unsorted_ranking_index.items(),
                                                       key=itemgetter(1),
                                                       reverse=True))

    def get_ranking_data(self):
        """
        Generates the final subreddit rankings list
        :return ranking_data_list: A list of ranked subreddits along with their scores and storage paths
        """
        ranking_data_list = []
        current_rank = 0

        for subreddit, score in self.sorted_ranking_index.items():
            current_rank += 1
            subreddit_save_path = self.get_saved_path(subreddit)
            ranking_data_list.append(tuple([self.start_date, current_rank, subreddit, score, subreddit_save_path]))

        return ranking_data_list

    def get_saved_path(self, subreddit):
        """
        Derive the storage path of each Subreddit specific json file
        :param subreddit: The subreddit name
        :return: Contents storage path of the subreddit name
        """
        return os.path.join(self.dir_path, f"{subreddit}.json")




