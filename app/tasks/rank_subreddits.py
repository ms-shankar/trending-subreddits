import luigi
import luigi.contrib.postgres
import os
import csv

from app.utils.constants import SUBREDDIT_CONTENTS_SAVE_DIR
from app.helpers.rank_subreddits import SubredditsRanking
from app.utils.helper import derive_subreddits_rank_save_path, derive_current_timestamp
from app.tasks.ingestion import Ingestion


class RankSubreddits(luigi.Task):
    """
    Get the ranking of all subreddits based on subreddit score

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)

    def requires(self):
        yield Ingestion(start=self.start,
                        top_n_subreddits=self.top_n_subreddits,
                        top_n_posts=self.top_n_posts,
                        top_n_comments=self.top_n_comments)

    def output(self):
        output_path = os.path.join(SUBREDDIT_CONTENTS_SAVE_DIR, f"{str(self.start)}", "SubredditsRanking.csv")
        return luigi.LocalTarget(output_path)

    def run(self):
        # Instantiating SubredditsRanking() object
        dir_path = derive_subreddits_rank_save_path(self.start)
        ranking = SubredditsRanking(self.start, dir_path)
        ranking.get_ranking_index()

        # Save subreddit data into a subreddit specific file
        ranking_data_list = ranking.get_ranking_data()

        # Save sorted_ranking_index data into a subreddits rankings file
        with open(self.output().path, "w") as out:
            csv_out = csv.writer(out)
            # csv_out.writerow(['timestamp', 'rank', 'subreddit', 'subreddit_score', 'storage_location'])
            for row in ranking_data_list:
                csv_out.writerow(row)
