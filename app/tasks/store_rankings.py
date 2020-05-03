import luigi
import os
import luigi.contrib.postgres
import configparser
import logging

from app.utils.constants import SUBREDDIT_CONTENTS_SAVE_DIR, CONFIG_PATH
from app.tasks.rank_subreddits import RankSubreddits
from app.helpers.ranking_storage import RankingStorage
from app.utils.helper import derive_current_timestamp

logger = logging.getLogger('luigi-interface')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)


class StoreRankings(luigi.Task):
    """
    Store the rankings data onto postgres for historical tracking
    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)

    def requires(self):
        yield RankSubreddits(start=self.start,
                             top_n_subreddits=self.top_n_subreddits,
                             top_n_posts=self.top_n_posts,
                             top_n_comments=self.top_n_comments)

    def output(self):
        output_path = os.path.join(SUBREDDIT_CONTENTS_SAVE_DIR, f"{str(self.start)}", "db_insert_status.txt")
        return luigi.LocalTarget(output_path)

    def run(self):
        store_rankings = RankingStorage()

        for input in self.input():
            with input.open('r') as csv_file:
                for line in csv_file:
                    row_elements = line.strip('\n').split(',')
                    store_rankings.insert_into_db(row_elements)

        with self.output().open('w') as f:
            f.write("Finished writing to database")
