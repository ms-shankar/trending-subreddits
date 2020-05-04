import luigi
import os
import luigi.contrib.postgres
import json

from app.utils.helper import derive_current_timestamp
from app.helpers.subreddit_ingestion import SubredditIngestion


class IngestSubreddit(luigi.Task):
    """
    Task to individually ingest the Subreddit data and store as separate output targets

    """
    subreddit_name = luigi.Parameter()
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    data_dir_path = luigi.Parameter()

    def run(self):
        # Instantiate the subreddit ingestion object
        subreddit_ingestion = SubredditIngestion(self.subreddit_name,
                                                 self.start,
                                                 self.top_n_subreddits,
                                                 self.top_n_posts,
                                                 self.top_n_comments)

        results = subreddit_ingestion.derive_top_data()

        # Save subreddit data into a subreddit specific file
        with open(self.output().path, "w") as output_file:
            json.dump(results, output_file)

    def output(self):
        # derive the save paths for each subreddit
        subreddit_save_path = os.path.join(self.data_dir_path, f"{self.subreddit_name}.json")
        return luigi.LocalTarget(subreddit_save_path)
