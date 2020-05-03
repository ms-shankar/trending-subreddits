import luigi
import os
import luigi.contrib.postgres

from app.utils.helper import derive_current_timestamp, create_dir, derive_home_dir
from app.tasks.all_subreddits import GetAllSubreddits
from app.tasks.ingest_subreddit import IngestSubreddit


class Ingestion(luigi.Task):
    """
    Ingest the reddit data for each subreddit and each post within that subreddit

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    home_dir = luigi.Parameter(default=derive_home_dir())
    data_lake_dir = luigi.Parameter(default=None)
    save_dir_path = luigi.Parameter(default=None)
    data_dir_path = luigi.Parameter(default=None)

    def output(self):
        # Create directory for the current run
        self.data_lake_dir = os.path.join(self.home_dir, "datalake")
        self.save_dir_path = os.path.join(self.data_lake_dir, str(self.start))
        self.data_dir_path = os.path.join(self.save_dir_path, 'data')

        output_path = os.path.join(self.save_dir_path, "Ingestion_status.txt")
        return luigi.LocalTarget(output_path)

    def run(self):
        # Running the ingestion pipeline to store reddit data for all subreddits and posts
        outputs = []

        create_dir(self.data_lake_dir)
        create_dir(self.save_dir_path)
        create_dir(self.data_dir_path)

        for input in self.input():
            with input.open('r') as list_file:
                subreddits = list_file.readlines()
                # remove whitespace characters like `\n` at the end of each line
                subreddits = [x.strip() for x in subreddits]

                for subreddit_name in subreddits:
                    subreddit_ingestions = IngestSubreddit(subreddit_name=subreddit_name,
                                                           start=self.start,
                                                           top_n_subreddits=self.top_n_subreddits,
                                                           top_n_posts=self.top_n_posts,
                                                           top_n_comments=self.top_n_comments,
                                                           data_dir_path=self.data_dir_path
                                                           )

                    outputs.append(subreddit_ingestions.output().path)
                    yield subreddit_ingestions

        with self.output().open('w') as f:
            f.write("Ingestion complete")

    def requires(self):
        yield GetAllSubreddits(start=self.start,
                               top_n_subreddits=self.top_n_subreddits,
                               top_n_posts=self.top_n_posts,
                               top_n_comments=self.top_n_comments)
