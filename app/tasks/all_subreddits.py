import luigi
import luigi.contrib.postgres

from app.helpers.prepare_ingestion import PrepareIngestion
from app.utils.helper import derive_subreddits_list_save_path, derive_current_timestamp, derive_home_dir


class GetAllSubreddits(luigi.Task):
    """
    Gets the latest list of available subreddits from r/ListOfSubreddits

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    home_dir = luigi.Parameter(default=derive_home_dir())

    def requires(self):
        return None

    def output(self):
        subreddits_list_save_file_path = derive_subreddits_list_save_path(self.start, self.home_dir)
        return luigi.LocalTarget(subreddits_list_save_file_path)

    def run(self):

        # Preparing Ingestion, obtaining all available latest subreddits from r/ListOfSubreddits
        prepare = PrepareIngestion()
        subreddits_list = prepare.fetch_all_subreddits_list()

        with self.output().open('w') as f:
            f.write('\n'.join(subreddits_list))
