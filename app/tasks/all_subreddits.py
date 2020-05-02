import luigi
from app.helpers.prepare_ingestion import PrepareIngestion
from app.utils.helper import derive_subreddits_list_save_path, derive_current_timestamp


class GetAllSubreddits(luigi.Task):
    """
    Gets the latest list of available subreddits from r/ListOfSubreddits
    """
    start = luigi.Parameter(default=derive_current_timestamp())
    # Obtain values for n which is configurable
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)

    def requires(self):
        return None

    def output(self):
        subreddits_list_save_file_path = derive_subreddits_list_save_path(self.start)
        return luigi.LocalTarget(subreddits_list_save_file_path)

    def run(self):

        # Preparing Ingestion, obtaining all available latest subreddits from r/ListOfSubreddits
        prepare = PrepareIngestion()
        subreddits_list = prepare.fetch_all_subreddits_list()

        with self.output().open('w') as f:
            f.write('\n'.join(subreddits_list))
