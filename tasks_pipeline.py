# default run:
# PYTHONPATH='.' luigi --module tasks_pipeline --local-scheduler PipelineWrappertask
# custom run:
# PYTHONPATH='.' luigi --module tasks_pipeline --local-scheduler PipelineWrappertask(<-configurable params passed->))

from luigi.contrib.simulate import RunAnywayTarget
import luigi.contrib.postgres
import configparser
import logging

from app.utils.constants import CONFIG_PATH
from app.utils.helper import derive_current_timestamp, derive_home_dir

from app.tasks.all_subreddits import GetAllSubreddits
from app.tasks.ingestion import Ingestion
from app.tasks.rank_subreddits import RankSubreddits
from app.tasks.store_rankings import StoreRankings

logger = logging.getLogger('luigi-interface')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)


class PipelineWrapperTask(luigi.WrapperTask):
    """
    A wrapper tasks that runs the entire pipeline in a specific order
    :params: Custom parameters can be passed for task start timestamp, top 'n' subreddits, posts and comments if necessary.

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    home_dir = luigi.Parameter(default=derive_home_dir())

    def run(self):
        self.output().done()

    def requires(self):

        yield GetAllSubreddits(start=self.start,
                               top_n_subreddits=self.top_n_subreddits,
                               top_n_posts=self.top_n_posts,
                               top_n_comments=self.top_n_comments,
                               home_dir=self.home_dir)

        yield Ingestion(start=self.start,
                        top_n_subreddits=self.top_n_subreddits,
                        top_n_posts=self.top_n_posts,
                        top_n_comments=self.top_n_comments,
                        home_dir=self.home_dir)

        yield RankSubreddits(start=self.start,
                             top_n_subreddits=self.top_n_subreddits,
                             top_n_posts=self.top_n_posts,
                             top_n_comments=self.top_n_comments)

        yield StoreRankings(start=self.start,
                            top_n_subreddits=self.top_n_subreddits,
                            top_n_posts=self.top_n_posts,
                            top_n_comments=self.top_n_comments)

    def output(self):
        return RunAnywayTarget(self)


if __name__ == '__main__':
    luigi.run()
