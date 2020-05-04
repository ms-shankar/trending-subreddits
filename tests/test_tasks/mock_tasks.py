import luigi.interface
from luigi.mock import MockTarget
from tests.test_config.constants import INPATH

from app.tasks.all_subreddits import GetAllSubreddits


class MockBaseTask(luigi.Task):

    def output(self):
        return MockTarget(INPATH)

    def run(self):
        f = self.output().open('w')
        f.close()


class GetAllSubredditsMock(GetAllSubreddits):

    def requires(self):
        return [MockBaseTask()]

    def output(self):
        return MockTarget(INPATH)
