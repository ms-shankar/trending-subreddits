import json

import luigi
import luigi.interface
import re
from luigi.configuration import add_config_path
from luigi.mock import MockTarget
from tests.test_config.constants import INPATH
import unittest
import os
import shutil
from app.tasks.all_subreddits import GetAllSubreddits
from tests.test_config.constants import SUBREDDIT_CONTENTS_SAVE_DIR


class TestAllTasks(unittest.TestCase):
    def setUp(self):
        if os.path.exists(SUBREDDIT_CONTENTS_SAVE_DIR):
            shutil.rmtree(SUBREDDIT_CONTENTS_SAVE_DIR, ignore_errors=True)

    def teardown(self):
        shutil.rmtree(SUBREDDIT_CONTENTS_SAVE_DIR, ignore_errors=True)

    def test_get_all_subreddits_task(self):

        # Act
        luigi.build([GetAllSubreddits()], local_scheduler=True, no_lock=True, workers=1)

        # Assert
        self.assertEqual(GetAllSubreddits().status, "Completed")

    def test_get_all_subreddits_task(self):

        # Act
        luigi.build([GetAllSubreddits()], local_scheduler=True, no_lock=True, workers=1)

        # Assert
        self.assertEqual(GetAllSubreddits().status, "Completed")


if __name__ == '__main__':
    unittest.main()
