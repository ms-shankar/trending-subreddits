from parameterized import parameterized
import unittest
from app.helpers.rank_subreddits import SubredditsRanking
from tests.test_config.constants import SUBREDDIT_CONTENTS_SAVE_DIR_1, START_DATE_1, SUBREDDIT_CONTENTS_SAVE_DIR_2, \
    START_DATE_2
from collections import OrderedDict


def get_ranking_index_test_inputs():
    # parameterize input data and expected result as test case inputs
    return [(SUBREDDIT_CONTENTS_SAVE_DIR_1,
             START_DATE_1,
             OrderedDict([('RatedChess', 1.25), ('IndianMusicOnline', 0.14893617021276595)])),
            (SUBREDDIT_CONTENTS_SAVE_DIR_2,
             START_DATE_2,
             OrderedDict([('IndianMusicOnline', 3.14893617021276595), ('RatedChess', 1.25)]))]


class TestSubredditsRanking(unittest.TestCase):

    def arrange_fixtures(self, start_date, save_dir):
        return SubredditsRanking(start_date, save_dir)

    @parameterized.expand(get_ranking_index_test_inputs)
    def test_get_ranking_index(self, save_dir, start_date, expected_result):
        # Arrange
        ranking = self.arrange_fixtures(start_date, save_dir)

        # Act
        ranking.get_ranking_index()

        # Assert
        self.assertEqual(ranking.sorted_ranking_index, expected_result)


if __name__ == '__main__':
    unittest.main()
