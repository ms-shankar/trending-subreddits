from parameterized import parameterized
import unittest
from app.helpers.prepare_ingestion import PrepareIngestion


def extract_subreddit_names_test_inputs():
    # parameterize input data and expected result as test case inputs
    return [(" \r\n/r/IndianMusicOnline  \r\n/r/RatedChess", ['IndianMusicOnline', 'RatedChess']),
            (" \r\n/r/IndianMusicOnline", ['IndianMusicOnline'])]


class TestPrepareIngestion(unittest.TestCase):

    def arrange_fixtures(self):
        return PrepareIngestion()

    @parameterized.expand(extract_subreddit_names_test_inputs)
    def test_extract_subreddit_names(self, input_string, expected_result):
        # Arrange
        prepare = self.arrange_fixtures()

        # Act
        result = prepare.extract_subreddit_names(input_string)

        # Assert
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
