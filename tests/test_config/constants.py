import os

HOME_DIR = os.getcwd()

SUBREDDIT_CONTENTS_SAVE_DIR_1 = os.path.join(HOME_DIR, "tests", "test_data", "35202015316", "data")
START_DATE_1 = "35202015316"

SUBREDDIT_CONTENTS_SAVE_DIR_2 = os.path.join(HOME_DIR, "tests", "test_data", "35202015317", "data")
START_DATE_2 = "35202015317"

# ALL_SUBREDDITS_URL = "https://www.reddit.com/r/ListOfSubreddits/wiki/listofsubreddits.json"
# INGESTION_TASKS_STATUS_PATH = os.path.join(HOME_DIR, "ingestion_status.txt")
# PIPELINE_STATUS_PATH = os.path.join(HOME_DIR, "pipeline_status.txt")
# SUBREDDITS_RANKING_PATH = os.path.join(HOME_DIR, "subreddits_ranking.json")
#
# CONFIG_PATH = os.path.join(HOME_DIR, 'app', 'utils', 'config.ini')