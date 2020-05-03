import os

HOME_DIR = os.getcwd().split('/app/utils')[0]

ALL_SUBREDDITS_URL = "https://www.reddit.com/r/ListOfSubreddits/wiki/listofsubreddits.json"

SUBREDDIT_CONTENTS_SAVE_DIR = os.path.join(HOME_DIR, "datalake")

INGESTION_TASKS_STATUS_PATH = os.path.join(HOME_DIR, "ingestion_status.txt")
PIPELINE_STATUS_PATH = os.path.join(HOME_DIR, "pipeline_status.txt")
SUBREDDITS_RANKING_PATH = os.path.join(HOME_DIR, "subreddits_ranking.json")

CONFIG_PATH = os.path.join(HOME_DIR, 'app', 'utils', 'config.ini')