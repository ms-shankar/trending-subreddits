import os

HOME_DIR = os.getcwd()

SUBREDDIT_CONTENTS_SAVE_DIR_1 = os.path.join(HOME_DIR, "tests", "test_data", "35202015316", "data")
START_DATE_1 = "35202015316"

SUBREDDIT_CONTENTS_SAVE_DIR_2 = os.path.join(HOME_DIR, "tests", "test_data", "35202015317", "data")
START_DATE_2 = "35202015317"

RANK1_PATH_1 = os.path.join(f"{SUBREDDIT_CONTENTS_SAVE_DIR_1}", "RatedChess.json")
RANK2_PATH_1 = os.path.join(f"{SUBREDDIT_CONTENTS_SAVE_DIR_1}", "IndianMusicOnline.json")

RANK1_PATH_2 = os.path.join(f"{SUBREDDIT_CONTENTS_SAVE_DIR_2}", "IndianMusicOnline.json")
RANK2_PATH_2 = os.path.join(f"{SUBREDDIT_CONTENTS_SAVE_DIR_2}", "RatedChess.json")

RANKING_DATA_1 = [('35202015316', 1, 'RatedChess', 1.25, RANK1_PATH_1),
                  ('35202015316', 2, 'IndianMusicOnline', 0.14893617021276595, RANK2_PATH_1)]

RANKING_DATA_2 = [('35202015317', 1, 'IndianMusicOnline', 3.148936170212766, RANK1_PATH_2),
                  ('35202015317', 2, 'RatedChess', 1.25, RANK2_PATH_2)]
