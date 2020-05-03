import os
import datetime
from app.utils.constants import SUBREDDIT_CONTENTS_SAVE_DIR, HOME_DIR, CONFIG_PATH
import configparser


def cleanup_exisiting_files(file_path):
    # Handle errors while calling os.remove()
    try:
        os.remove(file_path)
    except FileNotFoundError:
        print(f"No file in path {file_path} to delete")


def derive_current_timestamp():
    current_datetime = datetime.datetime.now()
    timestamp = f"{current_datetime.day}{current_datetime.month}{current_datetime.year}" \
        f"{current_datetime.hour}{current_datetime.minute}{current_datetime.second}"
    return timestamp


def derive_subreddits_rank_save_path(start_date):
    return os.path.join(SUBREDDIT_CONTENTS_SAVE_DIR, f"{start_date}", 'data')


def derive_subreddits_list_save_path(start_date, home_dir):
    filename = f"ListOfSubreddits_{start_date}.txt"
    return os.path.join(home_dir, 'datalake', start_date, filename)


def derive_db_config_value(param):
    # Obtain connection details from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config['POSTGRES'][param]


def create_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def derive_home_dir():
    return HOME_DIR





