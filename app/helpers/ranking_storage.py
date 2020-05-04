from app.utils.constants import SUBREDDIT_CONTENTS_SAVE_DIR
import os
import psycopg2
import csv
from app.utils.helper import derive_db_config_value


class RankingStorage:
    """
    A helper class for the primary task StoreRankings, that performs all task specific operations

    """
    def __init__(self):
        # self.rankings_csv = os.path.join(SUBREDDIT_CONTENTS_SAVE_DIR, f"{start_date}", "SubredditsRanking.csv")
        self.host = derive_db_config_value('host')
        self.user = derive_db_config_value('user')
        self.dbname = derive_db_config_value('dbname')
        self.password = derive_db_config_value('password')
        self.conn = self.get_db_conn()

    def get_db_conn(self):
        """
        Derive the connection string and supply PostgresDB connection
        :return conn: Connection to the specified database and table
        """
        connection_string = f"host={self.host} dbname={self.dbname} user={self.user} password={self.password}"
        return psycopg2.connect(connection_string)

    def insert_into_db(self, row):
        """
        Insert individual rankings row data into the database rankings table
        :param row: The ranking associated with each subreddit rank to be inserted into the rankings table
        """
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO subreddit_rankings VALUES (%s, %s, %s, %s, %s)",
                       (row[0], row[1], row[2], row[3], row[4]))
        self.conn.commit()



