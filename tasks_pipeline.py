# default run:
# PYTHONPATH='.' luigi --module tasks_pipeline --local-scheduler PipelineWrappertask
# custom run:
# PYTHONPATH='.' luigi --module tasks_pipeline --local-scheduler PipelineWrappertask(<-configurable params passed->))

import luigi
from luigi.contrib.simulate import RunAnywayTarget
import os
import json
import csv
import luigi.contrib.postgres
import configparser
import logging

from app.utils.constants import SUBREDDIT_CONTENTS_SAVE_DIR, CONFIG_PATH
from app.helpers.prepare_ingestion import PrepareIngestion
from app.helpers.subreddit_ingestion import SubredditIngestion
from app.helpers.rank_subreddits import SubredditsRanking
from app.helpers.ranking_storage import RankingStorage
from app.utils.helper import derive_subreddits_list_save_path, derive_subreddits_rank_save_path, \
    derive_current_timestamp, create_dir, derive_home_dir

logger = logging.getLogger('luigi-interface')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)


class GetAllSubreddits(luigi.Task):
    """
    Gets the latest list of available subreddits from r/ListOfSubreddits

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    home_dir = luigi.Parameter(default=derive_home_dir())

    def requires(self):
        return None

    def output(self):
        subreddits_list_save_file_path = derive_subreddits_list_save_path(self.start, self.home_dir)
        return luigi.LocalTarget(subreddits_list_save_file_path)

    def run(self):

        # Preparing Ingestion, obtaining all available latest subreddits from r/ListOfSubreddits
        prepare = PrepareIngestion()
        subreddits_list = prepare.fetch_all_subreddits_list()

        with self.output().open('w') as f:
            f.write('\n'.join(subreddits_list))


class Ingestion(luigi.Task):
    """
    Ingest the reddit data for each subreddit and each post within that subreddit

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    home_dir = luigi.Parameter(default=derive_home_dir())
    data_lake_dir = luigi.Parameter(default=None)
    save_dir_path = luigi.Parameter(default=None)
    data_dir_path = luigi.Parameter(default=None)

    def output(self):
        # Create directory for the current run
        self.data_lake_dir = os.path.join(self.home_dir, "datalake")
        self.save_dir_path = os.path.join(self.data_lake_dir, str(self.start))
        self.data_dir_path = os.path.join(self.save_dir_path, 'data')

        output_path = os.path.join(self.save_dir_path, "Ingestion_status.txt")
        return luigi.LocalTarget(output_path)

    def run(self):
        # Running the ingestion pipeline to store reddit data for all subreddits and posts
        outputs = []

        create_dir(self.data_lake_dir)
        create_dir(self.save_dir_path)
        create_dir(self.data_dir_path)

        for input in self.input():
            with input.open('r') as list_file:
                subreddits = list_file.readlines()
                # remove whitespace characters like `\n` at the end of each line
                subreddits = [x.strip() for x in subreddits]

                for subreddit_name in subreddits:
                    subreddit_ingestions = IngestSubreddit(subreddit_name=subreddit_name,
                                                           start=self.start,
                                                           top_n_subreddits=self.top_n_subreddits,
                                                           top_n_posts=self.top_n_posts,
                                                           top_n_comments=self.top_n_comments,
                                                           data_dir_path=self.data_dir_path
                                                           )

                    outputs.append(subreddit_ingestions.output().path)
                    yield subreddit_ingestions

        with self.output().open('w') as f:
            f.write("Ingestion complete")

    def requires(self):
        yield GetAllSubreddits(start=self.start,
                               top_n_subreddits=self.top_n_subreddits,
                               top_n_posts=self.top_n_posts,
                               top_n_comments=self.top_n_comments)


class IngestSubreddit(luigi.Task):
    """
    Task to individually ingest the Subreddit data and store as separate output targets based on save_path param

    """
    subreddit_name = luigi.Parameter()
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)
    data_dir_path = luigi.Parameter()

    def run(self):
        # Instantiate the subreddit ingestion object
        subreddit_ingestion = SubredditIngestion(self.subreddit_name,
                                                 self.start,
                                                 self.top_n_subreddits,
                                                 self.top_n_posts,
                                                 self.top_n_comments)

        results = subreddit_ingestion.derive_top_data()

        # Save subreddit data into a subreddit specific file
        with open(self.output().path, "w") as output_file:
            json.dump(results, output_file)

    def output(self):
        # derive the save paths for each subreddit
        subreddit_save_path = os.path.join(self.data_dir_path, f"{self.subreddit_name}.json")
        return luigi.LocalTarget(subreddit_save_path)


class RankSubreddits(luigi.Task):
    """
    Get the ranking of all subreddits based on subreddit score

    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)

    def requires(self):
        yield Ingestion(start=self.start,
                        top_n_subreddits=self.top_n_subreddits,
                        top_n_posts=self.top_n_posts,
                        top_n_comments=self.top_n_comments)

    def output(self):
        output_path = os.path.join(SUBREDDIT_CONTENTS_SAVE_DIR, f"{str(self.start)}", "SubredditsRanking.csv")
        return luigi.LocalTarget(output_path)

    def run(self):
        # Instantiating SubredditsRanking() object
        dir_path = derive_subreddits_rank_save_path(self.start)
        ranking = SubredditsRanking(self.start, dir_path)
        ranking.get_ranking_index()

        # # Save subreddit data into a subreddit specific file
        ranking_data_list = ranking.get_ranking_data()

        # Save sorted_ranking_index data into a subreddits rankings file
        with open(self.output().path, "w") as out:
            csv_out = csv.writer(out)
            # csv_out.writerow(['timestamp', 'rank', 'subreddit', 'subreddit_score', 'storage_location'])
            for row in ranking_data_list:
                csv_out.writerow(row)


class StoreRankings(luigi.Task):
    """
    Store the rankings data onto postgres for historical tracking
    """
    start = luigi.Parameter(default=derive_current_timestamp())
    top_n_subreddits = luigi.IntParameter(default=3)
    top_n_posts = luigi.IntParameter(default=3)
    top_n_comments = luigi.IntParameter(default=3)

    def requires(self):
        yield RankSubreddits(start=self.start,
                             top_n_subreddits=self.top_n_subreddits,
                             top_n_posts=self.top_n_posts,
                             top_n_comments=self.top_n_comments)

    def output(self):
        output_path = os.path.join(SUBREDDIT_CONTENTS_SAVE_DIR, f"{str(self.start)}", "db_insert_status.txt")
        return luigi.LocalTarget(output_path)

    def run(self):
        store_rankings = RankingStorage()

        for input in self.input():
            with input.open('r') as csv_file:
                for line in csv_file:
                    row_elements = line.strip('\n').split(',')
                    store_rankings.insert_into_db(row_elements)

        with self.output().open('w') as f:
            f.write("Finished writing to database")


class PipelineWrappertask(luigi.WrapperTask):
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
        # base_tasks = [GetAllSubreddits(start=self.start,
        #                                top_n_subreddits=self.top_n_subreddits,
        #                                top_n_posts=self.top_n_posts,
        #                                top_n_comments=self.top_n_comments),
        #               Ingestion(start=self.start,
        #                         top_n_subreddits=self.top_n_subreddits,
        #                         top_n_posts=self.top_n_posts,
        #                         top_n_comments=self.top_n_comments)]

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

        # return base_tasks

    def output(self):
        return RunAnywayTarget(self)


if __name__ == '__main__':
    luigi.run()
