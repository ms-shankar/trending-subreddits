# Trending Subreddits

**Trending Subreddits** is an application that ingests, ranks and tracks top subreddits of Reddit on a daily basis. 
The entire pipleline is orchestrated using Luigi. The final ranking result is obtained as a CSV report, which is also stored in PostgreSQL DB.

## Installation

This section describes the steps that need to be followed for setting up the project and its dependencies

### Pipeline setup

The pipeline setup can be achieved by running the following simple commands

- Cleanup pre-existing virtual environments
```
$ make clean
```
- Create a new virtual env and install dependencies
```
$ make setup
```

### Database setup

- Run PostgreSQL DB as a docker container
```
$ docker run --name trending-subreddits-db -e POSTGRES_PASSWORD=<password> -d -p 5432:5432 postgres
```
- Configure the DB container and create necessary resources
```
$ docker exec -it <container_id> bash
```
- Connect to the DB
```
$ psql -U postgres
```
- Create a new database to store rankings data
```
CREATE DATABASE db_rankings;
```
- Connect to the newly created database
```
\c db_rankings;
```
- Create a new table to store rankings data
```
CREATE TABLE subreddit_rankings (timestamp VARCHAR(20), rank INT, subreddit VARCHAR(50), subreddit_score FLOAT, storage_location VARCHAR(250));
```

## Configuration

This section describes the configuration requirements.

### Provide config values for API and DB connection

- Ensure that the `[POSTGRES]` and `[REDDIT]` sections in `app/utils/config.ini` config file are provided with the correct 
configuration values.

## Testing

This section describes how to run the unit tests.

### Invoke unit tests using Makefile
```
$ make test
```
***Alternately***

### Invoke unit tests normally
```
$ python -m unittest -v
```

## Run
This section describes how to run the entire workflow of tasks.

### Execute workflow wrapper task using local scheduler
```
$ PYTHONPATH='.' luigi --module tasks_pipeline --local-scheduler PipelineWrapperTask
```
### Execute workflow wrapper task using central scheduler
```
$ python tasks_pipeline.py PipelineWrappertask
```
### NOTE
- In order to quickly test the entire pipeline with minimal processing of subreddits, 
please use the file `minimal_listofsubreddits.json` indicated by the path `MINIMAL_SUBREDDITS_JSON`
instead of `listofsubreddits.json`, which is indicated by the path `ALL_SUBREDDITS_JSON` in `app/helpers/prepare_ingestion.py`






