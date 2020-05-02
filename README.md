# Trending Subreddits

**Trending Subreddits** is an application that ingests, ranks and tracks top subreddits on Reddit on a daily basis. The pipleline is orchestrated using Luigi.

## Setup

### Run PostgreSQL DB using docker
```
$ docker run --name trending-subreddits-db -e POSTGRES_PASSWORD=password -d -p 5432:5432 postgres
```
```
$ docker exec -it <container_id> bash
```
```
$ psql -U postgres
```
```
$ CREATE DATABASE db_rankings;
```
```
$ \c db_rankings;
```
```
$ CREATE TABLE subreddit_rankings(timestamp VARCHAR(20), rank INT, subreddit VARCHAR(50), subreddit_score FLOAT, storage_location VARCHAR(250));

```

