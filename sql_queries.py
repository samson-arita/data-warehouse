import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR, 
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length NUMERIC(16,6),
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration BIGINT,
sessionId VARCHAR,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
artist_id VARCHAR,
artist_latitude NUMERIC(16,6),
artist_location VARCHAR,
artist_longitude NUMERIC(16,6),
artist_name VARCHAR,
duration NUMERIC(16,6),
num_songs INT,
song_id VARCHAR,
title VARCHAR,
year INT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
start_time TIMESTAMP NOT NULL, 
user_id INT NOT NULL, 
level VARCHAR, 
song_id VARCHAR NOT NULL, 
artist_id VARCHAR NOT NULL, 
session_id VARCHAR NOT NULL, 
location TEXT, 
user_agent TEXT);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id INT PRIMARY KEY NOT NULL, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR, 
level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR PRIMARY KEY NOT NULL, 
title VARCHAR, 
artist_id VARCHAR, 
year INT, 
duration NUMERIC(16,6));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id VARCHAR PRIMARY KEY NOT NULL, 
name TEXT, 
location TEXT, 
latitude NUMERIC(16,6), 
longitude NUMERIC(16,6));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP PRIMARY KEY NOT NULL, 
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday INT);
""")


# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}' 
format as json {} compupdate off region 'us-west-2';
""").format(config['S3']['log_data'],config['IAM_ROLE']['arn'],config['S3']['log_jsonpath'])

staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}' 
format as json 'auto' compupdate off region 'us-west-2';
""").format(config['S3']['song_data'],config['IAM_ROLE']['arn'])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
        TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS start_time, 
        se.userId AS user_id, 
        se.level AS level, 
        ss.song_id AS song_id, 
        ss.artist_id AS artist_id,
        se.sessionId AS session_id, 
        se.location AS location, 
        se.userAgent AS user_agent
FROM staging_songs ss
JOIN staging_events se 
on (ss.artist_name=se.artist)
WHERE page='NextSong' 
and start_time is NOT NULL 
and user_id is NOT NULL 
and song_id is NOT NULL 
and artist_id is NOT NULL 
and session_id is NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
        DISTINCT(userId) AS user_id, 
        firstName AS first_name,
        lastName AS last_name, 
        gender, 
        level
FROM staging_events
WHERE userId is NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
        DISTINCT(song_id),
        title,
        artist_id,
        year,
        duration
FROM staging_songs
WHERE song_id is NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
        DISTINCT(artist_id), 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude
FROM staging_songs
WHERE artist_id is NOT NULL;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
        DISTINCT(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS start_time, 
        EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS hour, 
        EXTRACT(day  FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS day,
        EXTRACT(week  FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS week, 
        EXTRACT(month  FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS month, 
        EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS year,
        EXTRACT(dayofweek  FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS weekday        
FROM staging_events
WHERE ts is NOT NULL;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
