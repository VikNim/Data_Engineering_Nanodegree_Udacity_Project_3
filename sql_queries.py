"""
    - This file provides all sql queries to create, drop tables and
    - to insert data into tables.
"""
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
ARN = config.get("IAM_ROLE","ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE events_staging (artist TEXT,"+
                              " auth VARCHAR NOT NULL, firstName VARCHAR,"+
                              " gender VARCHAR(2), itemInSession INTEGER"+
                              " NOT NULL, lastName VARCHAR, length DOUBLE"+
                              " PRECISION, level VARCHAR, location TEXT,"+
                              " method VARCHAR, page VARCHAR, registration DOUBLE"+
                              " PRECISION, sessionId INTEGER NOT NULL, song VARCHAR,"+
                              "status INTEGER, ts BIGINT, userAgent"+
                              " TEXT, userId VARCHAR)")

staging_songs_table_create = ("CREATE TABLE songs_staging (num_songs INTEGER NOT NULL,"+
                              " artist_id VARCHAR, artist_latitude VARCHAR,"+
                              " artist_longitude VARCHAR, artist_location VARCHAR,"+
                              " artist_name VARCHAR, song_id VARCHAR NOT NULL,"+
                              " title VARCHAR NOT NULL, duration DOUBLE PRECISION NOT NULL,"+
                              " year INTEGER)")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id INTEGER IDENTITY(0,1) " +
                         "PRIMARY KEY, start_time timestamp NOT NULL, user_id TEXT NOT NULL, " +
                         "level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id" +
                         " INTEGER, location VARCHAR, user_agent VARCHAR, FOREIGN KEY(user_id)" +
                         " REFERENCES users(user_id), FOREIGN KEY(song_id) REFERENCES songs" +
                         "(song_id), FOREIGN KEY(artist_id) REFERENCES artists(artist_id))"
)

user_table_create = ("CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY," + 
                     " first_name VARCHAR, last_name VARCHAR,gender VARCHAR, level" +
                     " VARCHAR NOT NULL)")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY," + 
                     " title VARCHAR, artist_id VARCHAR NOT NULL, year INTEGER, duration " +
                     "NUMERIC, FOREIGN KEY(artist_id) REFERENCES artists(artist_id))")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY"+
                       " KEY, name VARCHAR, location VARCHAR, latitude NUMERIC, longitude NUMERIC)")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (time_id INTEGER IDENTITY(0,1) PRIMARY KEY," +
                     "start_time timestamp,hour INTEGER, day INTEGER, week INTEGER, month INTEGER,"+
                     "year INTEGER, weekday VARCHAR)")

# STAGING TABLES

staging_events_copy = (f"COPY events_staging from {LOG_DATA} credentials 'aws_iam_role={ARN}' format as json {LOG_JSONPATH} region 'us-west-2';")

staging_songs_copy = (f"COPY songs_staging from {SONG_DATA} credentials 'aws_iam_role={ARN}' format as json 'auto' region 'us-west-2';")

# FINAL TABLES

songplay_table_insert = ("insert into songplays (start_time, user_id, level, song_id, "+
                         "artist_id,session_id, location, user_agent) select "+
                         "(timestamp 'epoch' + e.ts / 1000 * interval '1 second') as start_time,"+
                         "e.userId as user_id, e.level as level,"+
                         "s.song_id as song_id, s.artist_id as artist_id, "+
                         "e.sessionId as session_id, e.location as location,"
                         "e.userAgent as user_agent"+
                         " from events_staging e, songs_staging s"+
                         " where e.song = s.title and e.artist = s.artist_name and e.page = 'NextSong'")

user_table_insert = ("insert into users(user_id, first_name, last_name, gender, level)"+
                     " select a.userId as user_id, a.firstName as first_name,"+
                     " a.lastName as last_name, a.gender as gender, "+
                     "a.level as level from events_staging a,(select max(ts)"+
                     " as ts, userId from events_staging where page = 'NextSong'"+
                     " group by userId) b where a.userId = b.userId and a.ts = b.ts")

song_table_insert = ("insert into songs (song_id, title, artist_id, "+
                     "year, duration) select song_id, title, "+
                     "artist_id, year, duration from songs_staging")

artist_table_insert = ("insert into artists ( artist_id, name, location, "+
                       "latitude, longitude ) select distinct artist_id, "+
                       "artist_name as name, artist_location as location, "+
                       "artist_latitude as latitude, artist_longitude as longitude "+
                       "from songs_staging")

time_table_insert =("insert into time (start_time,hour, day,"+
                    " week, month, year, weekday) select a.start_time as start_time,"+
                    "extract(hour from a.start_time) as hour,"+
                    "extract(day from a.start_time) as day,"+
                    "extract(week from a.start_time) as week,"+
                    "extract(month from a.start_time) as month,"+
                    "extract(year from a.start_time) as year,"+
                    "extract(weekday from a.start_time) as weekday"+
                    " from (select distinct "+
                    "(timestamp 'epoch' + ts / 1000 * interval '1 second') as start_time "+
                    "from events_staging where page = 'NextSong') a")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        user_table_create, artist_table_create, song_table_create, 
                        songplay_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
