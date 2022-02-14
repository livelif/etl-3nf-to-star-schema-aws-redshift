import configparser


# Config
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
SONGS_JSONPATH  = config.get('S3', 'SONGS_JSONPATH')

# Drop tables

raw_log_data_drop = "DROP TABLE IF EXISTS raw_log_data"
raw_song_data_drop = "DROP TABLE IF EXISTS raw_song_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# Create tables

# Bronze layer
raw_log_data_create = ("""
    CREATE TABLE IF NOT EXISTS raw_log_data (
                event_id BIGINT IDENTITY(0,1) NOT NULL,
                artist VARCHAR NULL,
                auth VARCHAR NULL,
                firstName VARCHAR NULL,
                gender VARCHAR NULL,
                itemInSession VARCHAR NULL,
                lastName VARCHAR NULL,
                length VARCHAR NULL,
                level VARCHAR NULL,
                location VARCHAR NULL,
                method VARCHAR NULL,
                page VARCHAR NULL,
                registration VARCHAR NULL,
                sessionId INTEGER NOT NULL SORTKEY DISTKEY,
                song VARCHAR NULL,
                status INTEGER NULL,
                ts BIGINT NOT NULL,
                userAgent VARCHAR NULL,
                userId INTEGER NULL
    );
""")

raw_song_data_create = ("""
    CREATE TABLE IF NOT EXISTS raw_song_data (
        num_songs INTEGER NULL,
        artist_id VARCHAR NOT NULL,
        artist_latitude VARCHAR NULL,
        artist_longitude VARCHAR NULL,
        artist_location VARCHAR(500) NULL,
        artist_name VARCHAR(500) NULL,
        song_id VARCHAR NOT NULL,
        title VARCHAR(500) NULL,
        duration DECIMAL(9) NULL,
        year INTEGER NULL
    );
""")

# silver layer

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) NOT NULL,
        start_time  TIMESTAMP NOT NULL,
        user_id VARCHAR(50) NOT NULL,
        level VARCHAR(10) NOT NULL,
        song_id VARCHAR(40) NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        session_id VARCHAR(50) NOT NULL,
        location VARCHAR(100) NULL,
        user_agent VARCHAR(255) NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL,
        first_name VARCHAR(50) NULL,
        last_name VARCHAR(80) NULL,
        gender VARCHAR(10) NULL,
        level VARCHAR(10) NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(50) NOT NULL SORTKEY,
        title VARCHAR(500) NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        year INTEGER NOT NULL,
        duration DECIMAL(9) NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(50) NOT NULL SORTKEY,
        name VARCHAR(500) NULL,
        location VARCHAR(500) NULL,
        latitude DECIMAL(9) NULL,
        longitude DECIMAL(9) NULL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL SORTKEY,
        hour INTEGER NULL,
        day INTEGER NULL,
        week INTEGER NULL,
        month INTEGER NULL,
        year INTEGER NULL,
        weekday INTEGER NULL
    );
""")

# Raw copy

raw_log_data_copy = ("""
    COPY raw_log_data FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

raw_song_data_copy = ("""
    COPY raw_song_data FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# TABLES

inserts = {
    "songplays": 
        """
            INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
            SELECT  DISTINCT 
                    TIMESTAMP 'epoch' + log.ts/1000 * INTERVAL '1 second' AS start_time,
                    log.userId AS user_id,
                    log.level AS level,
                    song.song_id AS song_id,
                    song.artist_id AS artist_id,
                    log.sessionId AS session_id,
                    log.location  AS location,
                    log.userAgent AS user_agent
            FROM raw_log_data log
            JOIN raw_song_data song
                ON (log.artist = song.artist_name)
            WHERE log.page = 'NextSong';
        """,
    "users": 
        """
            INSERT INTO users
            SELECT  DISTINCT 
                    log.userId AS user_id,
                    log.firstName AS first_name,
                    log.lastName AS last_name,
                    log.gender AS gender,
                    log.level AS level
            FROM raw_log_data log
            WHERE log.page = 'NextSong';
        """,
    "songs": 
        """
            INSERT INTO songs
            SELECT  DISTINCT 
                    song.song_id AS song_id,
                    song.title AS title,
                    song.artist_id AS artist_id,
                    song.year AS year,
                    song.duration AS duration
            FROM raw_song_data song;
        """,
    "artists":
        """
            INSERT INTO artists
            SELECT  DISTINCT 
                    song.artist_id AS artist_id,
                    song.artist_name AS name,
                    song.artist_location AS location,
                    song.artist_latitude AS latitude,
                    song.artist_longitude AS longitude
            FROM raw_song_data song;
        """,
    "time":
        """
            INSERT INTO time
            SELECT  DISTINCT 
                    TIMESTAMP 'epoch' + log.ts/1000 * INTERVAL '1 second' AS start_time,
                    EXTRACT(hour FROM start_time) AS hour,
                    EXTRACT(day FROM start_time) AS day,
                    EXTRACT(week FROM start_time) AS week,
                    EXTRACT(month FROM start_time) AS month,
                    EXTRACT(year FROM start_time) AS year,
                    EXTRACT(week FROM start_time) AS weekday
            FROM    raw_log_data log
            WHERE log.page = 'NextSong';
        """
    
}

create_table_queries = [raw_log_data_create, raw_song_data_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [raw_log_data_drop, raw_song_data_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [raw_log_data_copy, raw_song_data_copy]