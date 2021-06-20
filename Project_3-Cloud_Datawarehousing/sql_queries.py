import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
user_table_drop = "DROP TABLE IF EXISTS users;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_songs_table_create="CREATE TABLE IF NOT EXISTS staging_songs("\
                            "numsongs INT "\
                            ",artist_id VARCHAR(1000)"\
                            ",artist_latitude VARCHAR(100)"\
                            ",artist_longitude VARCHAR(1000)"\
                            ",artist_location VARCHAR(1000)"\
                            ",artist_name VARCHAR(1000)"\
                            ",song_id VARCHAR(1000)"\
                            ",title VARCHAR(1000)"\
                            ",duration VARCHAR(1000)"\
                            ",year INT);"

staging_events_table_create ="CREATE TABLE IF NOT EXISTS staging_events("\
                            "event_id BIGINT IDENTITY(0,1)"\
                            ",artist VARCHAR(1000)"\
                            ",auth VARCHAR(1000)"\
                            ",first_name VARCHAR(1000)"\
                            ",gender VARCHAR(1000)"\
                            ",item_in_session INT"\
                            ",last_name VARCHAR(1000)"\
                            ",length VARCHAR(1000)"\
                            ",level VARCHAR(1000)"\
                            ",location VARCHAR(1000)"\
                            ",method VARCHAR(1000)"\
                            ",page VARCHAR(1000)"\
                            ",registration VARCHAR(1000)"\
                            ",session_id INT"\
                            ",song VARCHAR(1000)"\
                            ",status INT"\
                            ",ts BIGINT"\
                            ",user_agent VARCHAR(1000)"\
                            ",user_id INT);"

songplay_table_create =     "CREATE TABLE IF NOT EXISTS songplays("\
                            "songplay_id INT IDENTITY(0,1) PRIMARY KEY"\
                            ",start_time TIMESTAMP NOT NULL"\
                            ",user_id INT NOT NULL"\
                            ",level VARCHAR(10)"\
                            ",song_id VARCHAR(18)"\
                            ",artist_id VARCHAR(18)"\
                            ",session_id INT"\
                            ",location VARCHAR(65)"\
                            ",user_agent VARCHAR(250));"

artist_table_create =       "CREATE TABLE IF NOT EXISTS artists("\
                            "artist_id VARCHAR(18) PRIMARY KEY"\
                            ",name VARCHAR(200) NOT NULL"\
                            ",location VARCHAR(200)"\
                            ",latitude NUMERIC(10,5)"\
                            ",longitude NUMERIC(10,5));"

song_table_create =         "CREATE TABLE IF NOT EXISTS songs("\
                            "song_id VARCHAR(18) PRIMARY KEY"\
                            ",title VARCHAR(200)"\
                            ",artist_id VARCHAR(18)"\
                            ",year INT" \
                            ",duration NUMERIC(10,5)"\
                            ",FOREIGN KEY (artist_id) REFERENCES artists(artist_id));"

user_table_create =         "CREATE TABLE IF NOT EXISTS users("\
                            "user_id INT PRIMARY KEY"\
                            ",first_name VARCHAR(20)"\
                            ",last_name VARCHAR(20)"\
                            ",gender CHAR(1)"\
                            ",level char(4) NOT NULL);"

time_table_create =         "CREATE TABLE IF NOT EXISTS time("\
                            "start_time TIMESTAMP PRIMARY KEY"\
                            ",hour INT"\
                            ",day INT"\
                            ",week INT"\
                            ",month INT"\
                            ",year INT"\
                            ",weekday INT);"

# STAGING TABLES

staging_events_copy = ("""
                            COPY staging_events FROM {}
                            credentials 'aws_iam_role={}'
                            format as json {}
                            STATUPDATE ON
                            region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
                            COPY staging_songs FROM {}
                            credentials 'aws_iam_role={}'
                            format as json 'auto'
                            ACCEPTINVCHARS AS '^'
                            STATUPDATE ON
                            region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays (             
                                start_time
                                ,user_id
                                ,level
                                ,song_id
                                ,artist_id
                                ,session_id
                                ,location
                                ,user_agent)
                            SELECT  
                                DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'
                                ,se.user_Id
                                ,se.level
                                ,ss.song_id
                                ,ss.artist_id
                                ,se.session_Id
                                ,se.location
                                ,se.user_Agent
                            FROM staging_events AS se
                            JOIN staging_songs AS ss
                                ON (se.artist = ss.artist_name)
                            WHERE 
                                se.page = 'NextSong';
""")

user_table_insert = ("""
                            INSERT INTO users ( 
                                user_id
                                ,first_name
                                ,last_name
                                ,gender
                                ,level
                                            )
                            SELECT  DISTINCT 
                                se.user_Id
                                ,se.first_Name
                                ,se.last_Name
                                ,se.gender
                                ,se.level
                            FROM staging_events AS se
                            WHERE 
                                se.page = 'NextSong';
""")

song_table_insert = ("""
                            INSERT INTO songs (
                                song_id
                                ,title
                                ,artist_id
                                ,year
                                ,duration
                                            )
                            SELECT  DISTINCT 
                                ss.song_id
                                ,ss.title
                                ,ss.artist_id
                                ,ss.year
                                ,CAST(ss.duration AS DECIMAL(10,5))
                            FROM staging_songs AS ss;
""")

artist_table_insert = ("""
                            INSERT INTO artists (
                                artist_id
                                ,name
                                ,location
                                ,latitude
                                ,longitude
                                                )
                            SELECT  DISTINCT 
                                ss.artist_id
                                ,ss.artist_name
                                ,ss.artist_location
                                ,ss.artist_latitude
                                ,ss.artist_longitude
                            FROM staging_songs AS ss;
""")

time_table_insert = ("""
                            INSERT INTO time ( 
                                start_time
                                ,hour
                                ,day
                                ,week
                                ,month
                                ,year
                                ,weekday    )
                            SELECT  DISTINCT 
                                TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time
                                ,EXTRACT(hour FROM start_time)
                                ,EXTRACT(day FROM start_time)
                                ,EXTRACT(week FROM start_time)
                                ,EXTRACT(month FROM start_time)
                                ,EXTRACT(year FROM start_time)
                                ,EXTRACT(week FROM start_time)
                            FROM    staging_events AS se
                            WHERE 
                                se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, artist_table_create, song_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
