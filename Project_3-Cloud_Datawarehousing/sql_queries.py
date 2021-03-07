import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
user_table_drop = "DROP TABLE IF EXISTS users;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create="CREATE TABLE IF NOT EXISTS staging_events("\
                            "numsongs INT "\
                            ",artist_id VARCHAR(18)"\
                            ",artist_latitude NUMERIC(8,5)"\
                            ",artist_longitude NUMERIC(8,5)"\
                            ",artist_location VARCHAR(30)"\
                            ",artist_name VARCHAR(100)"\
                            ",song_id VARCHAR(18)"\
                            ",title VARCHAR(200)"\
                            ",duration NUMERIC(8,5)"\
                            ",year INT);"

staging_songs_table_create ="CREATE TABLE IF NOT EXISTS staging_songs("\
                            "artist VARCHAR(100)"\
                            ",auth VARCHAR(30)"\
                            ",first_name VARCHAR(20)"\
                            ",gender CHAR(1)"\
                            ",item_in_session INT"\
                            ",last_name VARCHAR(20)"\
                            ",length NUMERIC(8,5)"\
                            ",level CHAR(4)"\
                            ",location VARCHAR(30)"\
                            ",method VARCHAR(10)"\
                            ",page VARCHAR(30)"\
                            ",registration BIGINT"\
                            ",session_id INT"\
                            ",song VARCHAR(200)"\
                            ",status INT"\
                            ",ts BIGINT"\
                            ",user_agent VARCHAR(200)"\
                            ",user_id INT);"

songplay_table_create =     "CREATE TABLE IF NOT EXISTS songplays("\
                            "songplay_id INT IDENTITY(0,1) PRIMARY KEY"\
                            ",start_time TIMESTAMP NOT NULL"\
                            ",user_id INT NOT NULL"\
                            ",song_id VARCHAR(18)"\
                            ",artist_id VARCHAR(18)"\
                            ",session_id INT"\
                            ",location VARCHAR(65)"\
                            ",user_agent VARCHAR(250));"

artist_table_create =       "CREATE TABLE IF NOT EXISTS artists("\
                            "artist_id VARCHAR(18) PRIMARY KEY"\
                            ",name VARCHAR(100) NOT NULL"\
                            ",location VARCHAR(30)"\
                            ",latitude NUMERIC(8,5)"\
                            ",longitude NUMERIC(8,5));"

song_table_create =         "CREATE TABLE IF NOT EXISTS songs("\
                            "song_id VARCHAR(18) PRIMARY KEY"\
                            ",title VARCHAR(200)"\
                            ",artist_id VARCHAR(18)"\
                            ",year INT" \
                            ",duration NUMERIC(8,5)"\
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
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, artist_table_create, song_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
