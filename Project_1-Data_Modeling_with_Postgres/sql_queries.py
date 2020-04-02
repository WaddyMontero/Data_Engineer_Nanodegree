# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
user_table_drop = "DROP TABLE IF EXISTS users;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create =     "CREATE TABLE IF NOT EXISTS songplays("\
                            "songplay_id SERIAL PRIMARY KEY"\
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

# INSERT RECORDS

songplay_table_insert =     ("INSERT INTO songplays(start_time, user_id, song_id"\
                            ", artist_id, session_id, location, user_agent)"\
	                        " VALUES(%s,%s,%s,%s,%s,%s,%s)")

user_table_insert =         ("INSERT INTO users(user_id, first_name, last_name,"\
                            " gender, level) VALUES(%s,%s,%s,%s,%s) ON CONFLICT"\
                            " ON CONSTRAINT users_pkey DO "\
                            " UPDATE set level = users.level;")

song_table_insert =         ("INSERT INTO songs(song_id, title, artist_id, year"\
                            ", duration) VALUES(%s,%s,%s,%s,%s) ON CONFLICT "\
                            "ON CONSTRAINT songs_pkey DO NOTHING;")

artist_table_insert =       ("INSERT INTO artists(artist_id, name, location, "\
                            "latitude, longitude) VALUES(%s,%s,%s,%s,%s) ON CONFLICT"\
                            " ON CONSTRAINT artists_pkey DO NOTHING;")

time_table_insert =         ("INSERT INTO time(start_time, hour, day, week"
                            ", month, year, weekday) VALUES(%s,%s,%s,%s,%s,%s,%s)"\
                            " ON CONFLICT ON CONSTRAINT time_pkey DO NOTHING;")

# FIND SONGS

song_select = ( "SELECT S.song_id, A.artist_id FROM"\
                " songs AS S JOIN artists AS A ON S.artist_id = A.artist_id"\
                " WHERE S.title = %s AND A.name = %s AND S.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, artist_table_create, song_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]