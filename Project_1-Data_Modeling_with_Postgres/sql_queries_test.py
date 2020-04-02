songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

songplay_table_create =     "CREATE TABLE IF NOT EXISTS fact_songplays("\
                            "songplay_id SERIAL NOT NULL"\
                            ",start_time TIMESTAMP NOT NULL"\
                            ",user_id INT NOT NULL"\
                            ",song_id INT NOT NULL"\
                            ",artist_id INT NOT NULL"\
                            ",session_id INT NOT NULL"\
                            ",location VARCHAR(65)"\
                            ",user_agent VARCHAR(250) NOT NULL);"

artist_table_create =       "CREATE TABLE IF NOT EXISTS dim_artists("\
                            "artist_id VARCHAR(18) NOT NULL"\
                            ",name VARCHAR(100)"\
                            ",location VARCHAR(30)"\
                            ",latitude NUMERIC(8,5)"\
                            ",longitude NUMERIC(8,5));"

song_table_create =         "CREATE TABLE IF NOT EXISTS dim_songs("\
                            "song_id VARCHAR(18) NOT NULL"\
                            ",title VARCHAR(200)"\
                            ",artist_id VARCHAR(18) NOT NULL"\
                            ",year INT" \
                            ",duration NUMERIC(8,5));"

user_table_create =         "CREATE TABLE IF NOT EXISTS dim_users("\
                            "user_id INT NOT NULL"\
                            ",first_name VARCHAR(20)"\
                            ",last_name VARCHAR(20)"\
                            ",gender CHAR(1)"\
                            ",level char(4) NOT NULL);"

time_table_create =         "CREATE TABLE IF NOT EXISTS dim_time("
                            "start_time TIMESTAMP"
                            ",hour INT"
                            ",day INT"
                            ",week INT"
                            ",month INT"
                            ",year INT"
                            ",weekday INT);"

