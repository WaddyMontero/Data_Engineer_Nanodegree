import os
import json
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function process the song json files on the Data folder.

    Args:
        cur: A postgres cursor used to interact with the Sparkify db.
        filepath: This parameter represents the absolute path to the json file being processed.

    Returns:
        None.

    Raises:
        None.
    """
    # open song file
      
    for json_file in open(filepath, 'r'):
       df = pd.json_normalize(json.loads(json_file))

     # insert artist record
    artistCols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artistCols].values.tolist()
    artist_data = [item for listx in artist_data for item in listx]

    cur.execute(artist_table_insert, artist_data)

    # insert song record
    songCols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[songCols].values.tolist()
    song_data = [item for listx in song_data for item in listx]

    cur.execute(song_table_insert, song_data)
    
def process_log_file(cur, filepath):

    """
    This function process the log json files on the Data folder.

    Args:
        cur: A postgres cursor used to interact with the Sparkify db.
        filepath: This parameter represents the absolute path to the json file being processed.

    Returns:
        None.

    Raises:
        None.
    """
    df = pd.DataFrame()
    # open log file
    for json_file in open(filepath, 'r'):
        data = pd.read_json(json_file, lines=True)
        df = df.append(data, ignore_index=True)

    # filter by NextSong action
    filt = df.loc[:,'page'] == 'NextSong'
    df = df[filt]
    df.loc[:,'ts'] = pd.to_datetime(df.loc[:,'ts'], unit='ms')
    # convert timestamp column to datetime

    t = df.loc[:,'ts'].to_frame()
    t['hour'] = t['ts'].dt.hour
    t['day'] = t['ts'].dt.hour
    t['week'] = t['ts'].dt.week
    t['month'] = t['ts'].dt.month
    t['year'] = t['ts'].dt.year
    t['weekday'] = t['ts'].dt.weekday
    
    # insert time data records
    time_data = t.itertuples(index=False, name=None)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


    # load user table
    user_cols = ['user_id', 'first_name', 'last_name', 'gender', 'level']

    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    user_df.columns = user_cols
    user_df.replace('', np.nan, inplace=True)
    user_df.dropna(subset=['user_id'], inplace=True)

    # insert user records

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    df_len = len(df)
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (str(row.song), str(row.artist), row.length))
        results = cur.fetchone()
        if results != None:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # insert songplay record
        songplay_data = (row.ts, row.userId, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):

    """
    Generic function which handles the execution of each part of the ETL process (song data and log data).

    Args:
        cur: A postgres cursor used to interact with the Sparkify db.
        conn: A connection object used to commit the changes to the Sparkify database.
        filepath: This parameter represents the absolute path to the json file being processed.
        func: This parameter is used to determine which function will be used to process the json data. (log/song)

    Returns:
        None.

    Raises:
        None.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        try:
            func(cur, datafile)
            conn.commit()
        except psycopg2.IntegrityError as e:
            conn.rollback()
        else:
            conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()