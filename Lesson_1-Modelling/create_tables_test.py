import pyodbc as SQL
import pandas as pd
import os
import json
from sys import *


def createDatabase(dbName):
    
    sqlConnectionString =   "Driver={SQL Server};"\
                            "Server=DRLTBOWF-3825;"\
                            "Initial Catalog=master;"\
                            "Trusted_Connection=yes"

    _conn = None
    try:
        _conn = SQL.connect(sqlConnectionString)
        _conn.autocommit = True
        print("Successful Connection")

        sqlCreateDatabase = f"IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = '{dbName}') DROP DATABASE {dbName} CREATE DATABASE {dbName}"

        _conn.execute(sqlCreateDatabase)

    except SQL.Error as e:
        print("Connection failed!\n")
        print(e)

def dropDatabase(dbName):


    sqlConnectionString =   "Driver={SQL Server};"\
                            "Server=DRLTBOWF-3825;"\
                            "Initial Catalog=master;"\
                            "Trusted_Connection=yes"

    yesConfirmation = {'yes', 'y', 'Y', 'YES'}

    _conn = None
    try:
        _conn = SQL.connect(sqlConnectionString)
        _conn.autocommit = True
        print("Successful Connection")
        _db = _conn.execute(f"SELECT 1 FROM sys.databases WHERE [name] = '{dbName}'")
        sqlDropDatabase = f"IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = '{dbName}') DROP DATABASE {dbName}"

        if (_db.fetchone()):
            print(f"You're about to drop the {dbName} database. Are you sure about it? [Y/N]")
            choice = input().lower()
            if (choice in yesConfirmation):
                _conn.execute(sqlDropDatabase)
                print(f"Database: {dbName} was successfully dropped!")
            else:
                print(f"Database: {dbName} was not dropped!")
        else:
            print(f"The {dbName} database doesn't exist!")

    except SQL.Error as e:
        print("Connection failed!\n")
        print(e)

def loadData(path):
    dataFiles = []


    for (dirpath, dirnames, filenames) in os.walk(path):
        for file in filenames:
            dataFiles.append(os.path.join(dirpath,file))   

    data = []
    for file in dataFiles:
        for line in open(file, 'r'):
            data.append(json.loads(line))

    df = pd.io.json.json_normalize(data)

    return df

newDf = loadData("./data/song_data")

newDf.head(5)
