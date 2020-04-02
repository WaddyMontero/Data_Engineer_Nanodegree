# Introduction - Modelling data with Postgres.


A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Description

The scripts that compose this project create a database schemafor the required song data analysis and establishes an ETL pipeline for the continued population of the database schema as the analytics team see fit.

The project is composed of three scripts:

1. sql_queries.py

    * Contains the helper queries used to create, drop, populate and query the tables that 
    support the ETL process.

2. create_tables.py

    * Base script that creates the Sparkify database and its base schema.

3. ETL.py

    * This script is the one in charge of extracting the json data located on the Data folder
    and appropiately transform and load said data based on the business rules established for
    the project.

The execution order of the scripts should be:

    1- create_tables.py = Lays out the foundation of the schema, and makes sure the sparkify database is clean.
    2- etl.py = Processes the data located on the data folder.
    

DB_Schema

![Sparkify Schema](/Sparkify_ERD.png)
