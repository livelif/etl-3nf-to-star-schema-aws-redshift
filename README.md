# Data Warehouse With AWS

## Datasets
There are two datasets being used in this project. The first is song metadata stored in data/song_data.
The other dataset is log data stored in data/log_data. 

## Project Template
The project template includes four files:

create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

## Schema 
### Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. users - users in the app
2. user_id, first_name, last_name, gender, level
3. songs - songs in music database song_id, title, artist_id, year, duration
4. artists - artists in music database artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

![Star Schema](/Song_ERD.png "")

### How to run this project:
Run python create_tables.py
Run python etl.py

## ETL Process
The steps:
1. Get the data from data/log_data and data/song_data
2. Get the most important values and create a pandas dataframe
3. Make some transformations
4. Execute SQL queries to insert the data in a postgres database


