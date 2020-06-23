The purpose of this project is to implement a basic, simple ETL process for SPRKLIFLY. We are implementing a star schema database using Amazon Redshift database and Python 3.

What is SPARKIFY: Sparkify is a fictional music streaming platform created by Udacity.

Project Files: 
i) create_tables: This files makes the database setup by creating sparklifly database schema, creating tables in database schema and setting necessary constraint for each table.

ii) etl.py: This file is the backbone of project.We are extracting data from files, transforming the values and loading the values in database with use of this file.

iii) sql_queries: This files provides sql querie for all the operations such as create table, insert a row etc.

iv) dwh.cfg: This file contains following records:
    
    [CLUSTER]
    HOST=CLUSTER_ENDPOINT
    DB_NAME=DATABASE_NAME
    DB_USER=DATABASE_USER
    DB_PASSWORD=DATABASE_USER_PASSWORD
    DB_PORT=5439

    [IAM_ROLE]
    ARN=USER_ARN_F0R_CLUSTER

    # Data for this project
    [S3]
    LOG_DATA='s3://udacity-dend/log_data'
    LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
    SONG_DATA='s3://udacity-dend/song_data'
    
Database Design: 
<br>
<img src="https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/33760/1591881849/Song_ERD.png" />
<br>

ETL Process:
Extraction:     Extracting data from song_files and log_files which are availabel at given S3 Bucket address.
Transformation: Transforming unix time data into readable format.
Load:           Loading the data into Redshift Database.

In this project we are initially fetching data from S3 buckets into staging tables using Redshift COPY command which will allow us to data in BULK manner,
then we will load the data into final i.e data base tables.

To run this project: 
i)   Setup a cluser at Amazon Redshift, get the cluster details and put them into dwh.cfg file. 
ii)  Run create_tables.py.
iii) Run etl.py.

