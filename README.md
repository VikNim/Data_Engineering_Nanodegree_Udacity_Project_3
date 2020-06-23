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
