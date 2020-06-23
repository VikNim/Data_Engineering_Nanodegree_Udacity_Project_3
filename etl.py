import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        - Takes cursor and connection as parameter.
        - Executes a loop to insert a records in tables.
        - The data is being copied in staging tables from s3 bucket 
        - and then inserted into database tables from staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        - Takes cursor and connection as parameter.
        - Executes a loop to insert data into final tables from staging tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        - Establishes connection to the Redshift cluster and database.
        - Connects to Amazon Redshift's database.
        - Invokes method to load the data in database.
        - Terminated the connection to database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()