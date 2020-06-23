import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        - Executing queries to drop all the tables just to create them again in database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        - Executing queries to create all tables in database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        - Fetching connection details from configuration file.
        - Establishing connection with database.
        - Invoking functions to empty the database and to create tables in database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    """
        - Initializing the situation.
    """
    main()