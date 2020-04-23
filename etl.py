import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This procedure processes a song and log files whose s3 bucket filepath has been provided as an arugment.
    It extracts the song and logs information in order to store them into the staging songs and events table.
    Then the staging tables would be used to load information to the dimensions and facts tables.

    INPUTS:
    * cur the cursor variable
    * conn the database connection variable
    '''    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This procedure processes infomation from the staging tables into dimensions and fact tables.

    INPUTS:
    * cur the cursor variable
    * conn the database connection variable
    '''    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
