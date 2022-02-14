import configparser
import psycopg2
from sql_queries import copy_table_queries, inserts


def load_staging_tables(cur, conn):
    """Load JSON input data from S3 and insert into staging tables.
    Arguments:
        * cur --    Cursor to connected DB. Allows to execute SQL commands.
        * conn --   Connection to Postgres database.
    """
    print("Start loading data")

    for query in copy_table_queries:
        try:
            print('Copying ', query)
            cur.execute(query)
            conn.commit()
            print('Query processed.')
        except psycopg2.OperationalError as err:
            print("ERROR: ", err)




def insert_tables(cur, conn):
    """Insert data from staging tables (staging_events and staging_songs) into bronze layer (delta lake):
    Arguments:
        * cur --    Cursor to connected DB. Allows to execute SQL commands.
        * conn --   Connection to Postgres database.
    """
    print("Start inserting data from staging tables into analysis tables...")
    for key in inserts.keys():
        print("Insert into: ", key)
        sql_query = inserts[key] 
        print("QUERY: ", sql_query)
        try:
            cur.execute(sql_query)
            conn.commit()
            print('Processed')
        except psycopg2.OperationalError as err:
            print("ERROR: ", err)
            
    print('All Inserts Ok')

def main():
    """
        * Load staging tables from S3
        * Ensert data using staging tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_DB= config.get("CLUSTER","DB_NAME")
    DWH_DB_USER= config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DB_PORT")

    DWH_ENDPOINT = config.get("AWS","ENDPOINT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD,DWH_PORT))
    
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()