import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables from redshift.
    Arguments:
    * cur --   Cursor to connected DB. Allows to execute SQL commands.
    * conn --  Connection to Postgres database.
    """
    print("Dropping tables ", drop_table_queries)
    for query in drop_table_queries:
        print("Droping: ", query)
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("ERROR: ", e)

    print("Tables dropped!!")

def create_tables(cur, conn):
    """Create new tables.
    Arguments:
    * cur --    Cursor to connected DB. Allows to execute SQL commands.
    * conn --   Connection to Postgres database.
    """
    print("Creating tables")
    for query in create_table_queries:
        try:
            print("Creating: ", query)
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("ERROR ", e)

    print("Tables created.")

def main():
    """Connect to AWS Redshift,drop any existing tables, create new tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_DB= config.get("CLUSTER","DB_NAME")
    DWH_DB_USER= config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DB_PORT")

    DWH_ENDPOINT = config.get("AWS","ENDPOINT")

    print("Connecting...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD,DWH_PORT))
    
    print("Connected")
    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()