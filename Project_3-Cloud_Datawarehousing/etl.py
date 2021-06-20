import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function is used to execute the queries from the copy_table_queries list.
    Args:
        cur: A db cursor object used to issue SQL operations to the analytics db.
        conn: A db connection object used to establish a connection to the analytics db.
    Returns:
        None.
    Raises:
        None.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function is used to execute the queries from the insert_table_queries list.
    Args:
        cur: A db cursor object used to issue SQL operations to the analytics db.
        conn: A db connection object used to establish a connection to the analytics db.
    Returns:
        None.
    Raises:
        None.
    """
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