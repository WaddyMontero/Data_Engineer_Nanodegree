import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function is used to execute the queries from the drop_table_queries list.
    Args:
        cur: A db cursor object used to issue SQL operations to the analytics db.
        conn: A db connection object used to establish a connection to the analytics db.
    Returns:
        None.
    Raises:
        None.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function is used to execute the queries from the create_table_queries list.
    Args:
        cur: A db cursor object used to issue SQL operations to the analytics db.
        conn: A db connection object used to establish a connection to the analytics db.
    Returns:
        None.
    Raises:
        None.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()