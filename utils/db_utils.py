import logging
import psycopg2
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def get_pg_conn():
    """
    Create PostgreSQL connection using Airflow connection
    """

    conn = BaseHook.get_connection("postgres_warehouse")

    return psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
    )


def execute_query(sql, params=None):
    """
    Execute a SQL query
    """

    conn = get_pg_conn()
    cur = conn.cursor()

    try:
        logger.info("Executing query")

        cur.execute(sql, params)

        conn.commit()

    except Exception as e:
        logger.error(f"Database query failed: {e}")
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


def fetch_query(sql, params=None):
    """
    Execute SELECT query and return results
    """

    conn = get_pg_conn()
    cur = conn.cursor()

    try:
        cur.execute(sql, params)
        result = cur.fetchall()
        return result

    finally:
        cur.close()
        conn.close()
        
def create_tables():

    logger.info("Creating warehouse tables")

    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_dimension (
            coin_id TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            category TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_prices_fact (
            id SERIAL PRIMARY KEY,
            coin_id TEXT,
            price_usd NUMERIC,
            market_cap NUMERIC,
            timestamp TIMESTAMP
        );
    """)

    conn.commit()

    cur.close()
    conn.close()

    logger.info("Tables verified")        