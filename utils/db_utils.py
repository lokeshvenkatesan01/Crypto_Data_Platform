import psycopg2
from airflow.hooks.base import BaseHook
from utils.db_utils import get_pg_conn


def get_pg_conn():

    conn = BaseHook.get_connection("postgres_warehouse")

    return psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
    )