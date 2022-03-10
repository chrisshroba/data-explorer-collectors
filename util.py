import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

load_dotenv()


def create_db_conn():

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", None),
        port=os.getenv("PGPORT", None),
        user=os.getenv("PGUSER", None),
        password=os.getenv("PGPASSWORD", None),
        database=os.getenv("PGDATABASE", None),
    )

    return conn


def get_db_conn():
    conn = create_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    return cur


def query(cur, q, *args):
    cur.execute(q, *args)
    info = {}
    try:
        results = cur.fetchall()
        info["colnames"] = [desc.name for desc in cur.description]
    except psycopg2.ProgrammingError as e:
        if e.args[0] == "no results to fetch":
            info["colnames"] = []
        else:
            raise
    info["statusmesage"] = cur.statusmessage
    info["rowcount"] = cur.rowcount

    return results, info


def insert(cur, sql, argslist, *args):
    # https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_values
    return psycopg2.extras.execute_values(cur, sql, argslist, *args)
