import psycopg2
import psycopg2.extras
import os
import dotenv

dotenv.load_dotenv()


def create_db_conn():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", None),
        port=os.getenv("PGPORT", None),
        user=os.getenv("PGUSER", None),
        password=os.getenv("PGPASSWORD", None),
        database=os.getenv("PGDATABASE", None),
    )
    return conn


def get_cursor_for_conn(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)


class QueryResults:
    def __init__(self, rows, colnames, statusmessage, rowcount):
        self.rows = rows
        self.colnames = colnames
        self.statusmessage = statusmessage
        self.rowcount = rowcount

    def __repr__(self):
        return f"QueryResults {self.__dict__}"


def query(cur, q, *args):
    cur.execute(q, *args)

    rows = None
    try:
        rows = cur.fetchall()
        colnames = [desc.name for desc in cur.description]
    except psycopg2.ProgrammingError as e:
        if e.args[0] == "no results to fetch":
            colnames = []
        else:
            raise
    statusmessage = cur.statusmessage
    rowcount = cur.rowcount

    results = QueryResults(
        rows=rows,
        colnames=colnames,
        statusmessage=statusmessage,
        rowcount=rowcount,
    )

    return results


def insert_many(cur, sql, argslist, *args):
    # https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_values
    return psycopg2.extras.execute_values(cur, sql, argslist, *args)


class MultiRowInsertion:
    def __init__(self, insert_query, row_tuples, table_name):
        self.insert_query = insert_query
        self.row_tuples = row_tuples
        self.table_name = table_name

    def execute(self, cur):
        return psycopg2.extras.execute_values(
            cur, self.insert_query, self.row_tuples
        )


class Collector:
    def __init__(self):
        self._logs = []

    def get_conn(self):
        return create_db_conn()

    def get_cursor_for_conn(self, conn):
        return get_cursor_for_conn(conn)

    def log(self, s):
        print(s)
        self._logs.append(s)

    def get_logs_as_string(self):
        return "\n".join(self._logs)

    def get_version(self):
        raise NotImplementedError()

    def get_collector_name(self):
        raise NotImplementedError()

    def get_multirow_insertions(self):
        raise NotImplementedError()

    def run(self):
        # First, create new run
        with create_db_conn() as conn, get_cursor_for_conn(conn) as cur:
            results = query(
                cur,
                self.INSERT_RUN_SQL,
                (self.get_collector_name(), self.get_version()),
            )

            run_id = results.rows[0].id

        # Next, compute the rows to insert, and insert them.
        error_str = None
        insertions_metadata = []
        try:
            multirow_insertions = self.get_multirow_insertions()
            with create_db_conn() as conn, get_cursor_for_conn(conn) as cur:
                for multirow_insertion in multirow_insertions:
                    self.log(
                        "Trying to insert "
                        + f"{len(multirow_insertion.row_tuples)} rows to "
                        + f"{multirow_insertion.table_name}."
                    )

                    rows_inserted = 0
                    if len(multirow_insertion.row_tuples) > 0:
                        multirow_insertion.execute(cur)
                        rows_inserted = cur.rowcount
                    insertions_metadata.append(
                        (multirow_insertion.table_name, rows_inserted)
                    )
                    self.log(
                        f"Table {multirow_insertion.table_name}: "
                        + f"{rows_inserted} rows inserted."
                    )
        # If anything goes wrong, record the error.
        except Exception as e:
            print(e)
            with create_db_conn() as conn, get_cursor_for_conn(conn) as cur:
                import traceback

                traceback_str = "".join(traceback.format_tb(e.__traceback__))
                error_str = f"{str(e)}\n\n{traceback_str}"
                print(error_str)
        with create_db_conn() as conn, get_cursor_for_conn(conn) as cur:
            cur.execute(
                self.INSERT_RUN_RESULTS_SQL,
                (run_id, self.get_logs_as_string(), error_str),
            )
            rows_added_rows_to_add = [
                (run_id, table_name, num_rows_added)
                for table_name, num_rows_added in insertions_metadata
            ]
            psycopg2.extras.execute_values(
                cur, self.INSERT_RUN_ROWS_ADDED_SQL, rows_added_rows_to_add
            )

    INSERT_RUN_SQL = """
        INSERT INTO collectors.runs
          (collector_name, collector_version, start_time)
        VALUES
          (%s, %s, NOW())
        RETURNING
          id;
    """

    INSERT_RUN_RESULTS_SQL = """
        INSERT INTO collectors.run_results
          (run_id, logs, errors, end_time)
        VALUES
          (%s, %s, %s, NOW());
    """

    INSERT_RUN_ROWS_ADDED_SQL = """
        INSERT INTO collectors.run_rows_added
          (run_id, table_name, num_rows_added)
        VALUES
          %s;
    """
