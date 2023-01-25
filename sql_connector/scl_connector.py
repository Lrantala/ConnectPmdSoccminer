import logging
import sqlite3
import pandas as pd

class SqliteConnector():
    def __init__(self):
        self._db_name = None
        self._connection = None

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, value):
        self._db_name = value

    @property
    def connection(self):
        return self._connection

    def connect_to_db(self):
        if self._db_name is not None:
            logging.info("Initializing connection to db.")
            self._connection = sqlite3.connect(self.db_name)
            connection_name = (self._db_name,)
            logging.info("Connection created to db %s", connection_name)
            # return self._connection
        else:
            logging.info("No connection provided")
            # return None

    def close_connection(self):
        self._connection.close()

    def check_if_table_exists(self, value):
        mycur = self._connection.cursor()
        mycur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        available_table = (mycur.fetchall())
        self.close_connection()
        if any(value in x for x in available_table):
            return True
        else:
            return False

    def read_data_to_df(self, name_of_table):
        logging.info("Reading data to numpy array.")
        cur = self.connect_to_db().cursor()
        table_list = cur.execute("SELECT name FROM ")
        query = cur.execute("SELECT * From %s WHERE type='table';", (name_of_table,))
        if query is None:
            logging.info("No such table found")
            return None
        else:
            cols = [column[0] for column in query.description]
            results = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
            return results

    def initialize_db(self):
        logging.info("Initializing db.")
        cur = self.connect_to_db().cursor()
        # Check if the table exists before creating it
        cur.execute("CREATE TABLE if NOT EXISTS comments(comment_id integer PRIMARY KEY, "
                    "hash text, "
                    "commit_id integer, "
                    "filename text, "
                    "filepath text, "
                    "type text, "
                    "start_line int, "
                    "start_chr int, "
                    "end_line int, "
                    "end_chr int, "
                    "comment text)")
        self._connection.commit()
        cur.execute("CREATE TABLE if NOT EXISTS commits(commit_id integer PRIMARY KEY, "
                    "hash text, "
                    "commit_msg text)")
        self._connection.commit()
        cur.execute("CREATE TABLE if NOT EXISTS analyses(analysis_id integer PRIMARY KEY AUTOINCREMENT, "
                    "commit_id int, "
                    "file text, "
                    "region text, "
                    "region_type text, "
                    "modified text, "
                    "line_start int, "
                    "line_end int, "
                    "code_complexity_cyclomatic int, "
                    "code_complexity_maxindent int, "
                    "filelines_code int, "
                    "filelines_comments int, "
                    "filelines_total int, "
                    "codelines_code int, "
                    "codelines_comments int, "
                    "codelines_total int, "
                    "codelines_todo_comments int, "
                    "general_procerrors int)")
        self._connection.commit()
        self.close_connection()

    def write_one_to_db(self, values):
        logging.info("Writing one to db %s", (self._db_name,))
        cur = self.connect_to_db().cursor()
        cur.execute("INSERT into comments VALUES (?,?,?,?,?,?,?,?,?,?)", values)
        self._connection.commit()
        self.close_connection()

    def write_many_comments_to_db(self, values):
        logging.info("Writing many to db %s", (self._db_name,))
        cur = self.connect_to_db().cursor()
        cur.executemany("INSERT into comments VALUES (?,?,?,?,?,?,?,?,?,?,?)", values)
        self._connection.commit()
        self.close_connection()

    def write_many_commits_to_db(self, values):
        logging.info("Writing many to db %s", (self._db_name,))
        cur = self.connect_to_db().cursor()
        cur.executemany("INSERT into commits VALUES (?,?,?)", values)
        self._connection.commit()
        self.close_connection()

    def write_many_mpplus_to_db(self, values):
        logging.info("Writing many to db %s", (self._db_name,))
        cur = self.connect_to_db().cursor()
        cur.executemany("INSERT into analyses(commit_id, file, region, region_type, modified,"
                        "line_start, line_end, code_complexity_cyclomatic, code_complexity_maxindent,"
                        "filelines_code, filelines_comments, filelines_total, codelines_code,"
                        "codelines_comments, codelines_total, codelines_todo_comments,"
                        "general_procerrors) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)
        self._connection.commit()
        self.close_connection()
