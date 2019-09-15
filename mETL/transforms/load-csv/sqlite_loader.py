import os
import sqlite3

from loader import SqlBaseLoader
from core_sql import Sql

class SqliteSql(Sql):
    @classmethod
    def table_exists(cls, table_name):
        return "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{}';".format(table_name)

    @classmethod
    def escape_table_name(cls, table_name):
        return table_name

class SqliteLoader(SqlBaseLoader):
    def connect(self, database):
        database = os.path.abspath(database)
        assert not os.path.isdir(database), 'The database argument cannot be a directory'
        dirname = os.path.dirname(database)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return sqlite3.connect(database)

    @property
    def sql(self):
        return SqliteSql

    def cursor(self):
        conn = self._conn
        class Cursor(object):
            def __enter__(self):
                self.__cursor = conn.cursor()
                return self.__cursor

            def __exit__(self, exc_type, exc_value, traceback):
                self.__cursor.close()

        return Cursor()
