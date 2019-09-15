from abc import ABC

class SqlBaseLoader(ABC):
    def __init__(self, **kwargs):
        self.__kwargs = kwargs

    def __enter__(self):
        self._conn = self.connect(**self.__kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def connect(self, **kwargs):
        raise NotImplementedError()

    def cursor(self):
        return self._conn.cursor()

    @property
    def sql(self):
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self._conn.commit()
        self._conn.close()

    def table_exists(self, table_name):
        sql = self.sql.table_exists(table_name)
        with self.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            return result and result[0]

    def drop_table(self, table_name):
        sql = self.sql.drop_table(table_name)
        with self.cursor() as cursor:
            cursor.execute(sql)

    def create_table(self, table_name, schema):
        sql = self.sql.create_table(table_name, schema)
        with self.cursor() as cursor:
            cursor.execute(sql)

    def insert_row(self, table_name, schema, row):
        sql = self.sql.insert_row(table_name, schema, row)
        with self.cursor() as cursor:
            cursor.execute(sql)
