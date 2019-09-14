import psycopg2

from loader import SqlBaseLoader
from core_sql import Sql

class PostgresSql(Sql):
    @classmethod
    def table_exists(cls, table_name):
        schema, table = cls.parse_table_name(table_name)
        return """
            SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_schema = '{schema}'
            AND    table_name = '{table_name}'
        );""".format(schema=schema, table_name=table)

    @classmethod
    def parse_table_name(cls, table_name):
        splits = [word for word in table_name.split('.') if word]
        if len(splits) == 1:
            schema, table = 'public', splits[0]
        elif len(splits) == 2:
            schema, table = splits
        else:
            raise ValueError('Table name should be in the format <table_name> or <schema>.<table_name>')
        return schema, table

    @classmethod
    def escape_table_name(cls, table_name):
        schema, table = cls.parse_table_name(table_name)
        return '"{schema}"."{table_name}"'.format(
            schema=schema,
            table_name=table
        )

    @classmethod
    def create_table(cls, table_name, columns):
        schema, table = cls.parse_table_name(table_name)
        create_query = super(PostgresSql, cls).create_table(table_name, columns)
        if schema != 'public':
            return """
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                {create_query};
            """.format(schema=schema, create_query=create_query)
        else:
            return create_query


class PostgresLoader(SqlBaseLoader):
    def __init__(self, connection):
        super().__init__(connection=connection)

    def connect(self, connection):
        return psycopg2.connect(connection)

    @property
    def sql(self):
        return PostgresSql
