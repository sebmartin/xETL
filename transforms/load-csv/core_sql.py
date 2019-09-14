from abc import ABC

class Sql(ABC):

    @classmethod
    def table_exists(cls, table_name):
        raise NotImplementedError()

    @classmethod
    def drop_table(cls, table_name):
        create_table = (
            'DROP TABLE IF EXISTS {}'.format(table_name)
        )
        return create_table

    @classmethod
    def create_table(cls, table_name, columns):
        create_table = (
            'CREATE TABLE {} ('.format(cls.escape_table_name(table_name)) +
            ', '.join('{} {}'.format(name_, type_) for name_, type_ in columns.items()) +
            ')'
        )
        return create_table

    @classmethod
    def insert_row(cls, table_name, columns, row):
        escaped_rows = [(col_name, cls.escape_value(value, col_type)) for (value, (col_name, col_type)) in zip(row, columns.items())]
        col_names, values = zip(*escaped_rows)
        sql = 'INSERT INTO {table_name} ({col_names}) VALUES ({values})'.format(
            table_name=cls.escape_table_name(table_name),
            col_names=', '.join(col_names),
            values=', '.join(values)
        )
        return sql

    @classmethod
    def escape_table_name(cls, table_name):
        raise NotImplementedError()

    @classmethod
    def escape_value(cls, value, type_):
        quote_prefix = [
            'VARCHAR',
            'CHAR',
            'TEXT',
            'TIMESTAMP'
        ]
        type_ = type_.upper()
        if any(type_.startswith(prefix) for prefix in quote_prefix):
            return "'{}'".format(value.replace("'", "\'"))
        return value

    # *** old implementation for sqlite ... still useful?
    # return {
    #     'TEXT':     lambda v: '"{}"'.format(v.replace('"', '\"')),
    #     'REAL':     lambda v: str(float(v)),
    #     'INTEGER':  lambda v: str(int(v)),
    # }.get(type_.upper(), lambda v: v)(value)
