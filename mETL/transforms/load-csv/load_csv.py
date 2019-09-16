import os
import simplejson as json
import csv

from mETL.utils import options_parser

from postgres_loader import PostgresLoader
from sqlite_loader import SqliteLoader

class PostgresCsvDialect(csv.Dialect):
    delimiter = '~'
    quoting = csv.QUOTE_MINIMAL
    quotechar = '"'
    lineterminator = '\n'

def load_table_schema(path):
    path = os.path.abspath(path)
    assert os.path.exists(path), 'Schema not found: {}'.format(path)
    with open(path, 'r') as fd:
        return json.load(fd)

def load_csv_rows(path):
    path = os.path.abspath(path)
    assert os.path.exists(path), 'CSV path does not exist: {}'.format(path)
    print(' > Loading CSV at {}'.format(path))
    with open(path, 'r') as fd:
        reader = csv.reader(fd, dialect=PostgresCsvDialect())
        for row in reader:
            yield row

def get_loader(type_, **configuration):
    LOADERS = {
        'postgres': PostgresLoader,
        'sqlite': SqliteLoader
    }
    assert type_ in LOADERS, 'Invalid loader type: {}, expected one of {}'.format(type_, LOADERS.keys())
    return LOADERS.get(type_)(**configuration)

def load(load_type, table_name, csv_path, schema_path, configuration):
    table_schema = load_table_schema(schema_path)
    with open(configuration) as fd:
        configuration = json.load(fd)
    rows = 0
    with get_loader(load_type, **configuration) as loader:
        if loader.table_exists(table_name):
            loader.drop_table(table_name)
        loader.create_table(table_name, table_schema)

        for row in load_csv_rows(csv_path):
            loader.insert_row(table_name, table_schema, row)
            rows += 1
    print(' > Successfully loaded {} rows.'.format(rows))

if __name__ == '__main__':
    options = options_parser.from_manifest()
    load(**options)
