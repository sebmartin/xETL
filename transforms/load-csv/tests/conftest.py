import os
import mock
import pytest

@pytest.fixture
def cursor():
    cursor = mock.MagicMock(name='mock postres cursor')
    cursor.__enter__.return_value = cursor
    return cursor

@pytest.fixture
def connection(cursor):
    connection = mock.MagicMock(name='mock postres connection')
    connection.__enter__.return_value = connection
    connection.cursor.return_value = cursor
    return connection

@pytest.fixture
def csv_path():
    return os.path.join(os.path.dirname(__file__), 'fixtures', 'altars', 'altars.csv')

@pytest.fixture
def table_schema_path():
    return os.path.join(os.path.dirname(__file__), 'fixtures', 'altars', 'altars.schema.json')
