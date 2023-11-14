import os

import mock
import pytest

from load_csv import load
from postgres_loader import PostgresLoader


@pytest.fixture
def postgres_config_path():
    return os.path.join(os.path.dirname(__file__), 'fixtures', 'conn.postgres')

@pytest.fixture
def loader(connection, cursor):
    with mock.patch('postgres_loader.PostgresLoader.connect', return_value=connection):
        loader = PostgresLoader('dbconnection')
        loader._conn = connection
        loader.connect.return_value = connection
        with mock.patch('load_csv.get_loader', return_value=loader):
            yield loader

def clean_execute_call_list(call_args_list):
    def clean_call(call_):
        return ' '.join(slug.strip() for slug in call_[0][0].split('\n') if slug.strip())
    return [clean_call(call_) for call_ in call_args_list]

def test_run_without_schema(loader, cursor, csv_path, table_schema_path, postgres_config_path):
    load('postgres', 'my_table', csv_path, table_schema_path, postgres_config_path)
    assert clean_execute_call_list(cursor.execute.call_args_list) == [
        "SELECT EXISTS ( SELECT 1 FROM   information_schema.tables WHERE  table_schema = 'public' AND    table_name = 'my_table' );",
        'DROP TABLE IF EXISTS my_table',
        'CREATE TABLE "public"."my_table" (player VARCHAR(255), game_date_key INTEGER, game_at TIMESTAMP, turn INTEGER, branch VARCHAR(255), branch_level INTEGER, note VARCHAR(255), ornamentation VARCHAR(255), god VARCHAR(255))',
        "INSERT INTO \"public\".\"my_table\" (player, game_date_key, game_at, turn, branch, branch_level, note, ornamentation, god) "
            "VALUES ('Chronos', 20181107, '2018-11-07 23:07:10', 1801, 'D', 2, 'Found a blossoming altar of Fedhas.', 'a blossoming', 'Fedhas')",
        "INSERT INTO \"public\".\"my_table\" (player, game_date_key, game_at, turn, branch, branch_level, note, ornamentation, god) "
            "VALUES ('COBRA', 20180915, '2018-09-15 01:09:38', 790, 'D', 2, 'Found a white marble altar of Elyvilon.', 'a white marble', 'Elyvilon')",
        "INSERT INTO \"public\".\"my_table\" (player, game_date_key, game_at, turn, branch, branch_level, note, ornamentation, god) "
            "VALUES ('CanOfBees', 20181017, '2018-10-17 13:16:48', 1449, 'D', 2, 'Found an iron altar of Okawaru.', 'an iron', 'Okawaru')",
    ]

def test_run_with_schema(loader, cursor, csv_path, table_schema_path, postgres_config_path):
    load('postgres', 'my_schema.my_table', csv_path, table_schema_path, postgres_config_path)
    assert clean_execute_call_list(cursor.execute.call_args_list) == [
        "SELECT EXISTS ( SELECT 1 FROM   information_schema.tables WHERE  table_schema = 'my_schema' AND    table_name = 'my_table' );",
        'DROP TABLE IF EXISTS my_schema.my_table',
        'CREATE SCHEMA IF NOT EXISTS "my_schema"; CREATE TABLE "my_schema"."my_table" (player VARCHAR(255), game_date_key INTEGER, game_at TIMESTAMP, turn INTEGER, branch VARCHAR(255), branch_level INTEGER, note VARCHAR(255), ornamentation VARCHAR(255), god VARCHAR(255));',
        "INSERT INTO \"my_schema\".\"my_table\" (player, game_date_key, game_at, turn, branch, branch_level, note, ornamentation, god) "
            "VALUES ('Chronos', 20181107, '2018-11-07 23:07:10', 1801, 'D', 2, 'Found a blossoming altar of Fedhas.', 'a blossoming', 'Fedhas')",
        "INSERT INTO \"my_schema\".\"my_table\" (player, game_date_key, game_at, turn, branch, branch_level, note, ornamentation, god) "
            "VALUES ('COBRA', 20180915, '2018-09-15 01:09:38', 790, 'D', 2, 'Found a white marble altar of Elyvilon.', 'a white marble', 'Elyvilon')",
        "INSERT INTO \"my_schema\".\"my_table\" (player, game_date_key, game_at, turn, branch, branch_level, note, ornamentation, god) "
            "VALUES ('CanOfBees', 20181017, '2018-10-17 13:16:48', 1449, 'D', 2, 'Found an iron altar of Okawaru.', 'an iron', 'Okawaru')",
    ]

def test_connection_is_closed_and_committed(loader, cursor, csv_path, table_schema_path, postgres_config_path):
    load('postgres', 'my_table', csv_path, table_schema_path, postgres_config_path)
    assert loader.connect.call_count == 1
    assert loader._conn.commit.call_count == 1
    assert loader._conn.close.call_count == 1

def test_cursors_are_closed(loader, cursor, csv_path, table_schema_path, postgres_config_path):
    load('postgres', 'my_table', csv_path, table_schema_path, postgres_config_path)
    assert cursor.__enter__.call_count == cursor.__exit__.call_count