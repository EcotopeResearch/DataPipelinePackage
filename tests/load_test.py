import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime
from ecopipeline import load_overwrite_database, create_new_table, check_table_exists, connect_db
from ecopipeline.config import _config_directory
import numpy as np
import math
import mysql.connector

config_info = {
    'database' : {
        'database' : 'test_db',
        'password' : 'pw',
        'host' : 'host',
        'user' : 'usr'
    },
    'minute' : {
        'table_name' :'minute_table'
    }
}

def test_load_overwrite_database(mocker):
    
    timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
    df = pd.DataFrame({'PowerIn_HPWH1': [3, 20, 30],
                    'PowerIn_HPWH2': [None, 75.2, 35]})
    df.index = timestamps

    # Create a mock for the cursor
    cursor_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')

    # Set the desired response for cursor.execute
    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [(datetime.datetime.strptime('03/01/2022', "%d/%m/%Y"),)],
        [('PowerIn_HPWH1',), ('PowerIn_HPWH2',)]
    ]

    # Call the function under test with the mock cursor
    load_overwrite_database(cursor_mock, df, config_info, 'minute')

    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "SELECT * FROM minute_table ORDER BY time_pt DESC LIMIT 1",
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test_db' AND table_name = 'minute_table'",
        "UPDATE minute_table SET PowerIn_HPWH1 = %s WHERE time_pt = '2022-01-01 00:00:00';",
        "UPDATE minute_table SET PowerIn_HPWH1 = %s, PowerIn_HPWH2 = %s WHERE time_pt = '2022-01-02 00:00:00';",
        'INSERT INTO minute_table (time_pt,PowerIn_HPWH1,PowerIn_HPWH2) VALUES (%s, %s, %s)'
    ]

    #  Verify the behavior and result
    assert cursor_mock.fetchall.call_count == 3
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_load_overwrite_database_add_columns(mocker):
    
    timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
    df = pd.DataFrame({'PowerIn_HPWH1': [3, 20, 30],
                    'float_column': [None, 75.2, 35],
                    'string_column': [None, 'hello', 'i am a test'],
                    'bool_column': [True, False, False],
                    'date_column': [None, datetime.datetime.strptime('03/01/2022', "%d/%m/%Y"), datetime.datetime.strptime('03/01/2022', "%d/%m/%Y")]})
    df.index = timestamps

    # Create a mock for the cursor
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [(datetime.datetime.strptime('03/01/2022', "%d/%m/%Y"),)],
        [('PowerIn_HPWH1',)]
    ]

    # Call the function under test with the mock cursor
    load_overwrite_database(cursor_mock, df, config_info, 'minute')

    #  Verify the behavior and result
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "SELECT * FROM minute_table ORDER BY time_pt DESC LIMIT 1",
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test_db' AND table_name = 'minute_table'",
        "ALTER TABLE minute_table ADD COLUMN float_column float DEFAULT NULL;",
        "ALTER TABLE minute_table ADD COLUMN string_column varchar(25) DEFAULT NULL;",
        "ALTER TABLE minute_table ADD COLUMN bool_column boolean DEFAULT NULL;",
        "ALTER TABLE minute_table ADD COLUMN date_column datetime DEFAULT NULL;",
        "UPDATE minute_table SET PowerIn_HPWH1 = %s, bool_column = %s WHERE time_pt = '2022-01-01 00:00:00';",
        "UPDATE minute_table SET PowerIn_HPWH1 = %s, float_column = %s, string_column = %s, bool_column = %s, date_column = %s WHERE time_pt = '2022-01-02 00:00:00';",
        'INSERT INTO minute_table (time_pt,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)'
    ]
    assert cursor_mock.fetchall.call_count == 3
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_load_overwrite_database_all_fail_update(mocker):
    
    timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
    df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan],
                    'PowerIn_HPWH2': [None, np.nan, None],
                    'None_column': [None, None, None]})
    df.index = timestamps

    # Create a mock for the cursor
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [(datetime.datetime.strptime('10/01/2022', "%d/%m/%Y"),)],
        [('PowerIn_HPWH1',)]
    ]

    # Call the function under test with the mock cursor
    load_overwrite_database(cursor_mock, df, config_info, 'minute')

    #  Verify the behavior and result
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "SELECT * FROM minute_table ORDER BY time_pt DESC LIMIT 1",
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test_db' AND table_name = 'minute_table'"
    ]
    assert cursor_mock.fetchall.call_count == 3
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_create_new_table_and_populate(mocker):
    
    timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
    df = pd.DataFrame({'PowerIn_HPWH1': [3, 20, 30],
                    'float_column': [None, 75.2, 35],
                    'string_column': [None, 'hello', 'i am a test'],
                    'bool_column': [True, False, False],
                    'date_column': [None, datetime.datetime.strptime('03/01/2022', "%d/%m/%Y"), datetime.datetime.strptime('03/01/2022', "%d/%m/%Y")],
                    'None_column': [None, None, None]})
    df.index = timestamps

    # Create a mock for the cursor
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    cursor_mock.fetchall.side_effect = [
        [(0,)]
    ]

    # Call the function under test with the mock cursor
    load_overwrite_database(cursor_mock, df, config_info, 'minute')

    #  Verify the behavior and result
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "CREATE TABLE minute_table (\ntime_pt datetime,\n"\
            +"PowerIn_HPWH1 float DEFAULT NULL,\n"\
            +"float_column float DEFAULT NULL,\n"\
            +"string_column varchar(25) DEFAULT NULL,\n"\
            +"bool_column boolean DEFAULT NULL,\n"\
            +"date_column datetime DEFAULT NULL,\n"\
            +"PRIMARY KEY (time_pt)\n"\
            +");",
        "INSERT INTO minute_table (time_pt,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)",
        "INSERT INTO minute_table (time_pt,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)",
        "INSERT INTO minute_table (time_pt,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)"
    ]
    assert cursor_mock.fetchall.call_count == 1
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_create_new_table_and_populate_different_primary(mocker):
    
    indexes = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
    df = pd.DataFrame({'PowerIn_HPWH1': [3, 20, 30],
                    'float_column': [None, 75.2, 35],
                    'string_column': [None, 'hello', 'i am a test'],
                    'bool_column': [True, False, False],
                    'date_column': [None, datetime.datetime.strptime('03/01/2022', "%d/%m/%Y"), datetime.datetime.strptime('03/01/2022', "%d/%m/%Y")],
                    'None_column': [None, None, None]})
    df.index = indexes

    # Create a mock for the cursor
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    cursor_mock.fetchall.side_effect = [
        [(0,)]
    ]

    # Call the function under test with the mock cursor
    load_overwrite_database(cursor_mock, df, config_info, 'minute', primary_key='primary_key')

    #  Verify the behavior and result
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "CREATE TABLE minute_table (\nprimary_key datetime,\n"\
            +"PowerIn_HPWH1 float DEFAULT NULL,\n"\
            +"float_column float DEFAULT NULL,\n"\
            +"string_column varchar(25) DEFAULT NULL,\n"\
            +"bool_column boolean DEFAULT NULL,\n"\
            +"date_column datetime DEFAULT NULL,\n"\
            +"PRIMARY KEY (primary_key)\n"\
            +");",
        "INSERT INTO minute_table (primary_key,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)",
        "INSERT INTO minute_table (primary_key,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)",
        "INSERT INTO minute_table (primary_key,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)"
    ]
    assert cursor_mock.fetchall.call_count == 1
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_create_new_table(mocker):

    # Create a mock for the cursor
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    assert create_new_table(cursor_mock, 'test_table', ['test_1','test_2','test_3','test_4','test_5'], ['float','float','varchar(25)','boolean','datetime'])

    #  Verify the behavior and result
    expected_queries = [
        "CREATE TABLE test_table (\ntime_pt datetime,\n"\
            +"test_1 float DEFAULT NULL,\n"\
            +"test_2 float DEFAULT NULL,\n"\
            +"test_3 varchar(25) DEFAULT NULL,\n"\
            +"test_4 boolean DEFAULT NULL,\n"\
            +"test_5 datetime DEFAULT NULL,\n"\
            +"PRIMARY KEY (time_pt)\n"\
            +");"
    ]
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_create_new_table_different_primary(mocker):

    # Create a mock for the cursor
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    assert create_new_table(
        cursor_mock, 
        'test_table', 
        ['test_1','test_2','test_3','test_4','test_5'], 
        ['float','float','varchar(25)','boolean','datetime'],
        primary_key = 'primary_key'
    )

    #  Verify the behavior and result
    expected_queries = [
        "CREATE TABLE test_table (\nprimary_key datetime,\n"\
            +"test_1 float DEFAULT NULL,\n"\
            +"test_2 float DEFAULT NULL,\n"\
            +"test_3 varchar(25) DEFAULT NULL,\n"\
            +"test_4 boolean DEFAULT NULL,\n"\
            +"test_5 datetime DEFAULT NULL,\n"\
            +"PRIMARY KEY (primary_key)\n"\
            +");"
    ]
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

def test_invalid_create_new_table(mocker):
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    with pytest.raises(Exception, match="Cannot create table. Type list and Field Name list are different lengths."):
        create_new_table(cursor_mock, 'test_table', ['test_1','test_2','test_3','test_4','test_5'], ['boolean','datetime'])

def test_check_table_exists(mocker):
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')
    cursor_mock.fetchall.side_effect = [
        [(0,)],
        [(1,)]
    ]
    assert check_table_exists(cursor_mock, 'dummy_table', 'dummy_db') == False
    assert check_table_exists(cursor_mock, 'dummy_table', 'dummy_db') == True

def test_connect_db():
    with patch('mysql.connector.connect') as mock_connect:
        # Set the desired response for mock_connect.return_value
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value

        # Call the function that uses mysql.connector.connect()
        connect_db(config_info['database'])

        # Assert that mysql.connector.connect() was called
        mock_connect.assert_called_once_with(user='usr', password='pw',
                                             host='host', database='test_db')
        