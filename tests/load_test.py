import pytest
from unittest.mock import MagicMock
import pandas as pd
import datetime
from ecopipeline import load_overwrite_database, create_new_table
from ecopipeline.config import _config_directory
import numpy as np
import math

config_info = {
    'database' : {
        'database' : 'test_db'
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
        'UPDATE minute_table SET PowerIn_HPWH1 = 3.0 WHERE time_pt = 2022-01-01 00:00:00;',
        'UPDATE minute_table SET PowerIn_HPWH1 = 20.0, PowerIn_HPWH2 = 75.2 WHERE time_pt = 2022-01-02 00:00:00;',
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
        'UPDATE minute_table SET PowerIn_HPWH1 = 3, bool_column = True WHERE time_pt = 2022-01-01 00:00:00;',
        'UPDATE minute_table SET PowerIn_HPWH1 = 20, float_column = 75.2, string_column = hello, bool_column = False, date_column = 2022-01-03 00:00:00 WHERE time_pt = 2022-01-02 00:00:00;',
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

# def test_create_new_columns(mocker):

#     # Create a mock for the cursor
#     cursor_mock = MagicMock()
#     mocker.patch.object(cursor_mock, 'execute')

#     # Call the function under test with the mock cursor
#     create_new_columns(cursor_mock, df, config_info, 'minute')

#     #  Verify the behavior and result
#     expected_queries = [
#         "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
#         "SELECT * FROM minute_table ORDER BY time_pt DESC LIMIT 1",
#         "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test_db' AND table_name = 'minute_table'",
#         "ALTER TABLE minute_table ADD COLUMN float_column float DEFAULT NULL;",
#         "ALTER TABLE minute_table ADD COLUMN string_column varchar(25) DEFAULT NULL;",
#         "ALTER TABLE minute_table ADD COLUMN bool_column boolean DEFAULT NULL;",
#         "ALTER TABLE minute_table ADD COLUMN date_column datetime DEFAULT NULL;",
#         'UPDATE minute_table SET PowerIn_HPWH1 = 3, bool_column = True WHERE time_pt = 2022-01-01 00:00:00;',
#         'UPDATE minute_table SET PowerIn_HPWH1 = 20, float_column = 75.2, string_column = hello, bool_column = False, date_column = 2022-01-03 00:00:00 WHERE time_pt = 2022-01-02 00:00:00;',
#         'INSERT INTO minute_table (time_pt,PowerIn_HPWH1,float_column,string_column,bool_column,date_column) VALUES (%s, %s, %s, %s, %s, %s)'
#     ]
#     assert cursor_mock.fetchall.call_count == 3
#     assert cursor_mock.execute.call_count == len(expected_queries)
#     for i in range (len(expected_queries)):
#         assert cursor_mock.execute.call_args_list[i][0][0] == expected_queries[i]

