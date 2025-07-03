import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime
from ecopipeline.load import *
from ecopipeline import ConfigManager
import numpy as np
import math

config_info = {
    'database' : 'test_db',
    'minute' : {
        'table_name' :'minute_table'
    }
}

def test_load_overwrite_database(mocker):
    
    timestamps = pd.to_datetime(['2022-01-01', '2022-01-05', '2022-01-02'])
    df = pd.DataFrame({'PowerIn_HPWH1': [3, 20, 30],
                    'PowerIn_HPWH2': [None, 75.2, 35]})
    df.index = timestamps

    # Create a mock for the db connect
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    # Set the desired response for cursor.execute
    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [(datetime.datetime.strptime('01/01/2022', "%d/%m/%Y"),),(datetime.datetime.strptime('05/01/2022', "%d/%m/%Y"),)],
        [('PowerIn_HPWH1',), ('PowerIn_HPWH2',)]
    ]
    configMock.connect_db.side_effect = [(con_mock,cursor_mock)]

    # Call the function under test with the mock cursor
    load_overwrite_database(configMock, df, config_info, 'minute')

    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "SELECT time_pt FROM minute_table WHERE time_pt >= '2022-01-01 00:00:00'",
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test_db' AND table_name = 'minute_table'",
        "UPDATE minute_table SET PowerIn_HPWH1 = %s WHERE time_pt = '2022-01-01 00:00:00';",
        "UPDATE minute_table SET PowerIn_HPWH1 = %s, PowerIn_HPWH2 = %s WHERE time_pt = '2022-01-05 00:00:00';",
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

    # Create a mock for the db connect
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [(datetime.datetime.strptime('01/01/2022', "%d/%m/%Y"),),(datetime.datetime.strptime('02/01/2022', "%d/%m/%Y"),)],
        [('PowerIn_HPWH1',)]
    ]
    configMock.connect_db.side_effect = [(con_mock,cursor_mock)]

    # Call the function under test with the mock cursor
    load_overwrite_database(configMock, df, config_info, 'minute')

    #  Verify the behavior and result
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "SELECT time_pt FROM minute_table WHERE time_pt >= '2022-01-01 00:00:00'",
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

    # Create a mock for the db connect
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [(datetime.datetime.strptime('01/01/2022', "%d/%m/%Y"),),(datetime.datetime.strptime('02/01/2022', "%d/%m/%Y"),),(datetime.datetime.strptime('05/01/2022', "%d/%m/%Y"),)],
        [('PowerIn_HPWH1',)]
    ]
    configMock.connect_db.side_effect = [(con_mock,cursor_mock)]
    # Call the function under test with the mock configMock
    load_overwrite_database(configMock, df, config_info, 'minute')

    #  Verify the behavior and result
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'minute_table')",
        "SELECT time_pt FROM minute_table WHERE time_pt >= '2022-01-01 00:00:00'",
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

    # Create a mock for the db connect
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    cursor_mock.fetchall.side_effect = [
        [(0,)]
    ]
    configMock.connect_db.side_effect = [(con_mock,cursor_mock)]
    # Call the function under test with the mock cursor
    load_overwrite_database(configMock, df, config_info, 'minute')

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

    # Create a mock for the db connect
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    cursor_mock.fetchall.side_effect = [
        [(0,)]
    ]
    configMock.connect_db.side_effect = [(con_mock,cursor_mock)]
    # Call the function under test with the mock cursor
    load_overwrite_database(configMock, df, config_info, 'minute', primary_key='primary_key')

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

# def test_connect_db():
#     with patch('os.path.exists') as mock_os_path:
#         mock_os_path.return_value = True
#     with patch('mysql.connector.connect') as mock_connect:
#         # Set the desired response for mock_connect.return_value
#         mock_connection = mock_connect.return_value
#         mock_cursor = mock_connection.cursor.return_value

#         # Call the function that uses mysql.connector.connect()
#         connect_db(config_info['database'])

#         # Assert that mysql.connector.connect() was called
#         mock_connect.assert_called_once_with(user='usr', password='pw',
#                                              host='host', database='test_db')

def test_load_event_table(mocker):
    
    event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02'])
    df = pd.DataFrame({
                    'start_time_pt': event_time_pts,
                    'end_time_pt': event_time_pts,
                    'event_type': ['SILENT_ALARM'] * 3,
                    'event_detail': ["Upper bound alarm for serious_var_1 (first one at 01:01).",
                                        "Upper bound alarm for serious_var_2 (first one at 01:03).",
                                        "Upper bound alarm for serious_var_2 (first one at 01:01)."],
                    'variable_name' : ['serious_var_1','serious_var_2','serious_var_2']})
    df.set_index('start_time_pt', inplace=True)

    # Create a mock for the db connect
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    # Set the desired response for cursor.execute
    cursor_mock.fetchall.side_effect = [
        [(1,)],
        [
            (1, datetime.datetime(2022, 1, 1, 0, 0), datetime.datetime(2022, 1, 1, 0, 0), "Upper bound alarm for serious_var_2 (first one at 01:03).", 'SILENT_ALARM', 'serious_var_2', 'automatic_upload'),
            (2, datetime.datetime(2022, 1, 2, 0, 0), datetime.datetime(2022, 1, 2, 0, 0), 'its-a me! Mario!', 'MISC_EVENT', None, 'user_b'),
            (3, datetime.datetime(2022, 1, 1, 0, 0), datetime.datetime(2022, 1, 1, 0, 0), "Upper bound alarm for serious_var_5 (first one at 01:03).", 'SILENT_ALARM', 'serious_var_5', 'automatic_upload'),
        ]
    ]
    configMock.connect_db.side_effect = [(con_mock,cursor_mock)]
    configMock.get_db_name.return_value = "db_name"

    # Call the function under test with the mock cursor
    load_event_table(configMock, df, 'silly_site')

    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'db_name') AND (TABLE_NAME = 'site_events')",
        "SELECT id, start_time_pt, end_time_pt, event_detail, event_type, variable_name, last_modified_by FROM site_events WHERE start_time_pt >= '2022-01-01 00:00:00' AND site_name = 'silly_site'",
        "INSERT INTO site_events (start_time_pt,site_name,end_time_pt,event_type,event_detail, variable_name, last_modified_date, last_modified_by) VALUES (%s,%s,%s,%s,%s,%s",
        "UPDATE site_events SET end_time_pt = %s, event_type = %s, event_detail = %s, variable_name = %s, last_modified_by = 'automatic_upload', last_modified_date = ",
        "INSERT INTO site_events (start_time_pt,site_name,end_time_pt,event_type,event_detail, variable_name, last_modified_date, last_modified_by) VALUES (%s,%s,%s,%s,%s,%s",
    ]

    #  Verify the behavior and result
    assert cursor_mock.fetchall.call_count == 2
    assert cursor_mock.execute.call_count == len(expected_queries)
    for i in range (len(expected_queries)):
        assert cursor_mock.execute.call_args_list[i][0][0][:len(expected_queries[i])] == expected_queries[i]
        