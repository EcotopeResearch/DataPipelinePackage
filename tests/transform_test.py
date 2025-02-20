import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pandas.testing import assert_frame_equal
from ecopipeline.transform import *
import numpy as np
import math

def test_concat_last_row():
    df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan],
                    'PowerIn_HPWH3': [4, 8, 6],
                    'silly_strings': ['imma','goffygoober','yeah']})
    df.index = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])

    # put last row at begining of df
    last_row = pd.DataFrame({'PowerIn_HPWH1': [float('inf')],
                    'PowerIn_HPWH3': [4],
                    'silly_strings': ['imma']})
    last_row.index = pd.to_datetime(['2021-12-25'])

    expected = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('inf'), float('-inf'), math.nan],
                    'PowerIn_HPWH3': [4, 4, 8, 6],
                    'silly_strings': ['imma', 'imma','goffygoober','yeah']})
    expected.index = pd.to_datetime(['2021-12-25', '2022-01-01', '2022-01-02', '2022-01-05'])

    assert_frame_equal(concat_last_row(df, last_row), expected)

    #put last row in middle of df
    last_row = pd.DataFrame({'PowerIn_HPWH1': [float('inf')],
                    'PowerIn_HPWH3': [4],
                    'silly_strings': ['imma']})
    last_row.index = pd.to_datetime(['2022-01-04'])
    expected = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), float('inf'), math.nan],
                    'PowerIn_HPWH3': [4, 8, 4, 6],
                    'silly_strings': ['imma', 'goffygoober', 'imma','yeah']})
    expected.index = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-04', '2022-01-05'])
    assert_frame_equal(concat_last_row(df, last_row), expected)

@patch('ecopipeline.ConfigManager')
def test_rename_sensors_no_site(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'other_column_1': [None, None, None,None],
                        'other_column_2': [1,2,3,4]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
        df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan],
                        'PowerIn_HPWH2': [50, 70, None],
                        'None_column': [None, None, None],
                        'silly_varriable': [None, None, None],
                        'PowerIn_HPWH5': [float('inf'), 35, math.nan],
                        'silly_name': [7, 20, math.nan],
                        'PowerIn_HPWH3': [4, 8, 6],
                        'silly_strings': ['imma','goffygoober','yeah']})
        df.index = timestamps

        df_expected = pd.DataFrame({
                        'serious_var_3': [None, None, None],
                        'serious_var_2': [7, 20, math.nan],
                        'serious_var_4': ['imma','goffygoober','yeah']})
        df_expected.index = timestamps

        # Call the function that uses mysql.connector.connect()
        df = rename_sensors(df, mock_config_manager)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_rename_sensors_with_site(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'other_column_1': [None, None, None,None],
                        'site': ["silly_site","cereal_site","silly_site","silly_site"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
        df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan],
                        'PowerIn_HPWH2': [50, 70, None],
                        'None_column': [None, None, None],
                        'silly_varriable': [None, None, None],
                        'PowerIn_HPWH5': [float('inf'), 35, math.nan],
                        'silly_name': [7, 20, math.nan],
                        'PowerIn_HPWH3': [4, 8, 6],
                        'silly_strings': ['imma','goffygoober','yeah']})
        df.index = timestamps

        df_expected = pd.DataFrame({
                        'serious_var_3': [None, None, None],
                        'serious_var_4': ['imma','goffygoober','yeah']})
        df_expected.index = timestamps

        # Call the function that uses mysql.connector.connect()
        df = rename_sensors(df, mock_config_manager, "silly_site")

        # Assert that mysql.connector.connect() was called
        mock_csv.assert_called_once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_rename_sensors_error(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        # Set the desired response for mock_connect.return_value
        mock_csv.side_effect = FileNotFoundError

        timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
        df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan]})
        df.index = timestamps
        with pytest.raises(Exception, match="File Not Found: fake/path/whatever/Variable_Names.csv"):
            # Call the function that uses mysql.connector.connect()
            df = rename_sensors(df, mock_config_manager, "silly_site")
        # Assert that mysql.connector.connect() was called
        mock_csv.assert_called_once_with('fake/path/whatever/Variable_Names.csv')

@patch('ecopipeline.ConfigManager')
def test_rename_sensors_with_system(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'other_column_1': [None, None, None,None],
                        'system': ["silly_system","serious_system","silly_system","silly_system"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
        df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan],
                        'PowerIn_HPWH2': [50, 70, None],
                        'None_column': [None, None, None],
                        'silly_varriable': [None, None, None],
                        'PowerIn_HPWH5': [float('inf'), 35, math.nan],
                        'silly_name': [7, 20, math.nan],
                        'PowerIn_HPWH3': [4, 8, 6],
                        'silly_strings': ['imma','goffygoober','yeah']})
        df.index = timestamps

        df_maintained = df.copy()

        df_expected = pd.DataFrame({
                        'serious_var_3': [None, None, None],
                        'serious_var_4': ['imma','goffygoober','yeah']})
        df_expected.index = timestamps

        # Call the function that uses mysql.connector.connect()
        df_silly_system = rename_sensors(df, mock_config_manager, system="silly_system")

        # Assert that mysql.connector.connect() was called
        mock_csv.assert_called_once_with("fake/path/whatever/Variable_Names.csv")
        assert_frame_equal(df_silly_system, df_expected)
        assert_frame_equal(df, df_maintained) # ensure original dataframe left undisturbed

def test_avg_duplicate_times():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-01-01 00:01:00'])
    df = pd.DataFrame({'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df.index = timestamps
    df_expected = pd.DataFrame({
                    'None_column': [None, None],
                    'string_column': ['imma','yeah'],
                    'PowerIn_HPWH1': [40, math.nan],
                    'PowerIn_HPWH2': [60, None],
                    'silly_varriable': [None, 15]
                    })
    df_expected.index = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00'])
    df = avg_duplicate_times(df, None)
    assert_frame_equal(df, df_expected)

def test_avg_duplicate_times_with_tz():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-01-01 00:01:00'])
    df = pd.DataFrame({'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df.index = timestamps
    df_expected = pd.DataFrame({
                    'None_column': [None, None],
                    'string_column': ['imma','yeah'],
                    'PowerIn_HPWH1': [40, math.nan],
                    'PowerIn_HPWH2': [60, None],
                    'silly_varriable': [None, 15]
                    })
    df_expected.index = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00']).tz_localize('US/Pacific')
    df = avg_duplicate_times(df, 'US/Pacific')
    assert_frame_equal(df, df_expected)

def test_convert_timezone():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 01:00:00', '2022-01-01 01:01:00'])
    df = pd.DataFrame({'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df.index = timestamps
    df_expected = pd.DataFrame({
                    'time_pt' :  pd.to_datetime(['2021-12-31 16:00:00', '2021-12-31 17:00:00', '2021-12-31 17:01:00']),
                    'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df_expected.set_index('time_pt', inplace=True)
    df = convert_time_zone(df)
    assert_frame_equal(df, df_expected)

def test_convert_timezone_mountain_time():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 01:00:00', '2022-01-01 01:01:00'])
    df = pd.DataFrame({'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df.index = timestamps
    df_expected = pd.DataFrame({
                    'time_pt' :  pd.to_datetime(['2021-12-31 17:00:00', '2021-12-31 18:00:00', '2021-12-31 18:01:00']),
                    'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df_expected.set_index('time_pt', inplace=True)
    df = convert_time_zone(df, tz_convert_to="America/Denver")
    assert_frame_equal(df, df_expected)

def test_convert_timezone_daylight_savings():
    timestamps = pd.to_datetime(['2023-11-05 08:00:00', '2023-11-05 08:30:00', '2023-11-05 09:00:00'])
    df = pd.DataFrame({'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df.index = timestamps
    df_expected = pd.DataFrame({
                    'time_pt' :  pd.to_datetime(['2023-11-05 01:00:00', '2023-11-05 01:30:00', '2023-11-05 01:00:00']),
                    'PowerIn_HPWH1': [30, 50, math.nan],
                    'PowerIn_HPWH2': [50, 70, None],
                    'None_column': [None, None, None],
                    'string_column': ['imma','goffygoober','yeah'],
                    'silly_varriable': [None, None, 15]
                    })
    df_expected.set_index('time_pt', inplace=True)
    df = convert_time_zone(df)
    assert_frame_equal(df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_ffill_missing(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'changepoint': [1, 0, None, 1],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'ffill_length': [1, 2, None,None],
                        'site': [1,2,"silly_site","silly_site"]})
        mock_csv.return_value = csv_df
        timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00', '2022-01-01 00:02:00', '2022-01-01 00:03:00','2022-01-01 00:04:00'])
        df_input = pd.DataFrame({
                        'serious_var_1': [None, 1, None, None,None],
                        'serious_var_2': [None,5,None,None,None],
                        'serious_var_3': [None,None,3,None,None],
                        'serious_var_4': [None,2,3,None,4]})
        df_input.index = timestamps
        df_unchanged = df_input.copy()
        df_expected = pd.DataFrame({
                        'serious_var_1': [None, 1, 1, 1, 1],
                        'serious_var_2': [None,5,5,5,None],
                        'serious_var_3': [None,None,3,None,None],
                        'serious_var_4': [None,2,3,3,4]})
        df_expected.index = timestamps
        df_result = ffill_missing(df_input, mock_config_manager)
        assert_frame_equal(df_result, df_expected)
        # check that df_input was not changed in place
        assert_frame_equal(df_input, df_unchanged)

@patch('ecopipeline.ConfigManager')
def test_ffill_missing_out_of_order(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'changepoint': [1, 0, None, 1],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'ffill_length': [1, 2, None,None],
                        'site': [1,2,"silly_site","silly_site"]})
        mock_csv.return_value = csv_df
        df_input = pd.DataFrame({
                        'serious_var_1': [None, None, None,None, 1],
                        'serious_var_2': [None,None,None,None, 5],
                        'serious_var_3': [None,3,None,None,None],
                        'serious_var_4': [None,3,None,4,2]})
        df_input.index = pd.to_datetime(['2022-01-01 00:03:00', '2022-01-01 00:02:00', '2022-01-01 00:00:00','2022-01-01 00:04:00', '2022-01-01 00:01:00'])
        df_unchanged = df_input.copy()
        df_expected = pd.DataFrame({
                        'serious_var_1': [None, 1, 1, 1, 1],
                        'serious_var_2': [None,5,5,5,None],
                        'serious_var_3': [None,None,3,None,None],
                        'serious_var_4': [None,2,3,3,4]})
        df_expected.index = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00', '2022-01-01 00:02:00', '2022-01-01 00:03:00','2022-01-01 00:04:00'])
        df_result = ffill_missing(df_input, mock_config_manager)
        assert_frame_equal(df_result, df_expected)
        # check that df_input was not changed in place
        assert_frame_equal(df_input, df_unchanged)

@patch('ecopipeline.ConfigManager')
def test_ffill_missing_out_of_order_timeswitch(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'changepoint': [1, 0, None, 1],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'ffill_length': [1, 2, None,None],
                        'site': [1,2,"silly_site","silly_site"]})
        mock_csv.return_value = csv_df
        df_input = pd.DataFrame({
                        'serious_var_1': [None, None, None,None, 1],
                        'serious_var_2': [None,None,None,None, 5],
                        'serious_var_3': [None,3,None,None,None],
                        'serious_var_4': [None,3,None,4,2]})
        df_input.index = pd.to_datetime(['2024-11-03 01:59:00-07:00', '2024-11-03 01:58:00-07:00', '2024-11-03 01:00:00-07:00','2024-11-03 01:00:00-08:00', '2024-11-03 01:57:00-07:00'])
        df_unchanged = df_input.copy()
        df_expected = pd.DataFrame({
                        'serious_var_1': [None, 1, 1, 1, 1],
                        'serious_var_2': [None,5,5,5,None],
                        'serious_var_3': [None,None,3,None,None],
                        'serious_var_4': [None,2,3,3,4]})
        df_expected.index = pd.to_datetime(['2024-11-03 01:00:00-07:00', '2024-11-03 01:57:00-07:00', '2024-11-03 01:58:00-07:00', '2024-11-03 01:59:00-07:00','2024-11-03 01:00:00-08:00'])
        df_result = ffill_missing(df_input, mock_config_manager)
        assert_frame_equal(df_result, df_expected)
        # check that df_input was not changed in place
        assert_frame_equal(df_input, df_unchanged)


def test_cop_method_1():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-01-01 00:01:00'])
    df = pd.DataFrame({'HeatOut_Primary': [4,4,4],
                    'PowerIn_Total': [1,1,1],
                    'rericLosses': [None, None, None],
                    })
    rericLosses = 2
    df.index = timestamps
    df_expected = pd.DataFrame({'HeatOut_Primary': [4,4,4],
                    'PowerIn_Total': [1, 1, 1],
                    'rericLosses': [None, None, None],
                    'COP_DHWSys_1': [6.,6.,6.]
                    })
    df_expected.index = timestamps
    df = cop_method_1(df, rericLosses)
    assert_frame_equal(df, df_expected)


def test_cop_method_1_list():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-01-01 00:01:00'])
    df = pd.DataFrame({'HeatOut_Primary': [4,4,4],
                    'PowerIn_Total': [1,1,1],
                    'rericLosses': [2, 1, 2]
                    })
    df.index = timestamps
    rericLosses = df['rericLosses']
    df_expected = pd.DataFrame({'HeatOut_Primary': [4,4,4],
                    'PowerIn_Total': [1, 1, 1],
                    'rericLosses': [2, 1, 2],
                    'COP_DHWSys_1': [6.,5.,6.]
                    })
    df_expected.index = timestamps
    df = cop_method_1(df, rericLosses)
    assert_frame_equal(df, df_expected)

def test_cop_method_2():
    timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-01-01 00:01:00'])
    df = pd.DataFrame({'HeatOut_Primary': [4,4,4],
                    'PowerIn_Total': [1,1,1],
                    'Primary_COP': [2, 2.4, 3],
                    'PowerIn_HPWH1': [2, 2.4, 3],
                    'PowerIn_SecLoopPump': [2, 2.4, 3],
                    'PowerIn_SwingTank2': [2, 2.4, 3],
                    'PowerIn_ERTank': [2, 2.4, 3],
                    'Not_Used': ['blah blah', 50, 100],
                    })
    cop_tm = 2
    #rericLosses = df['rericLosses']
    df.index = timestamps
    df_expected = pd.DataFrame({'HeatOut_Primary': [4,4,4],
                    'PowerIn_Total': [1,1,1],
                    'Primary_COP': [2, 2.4, 3],
                    'PowerIn_HPWH1': [2, 2.4, 3],
                    'PowerIn_SecLoopPump': [2, 2.4, 3],
                    'PowerIn_SwingTank2': [2, 2.4, 3],
                    'PowerIn_ERTank': [2, 2.4, 3],
                    'Not_Used': ['blah blah', 50, 100],
                    'COP_DHWSys_2': [16.0, 21.119999999999997, 30.0]
                    })
    df_expected.index = timestamps
    df = cop_method_2(df, cop_tm, 'Primary_COP')
    assert_frame_equal(df, df_expected)

def test_aggregate_df():
     with patch('os.path.exists') as mock_os:
        mock_os.return_value = True
        with patch('pandas.read_csv') as mock_csv:
            csv_df = pd.DataFrame({'date': ['4/21/2022', '4/21/2022', '4/22/2022', '4/24/2022'],
                            'startTime': ['12:00', '16:00', '10:00', '10:00'],
                            'endTime': ['16:00', '21:00', '16:00', '16:00'],
                            'event': ['loadUp', 'shed', 'loadUp', 'loadUp']})
            mock_csv.return_value = csv_df
            timestamps = pd.to_datetime(['2022-04-21 00:00:00', '2022-04-21 16:00:00', '2022-04-21 16:02:00', '2022-04-22 10:03:00','2022-04-23 15:04:00','2022-04-24 00:04:00'])
            hourly_timestamps = pd.to_datetime(['2022-04-21 00:00:00', '2022-04-21 16:00:00', '2022-04-22 10:00:00','2022-04-23 15:00:00'])
            daily_timestamps = pd.to_datetime(['2022-04-21 00:00:00', '2022-04-22 00:00:00','2022-04-23 00:00:00'])
            df_input = pd.DataFrame({
                            'Energy_serious_var_1': [None, 1, 2, 3,4,0],
                            'serious_var_2': [None,5,7,2,None,4],
                            'serious_var_3': [2,None,3,5,8,3]})
            df_input.index = timestamps
            df_unchanged = df_input.copy()
            hourly_df_expected = pd.DataFrame({
                            'Energy_serious_var_1': [0., 3., 3.,4.],
                            'serious_var_2': [None,6,2,None],
                            'serious_var_3': [2,3.,5.,8],
                            'system_state' : ['normal', 'shed', 'loadUp', 'normal']
                            })
            hourly_df_expected.index = hourly_timestamps
            daily_df_expected = pd.DataFrame({
                            'Energy_serious_var_1': [3., 3.,4.],
                            'serious_var_2': [6,2,None],
                            'serious_var_3': [2.5,5,8]
                            })
            daily_df_expected.index = daily_timestamps
            daily_df_expected = daily_df_expected.resample("D").mean()
            daily_df_expected['load_shift_day'] = True
            daily_df_expected.at[daily_df_expected.index[-1], 'load_shift_day'] = False

            hourly_result, daily_result = aggregate_df(df_input, "full/path/to/pipeline/input/loadshift_matrix.csv", remove_partial=False)
            assert len(hourly_result.index) == 73
            hourly_result = hourly_result.loc[hourly_result.index.isin(['2022-04-21 00:00:00', '2022-04-21 16:00:00', '2022-04-22 10:00:00','2022-04-23 15:00:00'])]
            assert_frame_equal(hourly_result, hourly_df_expected)
            daily_result = daily_result.loc[daily_result.index.isin(['2022-04-21 00:00:00', '2022-04-22 00:00:00','2022-04-23 00:00:00'])]
            assert_frame_equal(daily_df_expected, daily_result)
            # check that df_input was not changed in place
            assert_frame_equal(df_input, df_unchanged)

def test_remove_partial_days():
    # UTC
    minute_times = []
    hour_times = []
    day_times = []
    for day in range(1,18):
        day_times.append(f"2022-06-{'{:02d}'.format(day)} 00:00:00")
        for hour in range(24):
            hour_times.append(f"2022-06-{'{:02d}'.format(day)} {'{:02d}'.format(hour)}:00:00")
            for minute in range(60):
                minute_times.append(f"2022-06-{'{:02d}'.format(day)} {'{:02d}'.format(hour)}:{'{:02d}'.format(minute)}:00")

    minute_timestamps = pd.to_datetime(minute_times)
    minute_df = pd.DataFrame({'HeatOut_Primary': [4]*len(minute_times)})
    minute_df.index = minute_timestamps
    hour_timestamps = pd.to_datetime(hour_times)
    hour_df = pd.DataFrame({'HeatOut_Primary': [4]*len(hour_times)})
    hour_df.index = hour_timestamps
    day_timestamps = pd.to_datetime(day_times)
    day_df = pd.DataFrame({'HeatOut_Primary': [4]*len(day_times)})
    day_df.index = day_timestamps

    hour_df, day_df = remove_partial_days(minute_df, hour_df, day_df)
    
    # full data set, nothing is removed.
    assert day_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == day_times
    assert hour_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == hour_times

    # incomplete in begining
    start_time = pd.Timestamp('2021-06-03 01:00:00')
    end_time = pd.Timestamp('2022-06-02 02:05:00')
    minute_df = minute_df.loc[(minute_df.index < start_time) | (minute_df.index > end_time)]
    day_times = day_times[2:]
    hour_times = hour_times[26:]
    hour_df, day_df = remove_partial_days(minute_df, hour_df, day_df)
    assert day_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == day_times
    assert hour_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == hour_times

    # incomplete in middle
    start_time = pd.Timestamp('2022-06-05 01:00:00')
    end_time = pd.Timestamp('2022-06-05 01:30:00')
    minute_df = minute_df.loc[(minute_df.index < start_time) | (minute_df.index > end_time)]
    day_times.remove('2022-06-05 00:00:00')
    hour_times.remove('2022-06-05 01:00:00')
    hour_df, day_df = remove_partial_days(minute_df, hour_df, day_df)
    assert day_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == day_times
    assert hour_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == hour_times

    # incomplete in end
    start_time = pd.Timestamp('2022-06-17 20:00:00')
    end_time = pd.Timestamp('2023-06-05 01:30:00')
    minute_df = minute_df.loc[(minute_df.index < start_time) | (minute_df.index > end_time)]
    day_times = day_times[:-1]
    hour_times = hour_times[:-4]
    hour_df, day_df = remove_partial_days(minute_df, hour_df, day_df)
    assert day_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == day_times
    assert hour_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == hour_times

def test_remove_partial_days_one_column():
    # UTC
    minute_times = []
    hour_times = []
    day_times = []
    for day in range(1,18):
        day_times.append(f"2022-06-{'{:02d}'.format(day)} 00:00:00")
        for hour in range(24):
            hour_times.append(f"2022-06-{'{:02d}'.format(day)} {'{:02d}'.format(hour)}:00:00")
            for minute in range(60):
                minute_times.append(f"2022-06-{'{:02d}'.format(day)} {'{:02d}'.format(hour)}:{'{:02d}'.format(minute)}:00")

    minute_timestamps = pd.to_datetime(minute_times)
    minute_df = pd.DataFrame({'HeatOut_Primary': [4]*len(minute_times),
                              'HeatOut_Swing': [4]*len(minute_times)})
    minute_df.index = minute_timestamps
    hour_timestamps = pd.to_datetime(hour_times)
    hour_df = pd.DataFrame({'HeatOut_Primary': [4]*len(hour_times),
                            'HeatOut_Swing': [4]*len(hour_times)})
    hour_df.index = hour_timestamps
    day_timestamps = pd.to_datetime(day_times)
    day_df = pd.DataFrame({'HeatOut_Primary': [4]*len(day_times),
                           'HeatOut_Swing': [4]*len(day_times)})
    day_df.index = day_timestamps

    hour_df, day_df = remove_partial_days(minute_df, hour_df, day_df)
    
    # full data set, nothing is removed.
    assert day_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == day_times
    assert hour_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == hour_times

    # incomplete in middle
    start_time = pd.Timestamp('2022-06-05 01:00:00')
    end_time = pd.Timestamp('2022-06-05 10:30:00')
    minute_df.loc[(minute_df.index > start_time) & (minute_df.index < end_time), 'HeatOut_Primary'] = np.nan
    hour_df, day_df = remove_partial_days(minute_df, hour_df, day_df)
    assert day_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == day_times
    assert hour_df.index.strftime('%Y-%m-%d %H:%M:%S').tolist() == hour_times
    assert np.isnan(day_df.loc[pd.Timestamp('2022-06-05 00:00:00'), "HeatOut_Primary"])
    assert day_df.loc[pd.Timestamp('2022-06-05 00:00:00'), "HeatOut_Swing"] == 4
    assert day_df.loc[pd.Timestamp('2022-06-06 00:00:00'), "HeatOut_Primary"] == 4
    assert np.isnan(hour_df.loc[pd.Timestamp('2022-06-05 02:00:00'), "HeatOut_Primary"])
    assert np.isnan(hour_df.loc[pd.Timestamp('2022-06-05 06:00:00'), "HeatOut_Primary"])
    assert hour_df.loc[pd.Timestamp('2022-06-05 02:00:00'), "HeatOut_Swing"] == 4
    assert hour_df.loc[pd.Timestamp('2022-06-05 06:00:00'), "HeatOut_Swing"] == 4
    assert hour_df.loc[pd.Timestamp('2022-06-06 11:00:00'), "HeatOut_Primary"] == 4

def test_round_time():
    # UTC
    timestamps = pd.to_datetime(['2022-01-01 08:00:25', '2023-11-05 08:01:59', '2023-11-05 09:01:01'])
    timestamps_expected = pd.to_datetime(['2022-01-01 08:00:00', '2023-11-05 08:01:00', '2023-11-05 09:01:00'])
    df = pd.DataFrame({'HeatOut_Primary': [4,4,4]})
    df.index = timestamps
    df_expected = pd.DataFrame({'HeatOut_Primary': [4,4,4]})
    df_expected.index = timestamps_expected
    round_time(df)
    assert_frame_equal(df, df_expected)

    #PST
    timestamps = timestamps.tz_localize('UTC').tz_convert('US/Pacific')
    timestamps_expected = timestamps_expected.tz_localize('UTC').tz_convert('US/Pacific')
    # timestamps = pd.to_datetime(['2023-11-05 01:51:00-07:00', '2023-11-05 01:51:00-07:04', '2023-11-05 01:01:00-08:00'])
    # timestamps_expected = pd.to_datetime(['2023-11-05 01:51:00-07:00', '2023-11-05 01:51:00-07:00', '2023-11-05 01:01:00-08:00'])
    df = pd.DataFrame({'HeatOut_Primary': [4,4,4]})
    df.index = timestamps
    df_expected = pd.DataFrame({'HeatOut_Primary': [4,4,4]})
    df_expected.index = timestamps_expected
    round_time(df)
    assert_frame_equal(df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_remove_outliers(mock_config_manager):

    mock_config_manager.get_var_names_path.return_value = "whatever/Variable_Names.csv"

    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'lower_bound': [5, 10, 5, 15],
                        'site': ["site_1", "site_1", "site_2", "site_1"],
                        'upper_bound': [15, 110, 15, 115]})
        mock_csv.return_value = csv_df
        timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00', '2022-01-01 00:02:00', '2022-01-01 00:03:00','2022-01-01 00:04:00'])
        df_input = pd.DataFrame({
                        'serious_var_1': [8, 1, 10, 50, -3],
                        'serious_var_2': [8, 1, 10.7, 50, -3],
                        'serious_var_3': [8, 1, 10, 50, -30.789],
                        'serious_var_4': [8, 1, 10, 50, -3]})
        df_input.index = timestamps
        df_expected = pd.DataFrame({
                        'serious_var_1': [8, np.NaN, 10, np.NaN, np.NaN],
                        'serious_var_2': [np.NaN, np.NaN, 10.7, 50, np.NaN],
                        'serious_var_3': [8, np.NaN, 10, np.NaN, np.NaN],
                        'serious_var_4': [np.NaN, np.NaN, np.NaN, 50, np.NaN]})
        df_expected.index = timestamps

        assert_frame_equal(remove_outliers(df_input, mock_config_manager), df_expected)

        timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00', '2022-01-01 00:02:00', '2022-01-01 00:03:00','2022-01-01 00:04:00'])
        df_expected = pd.DataFrame({
                        'serious_var_1': [8, np.NaN, 10, np.NaN, np.NaN],
                        'serious_var_2': [np.NaN, np.NaN, 10.7, 50, np.NaN],
                        'serious_var_3': [8, 1, 10, 50, -30.789],
                        'serious_var_4': [np.NaN, np.NaN, np.NaN, 50, np.NaN]})
        df_expected.index = timestamps

        assert_frame_equal(remove_outliers(df_input, mock_config_manager, site="site_1"), df_expected)

@patch('ecopipeline.ConfigManager')
def test_nullify_erroneous(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4'],
                        'error_value': [1, 2, None,None]})
        mock_csv.return_value = csv_df
        timestamps = pd.to_datetime(['2022-01-01 00:00:00', '2022-01-01 00:01:00', '2022-01-01 00:02:00', '2022-01-01 00:03:00','2022-01-01 00:04:00'])
        df_input = pd.DataFrame({
                        'serious_var_1': [None, 1, 2, 3,4],
                        'serious_var_2': [None,5,1.4,2,None],
                        'serious_var_3': [None,None,3,None,None]})
        df_input.index = timestamps
        df_unchanged = df_input.copy()
        df_expected = pd.DataFrame({
                        'serious_var_1': [None, np.NaN, 2, 3,4],
                        'serious_var_2': [None,5,1.4,np.NaN,None],
                        'serious_var_3': [None,None,3,None,None]})
        df_expected.index = timestamps
        df_result = nullify_erroneous(df_input, mock_config_manager)
        assert_frame_equal(df_result, df_expected)
        # check that df_input was not changed in place
        assert_frame_equal(df_input, df_unchanged)
