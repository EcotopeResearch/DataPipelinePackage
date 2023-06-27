import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pandas.testing import assert_frame_equal
import datetime
from ecopipeline import rename_sensors, avg_duplicate_times, remove_outliers, ffill_missing, nullify_erroneous, sensor_adjustment, round_time, aggregate_df, join_to_hourly, concat_last_row, join_to_daily, cop_method_1, cop_method_2, create_summary_tables, remove_partial_days
from ecopipeline.config import _config_directory
import numpy as np
import math
import mysql.connector

def test_rename_sensors_no_site():
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
        rename_sensors(df, 'fake/path/whatever/')

        # Assert that mysql.connector.connect() was called
        mock_csv.assert_called_once_with('fake/path/whatever/')
        assert_frame_equal(df, df_expected)

def test_rename_sensors_with_site():
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
        rename_sensors(df, 'fake/path/whatever/', "silly_site")

        # Assert that mysql.connector.connect() was called
        mock_csv.assert_called_once_with('fake/path/whatever/')
        assert_frame_equal(df, df_expected)

def test_rename_sensors_error():
    with patch('pandas.read_csv') as mock_csv:
        # Set the desired response for mock_connect.return_value
        mock_csv.side_effect = FileNotFoundError

        timestamps = pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-05'])
        df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), float('-inf'), math.nan]})
        df.index = timestamps
        with pytest.raises(Exception, match="File Not Found: fake/path/whatever/"):
            # Call the function that uses mysql.connector.connect()
            rename_sensors(df, 'fake/path/whatever/', "silly_site")
        # Assert that mysql.connector.connect() was called
        mock_csv.assert_called_once_with('fake/path/whatever/')

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


