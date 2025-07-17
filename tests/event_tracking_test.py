import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pandas.testing import assert_frame_equal
from ecopipeline.event_tracking import *
import numpy as np
import math

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'low_alarm': [0, None, 3,"what's a number?",12.5],
                        'high_alarm': [1,2,None,None,76]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05','2022-01-02 01:01','2022-01-02 01:02'])
        df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), float('inf')],
                        'serious_var_2': [2, 2, 90, 2, 2, 90, 90],
                        'serious_var_3': [4, 2, 2, 2, 4, 4, 4],
                        'serious_var_4': [4, 2, 2, 2, 4, 4, 4]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-01'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM','SILENT_ALARM'],
                        'event_detail': ["Upper bound alarm for serious_var_1 (longest at 01:01 for 5 minutes). Avg fault time : 5.0 minutes, Avg value during fault: inf",
                                         "Lower bound alarm for serious_var_3 (longest at 01:02 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: 2.0"],
                        'variable_name' : ['serious_var_1','serious_var_3']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_with_fault_times(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'low_alarm': [0, None, 3,"what's a number?",12.5],
                        'high_alarm': [1,2,None,None,76],
                        'fault_time' : [None,1,15,None,None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05','2022-01-02 01:01','2022-01-02 01:02'])
        df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), float('inf')],
                        'serious_var_2': [2, 2, 90, 2, 2, 90, 90],
                        'serious_var_3': [4, 2, 2, 2, 4, 4, 4],
                        'serious_var_4': [4, 2, 2, 2, 4, 4, 4]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM'] * 3,
                        'event_detail': ["Upper bound alarm for serious_var_1 (longest at 01:01 for 5 minutes). Avg fault time : 5.0 minutes, Avg value during fault: inf",
                                         "Upper bound alarm for serious_var_2 (longest at 01:03 for 1 minutes). Avg fault time : 1.0 minutes, Avg value during fault: 90.0",
                                         "Upper bound alarm for serious_var_2 (longest at 01:01 for 2 minutes). Avg fault time : 2.0 minutes, Avg value during fault: 90.0"],
                        'variable_name' : ['serious_var_1','serious_var_2','serious_var_2']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_with_days(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'pretty_name': ['serious_var_1', 'my sweet dude', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'low_alarm': [0, -1.7, 3,"what's a number?",12.5],
                        'high_alarm': [1,2,None,None,76]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05',
                                     '2022-01-02 01:01','2022-01-02 01:02','2022-01-02 01:03','2022-01-02 01:04','2022-01-02 01:05',
                                     '2022-01-02 01:06','2022-01-02 01:07','2022-01-02 01:08','2022-01-02 01:09','2022-01-02 01:10',
                                     '2022-01-03 01:01','2022-01-03 01:02','2022-01-03 01:03','2022-01-03 01:04','2022-01-03 01:05',])
        df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), 
                                             -3, -6, -3, -6, 0,
                                             -3, -6, -3, -6, -3,
                                             90, 82, 5, 7, 0],
                        'serious_var_2': [2, 90, 80, 90, 80, 
                                          90, 90, 2, 7, -2,
                                          -2, -2, 3, 7, 7,
                                          4,4,4,4,4],})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02', '2022-01-02', '2022-01-02'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM','SILENT_ALARM','SILENT_ALARM','SILENT_ALARM','SILENT_ALARM'],
                        'event_detail': ["Upper bound alarm for serious_var_1 (longest at 01:01 for 5 minutes). Avg fault time : 5.0 minutes, Avg value during fault: inf",
                                         "Upper bound alarm for my sweet dude (longest at 01:02 for 4 minutes). Avg fault time : 4.0 minutes, Avg value during fault: 85.0",
                                         "Lower bound alarm for serious_var_1 (longest at 01:06 for 5 minutes). Avg fault time : 4.5 minutes, Avg value during fault: -4.2",
                                         "Lower bound alarm for my sweet dude (longest at 01:05 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: -2.0",
                                         "Upper bound alarm for my sweet dude (longest at 01:08 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: 7.0"],
                        'variable_name' : ['serious_var_1','serious_var_2','serious_var_1','serious_var_2','serious_var_2']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3, full_days=pd.to_datetime(['2022-01-01', '2022-01-02']))

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)
