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

        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_with_days(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'pretty_name': [None, 'my sweet dude', 'serious_var_3', 'serious_var_4','serious_var_5'],
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

@patch('ecopipeline.ConfigManager')
def test_flag_ratio_alarms(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'alarm_codes': ['PR_HPWH:60-80', None, "PR_HPWH:20-40;PR_Other:0-100",None,"PR_Other:20-50"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
                        'serious_var_2': [2, 2, 90],
                        'serious_var_3': [100, 40, 7],
                        'serious_var_4': [4, 2, 2]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-03','2022-01-03','2022-01-03','2022-01-01','2022-01-01','2022-01-02'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM']*6,
                        'event_detail': [
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_1 accounted for 94.49% of HPWH energy use. 60.0-80.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_3 accounted for 5.51% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious_var_3 accounted for 0.0% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-02): serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected."
                                        ],
                        'variable_name' : ['serious_var_1','serious_var_3','serious_var_5','serious_var_3','serious_var_5','serious_var_5']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_flag_ratio_alarms_ignore_other_alarm_types(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5'],
                        'alarm_codes': ['TZ_HPWH:60-80', None, "PR_HPWH:0-100;PR_Other:0-100",None,"PR_Other:20-50"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
                        'serious_var_2': [2, 2, 90],
                        'serious_var_3': [100, 40, 7],
                        'serious_var_4': [4, 2, 2]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM']*3,
                        'event_detail': [
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-02): serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected."
                                        ],
                        'variable_name' : ['serious_var_5','serious_var_5','serious_var_5']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_flag_abnormal_COP(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['COP_Boundary', 'COP_Equipment', 'SystemCOP', 'serious_var_4','serious_var_5'],
                        'high_alarm': [5, None, 3,None,45]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'COP_Equipment': [float('inf'), 4, 120],
                        'serious_var_2': [2, 2, 90],
                        'SystemCOP': [4, 2.4, 2.7],
                        'COP_Boundary': [4.8, 2, -1]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-03', '2022-01-03', '2022-01-01', '2022-01-01'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM']*4,
                        'event_detail': [
                                        "Unexpected COP Value detected: COP_Boundary = -1.0",
                                        "Unexpected COP Value detected: COP_Equipment = 120.0",
                                        "Unexpected COP Value detected: COP_Equipment = inf",
                                        "Unexpected COP Value detected: SystemCOP = 4.0"
                                        ],
                        'variable_name' : [
                            'COP_Boundary',
                            'COP_Equipment',
                            'COP_Equipment',
                            'SystemCOP'
                            ]})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = flag_abnormal_COP(df, mock_config_manager)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_central_alarm_function(mock_config_manager, mocker):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_table_name.return_value = "fake_daily_table"
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    # Set the desired response for cursor.execute
    cursor_mock.fetchall.side_effect = [
        []
    ]
    mock_config_manager.connect_db.side_effect = [(con_mock,cursor_mock)]
    with patch('pandas.read_csv') as mock_csv:

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05',
                                     '2022-01-02 01:01','2022-01-02 01:02','2022-01-02 01:03','2022-01-02 01:04','2022-01-02 01:05',
                                     '2022-01-02 01:06','2022-01-02 01:07','2022-01-02 01:08','2022-01-02 01:09','2022-01-02 01:10',
                                     '2022-01-03 01:01','2022-01-03 01:02'])
        minute_df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), 
                                             -3, -6, -3, -6, 0,
                                             -3, -6, -3, -6, -3,
                                             90, 82],
                        'serious_var_2': [2, 90, 80, 90, 80, 
                                          90, 90, 2, 7, -2,
                                          -2, -2, 3, 7, 7,
                                          4,4],})
        minute_df.index = minute_timestamps

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({
                        'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh','cop'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5','COP_Boundary'],
                        'pretty_name': [None, 'my sweet dude', 'serious_var_3', 'serious_var_4','serious var 5','System COP (Boundary Method)'],
                        'low_alarm': [0, -1.7, 3,"what's a number?",12.5,None],
                        'high_alarm': [1,2,None,None,76,None],
                        'fault_time': [3,3,3,None,3,None],
                        'alarm_codes': ['PR_HPWH:60-80', None, "PR_HPWH:20-40;PR_Other:0-100",None,"PR_Other:20-50",None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        daily_df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
                        'serious_var_2': [2, 2, 90],
                        'serious_var_3': [100, 40, 7],
                        'serious_var_4': [4, 2, 2],
                        'COP_Boundary': [4.6, 2.7, -1]})
        daily_df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02', '2022-01-02', '2022-01-02','2022-01-03','2022-01-03','2022-01-03','2022-01-01','2022-01-01','2022-01-02',
                                         '2022-01-01','2022-01-03'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM']*13,
                        'event_detail': [
                                        "Upper bound alarm for serious_var_1 (longest at 01:01 for 5 minutes). Avg fault time : 5.0 minutes, Avg value during fault: inf",
                                        "Upper bound alarm for my sweet dude (longest at 01:02 for 4 minutes). Avg fault time : 4.0 minutes, Avg value during fault: 85.0",
                                        "Lower bound alarm for serious_var_1 (longest at 01:06 for 5 minutes). Avg fault time : 4.5 minutes, Avg value during fault: -4.2",
                                        "Lower bound alarm for my sweet dude (longest at 01:05 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: -2.0",
                                        "Upper bound alarm for my sweet dude (longest at 01:08 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: 7.0",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_1 accounted for 94.49% of HPWH energy use. 60.0-80.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_3 accounted for 5.51% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious_var_3 accounted for 0.0% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-02): serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Unexpected COP Value detected: System COP (Boundary Method) = 4.6",
                                        "Unexpected COP Value detected: System COP (Boundary Method) = -1.0"
                                        ],
                        'variable_name' : ['serious_var_1','serious_var_2','serious_var_1','serious_var_2','serious_var_2',
                                           'serious_var_1','serious_var_3','serious_var_5','serious_var_3','serious_var_5','serious_var_5',
                                           "COP_Boundary","COP_Boundary"]})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = central_alarm_df_creator(minute_df, daily_df, mock_config_manager, power_ratio_period_days=1)

        # Assert that mysql.connector.connect() was called
        # mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)


@patch('ecopipeline.ConfigManager')
def test_central_alarm_function_with_ongoing_cop_data_loss(mock_config_manager, mocker):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_table_name.return_value = "fake_daily_table"
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    # Patch the cursor.execute method with the mock
    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')

    # Set the desired response for cursor.execute
    cursor_mock.fetchall.side_effect = [
        [(1,)]
    ]
    mock_config_manager.connect_db.side_effect = [(con_mock,cursor_mock)]
    with patch('pandas.read_csv') as mock_csv:

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05',
                                     '2022-01-02 01:01','2022-01-02 01:02','2022-01-02 01:03','2022-01-02 01:04','2022-01-02 01:05',
                                     '2022-01-02 01:06','2022-01-02 01:07','2022-01-02 01:08','2022-01-02 01:09','2022-01-02 01:10',
                                     '2022-01-03 01:01','2022-01-03 01:02'])
        minute_df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'), 
                                             -3, -6, -3, -6, 0,
                                             -3, -6, -3, -6, -3,
                                             90, 82],
                        'serious_var_2': [2, 90, 80, 90, 80, 
                                          90, 90, 2, 7, -2,
                                          -2, -2, 3, 7, 7,
                                          4,4],})
        minute_df.index = minute_timestamps

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({
                        'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh','cop'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5','COP_Boundary'],
                        'pretty_name': [None, 'my sweet dude', 'serious_var_3', 'serious_var_4','serious var 5','System COP (Boundary Method)'],
                        'low_alarm': [0, -1.7, 3,"what's a number?",12.5,None],
                        'high_alarm': [1,2,None,None,76,None],
                        'fault_time': [3,3,3,None,3,None],
                        'alarm_codes': ['PR_HPWH:60-80', None, "PR_HPWH:20-40;PR_Other:0-100",None,"PR_Other:20-50",None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        daily_df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
                        'serious_var_2': [2, 2, 90],
                        'serious_var_3': [100, 40, 7],
                        'serious_var_4': [4, 2, 2],
                        'COP_Boundary': [4.6, 2.7, -1]})
        daily_df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-01', '2022-01-02', '2022-01-02', '2022-01-02','2022-01-03','2022-01-03','2022-01-03','2022-01-01','2022-01-01','2022-01-02'
                                    ])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['SILENT_ALARM']*11,
                        'event_detail': [
                                        "Upper bound alarm for serious_var_1 (longest at 01:01 for 5 minutes). Avg fault time : 5.0 minutes, Avg value during fault: inf",
                                        "Upper bound alarm for my sweet dude (longest at 01:02 for 4 minutes). Avg fault time : 4.0 minutes, Avg value during fault: 85.0",
                                        "Lower bound alarm for serious_var_1 (longest at 01:06 for 5 minutes). Avg fault time : 4.5 minutes, Avg value during fault: -4.2",
                                        "Lower bound alarm for my sweet dude (longest at 01:05 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: -2.0",
                                        "Upper bound alarm for my sweet dude (longest at 01:08 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: 7.0",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_1 accounted for 94.49% of HPWH energy use. 60.0-80.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious_var_3 accounted for 5.51% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious_var_3 accounted for 0.0% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-02): serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected."
                                        ],
                        'variable_name' : ['serious_var_1','serious_var_2','serious_var_1','serious_var_2','serious_var_2',
                                           'serious_var_1','serious_var_3','serious_var_5','serious_var_3','serious_var_5','serious_var_5']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = central_alarm_df_creator(minute_df, daily_df, mock_config_manager, power_ratio_period_days=1)

        # Assert that mysql.connector.connect() was called
        # mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)
        
@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_all_null_values(mock_config_manager):
    """Test that flag_boundary_alarms returns empty dataframe when all values in a column are null"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name'],
                        'variable_name': ['serious_var_1', 'serious_var_2'],
                        'low_alarm': [0, None],
                        'high_alarm': [1, 2]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        df = pd.DataFrame({'serious_var_1': [None, None, None],
                        'serious_var_2': [None, None, None]})
        df.index = timestamps

        # Should return empty dataframe since all values are null
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_all_nan_values(mock_config_manager):
    """Test that flag_boundary_alarms returns empty dataframe when all values in a column are NaN"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name'],
                        'variable_name': ['serious_var_1', 'serious_var_2'],
                        'low_alarm': [0, -10],
                        'high_alarm': [100, 50]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                     '2022-01-01 01:04','2022-01-01 01:05'])
        df = pd.DataFrame({'serious_var_1': [np.nan, np.nan, np.nan, np.nan, np.nan],
                        'serious_var_2': [np.nan, np.nan, np.nan, np.nan, np.nan]})
        df.index = timestamps

        # Should return empty dataframe since all values are NaN
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_power_ratio_alarm_all_null_values(mock_config_manager):
    """Test that power_ratio_alarm returns empty dataframe when all energy values are null"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3'],
                        'alarm_codes': ['PR_HPWH:60-80', None, "PR_HPWH:20-40"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [None, None, None],
                        'serious_var_2': [None, None, None],
                        'serious_var_3': [None, None, None]})
        df.index = timestamps

        # Should return empty dataframe since all energy values are null (division by zero protection)
        event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)

        # Empty or all NaN ratios should not trigger alarms
        assert event_df.empty or len(event_df) == 0

@patch('ecopipeline.ConfigManager')
def test_power_ratio_alarm_all_zero_values(mock_config_manager):
    """Test that power_ratio_alarm handles all zero energy values gracefully (no division by zero)"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3'],
                        'alarm_codes': ['PR_HPWH:60-80', None, "PR_HPWH:20-40"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [0, 0, 0],
                        'serious_var_2': [0, 0, 0],
                        'serious_var_3': [0, 0, 0]})
        df.index = timestamps

        # Should not raise division by zero error and return empty or acceptable result
        try:
            event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)
            # If it returns a result, it should be a valid dataframe
            assert isinstance(event_df, pd.DataFrame)
        except Exception as e:
            pytest.fail(f"power_ratio_alarm raised an exception with all zero values: {e}")

@patch('ecopipeline.ConfigManager')
def test_flag_abnormal_COP_all_null_values(mock_config_manager):
    """Test that flag_abnormal_COP returns empty dataframe when all COP values are null"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['cop1', 'cop2', 'cop3'],
                        'variable_name': ['COP_Boundary', 'COP_Equipment', 'SystemCOP'],
                        'high_alarm': [5, None, 3]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'COP_Equipment': [None, None, None],
                        'SystemCOP': [None, None, None],
                        'COP_Boundary': [None, None, None]})
        df.index = timestamps

        # Should return empty dataframe since all COP values are null
        event_df = flag_abnormal_COP(df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_abnormal_COP_all_nan_values(mock_config_manager):
    """Test that flag_abnormal_COP returns empty dataframe when all COP values are NaN"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['cop1', 'cop2', 'cop3'],
                        'variable_name': ['COP_Boundary', 'COP_Equipment', 'SystemCOP'],
                        'high_alarm': [5, 4.5, 3]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'COP_Equipment': [np.nan, np.nan, np.nan],
                        'SystemCOP': [np.nan, np.nan, np.nan],
                        'COP_Boundary': [np.nan, np.nan, np.nan]})
        df.index = timestamps

        # Should return empty dataframe since all COP values are NaN
        event_df = flag_abnormal_COP(df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_mixed_null_and_valid(mock_config_manager):
    """Test that flag_boundary_alarms handles mixed null and valid values correctly"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G'],
                        'variable_name': ['serious_var_1'],
                        'low_alarm': [0],
                        'high_alarm': [1]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                     '2022-01-01 01:04','2022-01-01 01:05'])
        df = pd.DataFrame({'serious_var_1': [None, None, 100, 100, 100]})
        df.index = timestamps

        # Should detect alarm for the three consecutive high values
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Should have one alarm for upper bound
        assert len(event_df) == 1
        assert 'Upper bound alarm' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_empty_dataframe(mock_config_manager):
    """Test that flag_boundary_alarms handles empty dataframe gracefully"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G'],
                        'variable_name': ['serious_var_1'],
                        'low_alarm': [0],
                        'high_alarm': [1]})
        mock_csv.return_value = csv_df

        # Empty dataframe
        df = pd.DataFrame({'serious_var_1': []})

        # Should return empty dataframe without errors
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_all_null_low_alarm(mock_config_manager):
    """Test that flag_boundary_alarms handles all null low_alarm values"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name'],
                        'variable_name': ['serious_var_1', 'serious_var_2'],
                        'low_alarm': [None, None],
                        'high_alarm': [100, 50]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                     '2022-01-01 01:04','2022-01-01 01:05'])
        df = pd.DataFrame({'serious_var_1': [150, 150, 150, 150, 150],
                        'serious_var_2': [60, 60, 60, 60, 60]})
        df.index = timestamps

        # Should still detect upper bound alarms even if low_alarm is null
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Should have alarms for upper bound violations
        assert len(event_df) == 2

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_all_null_high_alarm(mock_config_manager):
    """Test that flag_boundary_alarms handles all null high_alarm values"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name'],
                        'variable_name': ['serious_var_1', 'serious_var_2'],
                        'low_alarm': [10, 5],
                        'high_alarm': [None, None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                     '2022-01-01 01:04','2022-01-01 01:05'])
        df = pd.DataFrame({'serious_var_1': [1, 1, 1, 1, 1],
                        'serious_var_2': [2, 2, 2, 2, 2]})
        df.index = timestamps

        # Should still detect lower bound alarms even if high_alarm is null
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Should have alarms for lower bound violations
        assert len(event_df) == 2

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_all_null_both_alarms(mock_config_manager):
    """Test that flag_boundary_alarms handles all null for both low_alarm and high_alarm"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name'],
                        'variable_name': ['serious_var_1', 'serious_var_2'],
                        'low_alarm': [None, None],
                        'high_alarm': [None, None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                     '2022-01-01 01:04','2022-01-01 01:05'])
        df = pd.DataFrame({'serious_var_1': [100, 100, 100, 100, 100],
                        'serious_var_2': [50, 50, 50, 50, 50]})
        df.index = timestamps

        # Should return empty dataframe when no alarms are configured
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_boundary_alarms_all_null_fault_time(mock_config_manager):
    """Test that flag_boundary_alarms uses default_fault_time when fault_time column is all null"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G'],
                        'variable_name': ['serious_var_1'],
                        'low_alarm': [0],
                        'high_alarm': [10],
                        'fault_time': [None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                     '2022-01-01 01:04','2022-01-01 01:05'])
        df = pd.DataFrame({'serious_var_1': [100, 100, 100, 100, 100]})
        df.index = timestamps

        # Should use default_fault_time when fault_time is null
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Should detect alarm using default fault time
        assert len(event_df) == 1

@patch('ecopipeline.ConfigManager')
def test_power_ratio_alarm_all_null_alarm_codes(mock_config_manager):
    """Test that power_ratio_alarm returns empty when all alarm_codes are null"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3'],
                        'alarm_codes': [None, None, None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [100, 60, 120],
                        'serious_var_2': [50, 40, 90],
                        'serious_var_3': [100, 40, 7]})
        df.index = timestamps

        # Should return empty dataframe when no alarm codes are configured
        event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_power_ratio_alarm_all_nan_alarm_codes(mock_config_manager):
    """Test that power_ratio_alarm returns empty when all alarm_codes are NaN"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3'],
                        'alarm_codes': [np.nan, np.nan, np.nan]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [100, 60, 120],
                        'serious_var_2': [50, 40, 90],
                        'serious_var_3': [100, 40, 7]})
        df.index = timestamps

        # Should return empty dataframe when no alarm codes are configured
        event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_abnormal_COP_all_null_high_alarm(mock_config_manager):
    """Test that flag_abnormal_COP uses default bounds when high_alarm column is all null"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({'variable_alias': ['cop1', 'cop2'],
                        'variable_name': ['COP_Boundary', 'COP_Equipment'],
                        'high_alarm': [None, None]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'COP_Equipment': [5.0, 4, 3],
                        'COP_Boundary': [4.8, 2.7, 3.5]})
        df.index = timestamps

        # Should use default_high_bound (4.5) when high_alarm is null
        event_df = flag_abnormal_COP(df, mock_config_manager, default_high_bound=4.5)

        # Should detect alarms for values exceeding default bound
        assert len(event_df) == 2  # COP_Equipment at 5.0 and COP_Boundary at 4.8
