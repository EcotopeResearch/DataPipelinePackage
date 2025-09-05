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
                                        "Power ratio alarm: serious_var_1 accounted for 94.49% of HPWH energy use. 60.0-80.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious_var_3 accounted for 5.51% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious_var_3 accounted for 0.0% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected."
                                        ],
                        'variable_name' : ['serious_var_1','serious_var_3','serious_var_5','serious_var_3','serious_var_5','serious_var_5']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = power_ratio_alarm(df, mock_config_manager)

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
                                        "Power ratio alarm: serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious_var_5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected."
                                        ],
                        'variable_name' : ['serious_var_5','serious_var_5','serious_var_5']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = power_ratio_alarm(df, mock_config_manager)

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
        
        event_time_pts = pd.to_datetime(['2022-01-03', '2022-01-01'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts,
                        'event_type': ['DATA_LOSS_COP']*2,
                        'event_detail': [
                                        "Unexpected COP Value(s) detected: COP_Boundary = -1.0, COP_Equipment = 120.0",
                                        "Unexpected COP Value(s) detected: COP_Equipment = inf, SystemCOP = 4.0"
                                        ],
                        'variable_name' : [None,None]})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = flag_abnormal_COP(df, mock_config_manager)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_central_alarm_function(mock_config_manager, mocker):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
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
                        'event_type': ['SILENT_ALARM']*11 + ['DATA_LOSS_COP']*2,
                        'event_detail': [
                                        "Upper bound alarm for serious_var_1 (longest at 01:01 for 5 minutes). Avg fault time : 5.0 minutes, Avg value during fault: inf",
                                        "Upper bound alarm for my sweet dude (longest at 01:02 for 4 minutes). Avg fault time : 4.0 minutes, Avg value during fault: 85.0",
                                        "Lower bound alarm for serious_var_1 (longest at 01:06 for 5 minutes). Avg fault time : 4.5 minutes, Avg value during fault: -4.2",
                                        "Lower bound alarm for my sweet dude (longest at 01:05 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: -2.0",
                                        "Upper bound alarm for my sweet dude (longest at 01:08 for 3 minutes). Avg fault time : 3.0 minutes, Avg value during fault: 7.0",
                                        "Power ratio alarm: serious_var_1 accounted for 94.49% of HPWH energy use. 60.0-80.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious_var_3 accounted for 5.51% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious_var_3 accounted for 0.0% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Unexpected COP Value(s) detected: System COP (Boundary Method) = 4.6",
                                        "Unexpected COP Value(s) detected: System COP (Boundary Method) = -1.0"
                                        ],
                        'variable_name' : ['serious_var_1','serious_var_2','serious_var_1','serious_var_2','serious_var_2',
                                           'serious_var_1','serious_var_3','serious_var_5','serious_var_3','serious_var_5','serious_var_5',
                                           None,None]})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = central_alarm_df_creator(minute_df, daily_df, mock_config_manager)

        # Assert that mysql.connector.connect() was called
        # mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)


@patch('ecopipeline.ConfigManager')
def test_central_alarm_function_with_ongoing_cop_data_loss(mock_config_manager, mocker):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
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
                                        "Power ratio alarm: serious_var_1 accounted for 94.49% of HPWH energy use. 60.0-80.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious_var_3 accounted for 5.51% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious_var_3 accounted for 0.0% of HPWH energy use. 20.0-40.0% of HPWH energy use expected.",
                                        "Power ratio alarm: serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected.",
                                        "Power ratio alarm: serious var 5 accounted for 0.0% of Other energy use. 20.0-50.0% of Other energy use expected."
                                        ],
                        'variable_name' : ['serious_var_1','serious_var_2','serious_var_1','serious_var_2','serious_var_2',
                                           'serious_var_1','serious_var_3','serious_var_5','serious_var_3','serious_var_5','serious_var_5']})
        df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = central_alarm_df_creator(minute_df, daily_df, mock_config_manager)

        # Assert that mysql.connector.connect() was called
        # mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)