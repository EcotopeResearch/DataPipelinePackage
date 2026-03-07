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

        # Call the function that uses mysql.connector.connect()
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')

        # Verify alarm count and content
        assert len(event_df) == 2
        assert 'serious_var_1' in event_df['variable_name'].values
        assert 'serious_var_3' in event_df['variable_name'].values
        assert all(event_df['alarm_type'] == 'BOUNDARY')
        assert all('Boundary alarm' in detail for detail in event_df['event_detail'])

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
        
        # Call the function that uses mysql.connector.connect()
        event_df = flag_boundary_alarms(df, mock_config_manager, default_fault_time=3)

        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')

        # Verify alarm count and content
        assert len(event_df) == 3
        assert 'serious_var_1' in event_df['variable_name'].values
        assert list(event_df['variable_name'].values).count('serious_var_2') == 2
        assert all(event_df['alarm_type'] == 'BOUNDARY')
        assert all('Boundary alarm' in detail for detail in event_df['event_detail'])

@patch('ecopipeline.ConfigManager')
def test_flag_ratio_alarms(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['PowerIn_HPWH1', 'Temp_whatever', 'PowerIn_HPWH2','PowerIn_Total','PowerIn_SwingTank'],
                        'alarm_codes': ['POWRRAT:60-80', None, "POWRRAT:20-40","POWRRAT:0-100","POWRRAT:20-50"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'PowerIn_HPWH1': [float('inf'), 60, 120],
                        'PowerIn_HPWH2': [2, 40, 7],
                        'PowerIn_Total': [float('inf'), 100, 1000],
                        'PowerIn_SwingTank': [4, 20, 500]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01','2022-01-03','2022-01-01','2022-01-03'])
        end_event_time_pts = pd.to_datetime(['2022-01-02','2022-01-04','2022-01-02','2022-01-04'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': end_event_time_pts,
                        'alarm_type': ['POWER_RATIO']*4,
                        'event_detail': [
                                        "Power ratio alarm (1-day block ending 2022-01-01): PowerIn_HPWH2 accounted for 0.0% of HPWH energy use. 20.0-40.0% expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): PowerIn_HPWH2 accounted for 5.5% of HPWH energy use. 20.0-40.0% expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-01): PowerIn_SwingTank accounted for 0.0% of Total energy use. 20.0-50.0% expected.",
                                        "Power ratio alarm (1-day block ending 2022-01-03): PowerIn_HPWH1 accounted for 94.5% of HPWH energy use. 60.0-80.0% expected."
                                        ],
                        'variable_name' : ['PowerIn_HPWH2','PowerIn_HPWH2','PowerIn_SwingTank','PowerIn_HPWH1'],
                        'certainty': [3]*4})
        # df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = power_ratio_alarm(df, mock_config_manager, "fake_table", ratio_period_days=1)
        df_expected.sort_values('start_time_pt').reset_index(drop=True)

        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

@patch('ecopipeline.ConfigManager')
def test_flag_ratio_alarms_ignore_other_alarm_types(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        csv_df = pd.DataFrame({'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'PowerIn_Total', 'serious_var_4','PowerIn_Whatever'],
                        'alarm_codes': ['TZ_HPWH:60-80', None, "POWRRAT:0-100",None,"POWRRAT:20-50"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
                        'serious_var_2': [2, 2, 90],
                        'PowerIn_Total': [100, 40, 7],
                        'serious_var_4': [4, 2, 2]})
        df.index = timestamps
        
        event_time_pts = pd.to_datetime(['2022-01-01 00:00'])
        event_time_pts_end = pd.to_datetime(['2022-01-04 00:00'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts_end,
                        'alarm_type': ['POWER_RATIO'],
                        'event_detail': [
                                        "Power ratio alarm (1-day block ending 2022-01-01): PowerIn_Whatever accounted for 0.0% of Total energy use. 20.0-50.0% expected."
                                        ],
                        'variable_name' : ['PowerIn_Whatever'],
                        'certainty': [3]})
        # df_expected.set_index('start_time_pt', inplace=True)

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
        
        event_time_pts = pd.to_datetime(['2022-01-01', '2022-01-03', '2022-01-01', '2022-01-03'])
        event_time_pts_end = pd.to_datetime(['2022-01-02', '2022-01-04', '2022-01-02', '2022-01-04'])
        df_expected = pd.DataFrame({
                        'start_time_pt': event_time_pts,
                        'end_time_pt': event_time_pts_end,
                        'alarm_type': ['ABNORMAL_COP']*4,
                        'event_detail': [
                                        "Unexpected COP Value detected: COP_Equipment = inf",
                                        "Unexpected COP Value detected: COP_Equipment = 120.0",
                                        "Unexpected COP Value detected: SystemCOP = 4.0",
                                        "Unexpected COP Value detected: COP_Boundary = -1.0"
                                        ],
                        'variable_name' : [
                            'COP_Equipment',
                            'COP_Equipment',
                            'SystemCOP',
                            'COP_Boundary'
                            ],
                        'certainty': [3]*4})
        # df_expected.set_index('start_time_pt', inplace=True)

        # Call the function that uses mysql.connector.connect()
        event_df = flag_abnormal_COP(df, mock_config_manager)
        # df_expected = df_expected.sort_values('start_time_pt').reset_index(drop=True)
        # Assert that mysql.connector.connect() was called
        mock_csv._once_with('fake/path/whatever/Variable_Names.csv')
        assert_frame_equal(event_df, df_expected)

# # @patch('ecopipeline.ConfigManager')
# # def test_central_alarm_function(mock_config_manager, mocker):
# #     mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
# #     mock_config_manager.get_table_name.return_value = "fake_daily_table"
# #     cursor_mock = MagicMock()
# #     con_mock = MagicMock()

# #     # Patch the cursor.execute method with the mock
# #     mocker.patch.object(cursor_mock, 'execute')
# #     mocker.patch.object(con_mock, 'close')
# #     mocker.patch.object(con_mock, 'commit')

# #     # Set the desired response for cursor.execute
# #     cursor_mock.fetchall.side_effect = [
# #         []
# #     ]
# #     mock_config_manager.connect_db.side_effect = [(con_mock,cursor_mock)]
# #     with patch('pandas.read_csv') as mock_csv:

# #         minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05',
# #                                      '2022-01-02 01:01','2022-01-02 01:02','2022-01-02 01:03','2022-01-02 01:04','2022-01-02 01:05',
# #                                      '2022-01-02 01:06','2022-01-02 01:07','2022-01-02 01:08','2022-01-02 01:09','2022-01-02 01:10',
# #                                      '2022-01-03 01:01','2022-01-03 01:02'])
# #         minute_df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'),
# #                                              -3, -6, -3, -6, 0,
# #                                              -3, -6, -3, -6, -3,
# #                                              90, 82],
# #                         'serious_var_2': [2, 90, 80, 90, 80,
# #                                           90, 90, 2, 7, -2,
# #                                           -2, -2, 3, 7, 7,
# #                                           4,4],})
# #         minute_df.index = minute_timestamps

# #         # Set the desired response for mock_connect.return_value
# #         csv_df = pd.DataFrame({
# #                         'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh','cop'],
# #                         'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5','COP_Boundary'],
# #                         'pretty_name': [None, 'my sweet dude', 'serious_var_3', 'serious_var_4','serious var 5','System COP (Boundary Method)'],
# #                         'low_alarm': [0, -1.7, 3,"what's a number?",12.5,None],
# #                         'high_alarm': [1,2,None,None,76,None],
# #                         'fault_time': [3,3,3,None,3,None],
# #                         'alarm_codes': ['POWRRAT_HPWH:60-80', None, "POWRRAT_HPWH:20-40;POWRRAT_Other:0-100",None,"POWRRAT_Other:20-50",None]})
# #         mock_csv.return_value = csv_df

# #         timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
# #         daily_df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
# #                         'serious_var_2': [2, 2, 90],
# #                         'serious_var_3': [100, 40, 7],
# #                         'serious_var_4': [4, 2, 2],
# #                         'COP_Boundary': [4.6, 2.7, -1]})
# #         daily_df.index = timestamps

# #         # Call the function
# #         event_df = central_alarm_df_creator(minute_df, daily_df, mock_config_manager, power_ratio_period_days=1)

# #         # Check that all expected alarm types are present
# #         event_details = event_df['event_detail'].tolist()

# #         # Check boundary alarms
# #         assert any('Boundary alarm for serious_var_1' in d for d in event_details)
# #         assert any('Boundary for my sweet dude' in d for d in event_details)

# #         # Check power ratio alarms
# #         assert any('Power ratio alarm' in d and 'serious_var_1' in d for d in event_details)
# #         assert any('Power ratio alarm' in d and 'serious_var_3' in d for d in event_details)
# #         assert any('Power ratio alarm' in d and 'serious var 5' in d for d in event_details)

# #         # Check COP alarms
# #         assert any('Unexpected COP Value detected' in d and '4.6' in d for d in event_details)
# #         assert any('Unexpected COP Value detected' in d and '-1.0' in d for d in event_details)

# #         # Check all events are SILENT_ALARM type
# #         assert all(event_df['alarm_type'] == 'SILENT_ALARM')

# #         # Check that boundary alarms have proper start/end times (not just day)
# #         boundary_alarms = event_df[event_df['event_detail'].str.contains('bound alarm')]
# #         for idx in boundary_alarms.index:
# #             # Index should include time component, not just date
# #             assert idx.hour > 0 or idx.minute > 0


# # @patch('ecopipeline.ConfigManager')
# # def test_central_alarm_function_with_ongoing_cop_data_loss(mock_config_manager, mocker):
# #     mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
# #     mock_config_manager.get_table_name.return_value = "fake_daily_table"
# #     cursor_mock = MagicMock()
# #     con_mock = MagicMock()

# #     # Patch the cursor.execute method with the mock
# #     mocker.patch.object(cursor_mock, 'execute')
# #     mocker.patch.object(con_mock, 'close')
# #     mocker.patch.object(con_mock, 'commit')

# #     # Set the desired response for cursor.execute - simulates ongoing COP data loss
# #     cursor_mock.fetchall.side_effect = [
# #         [(1,)]
# #     ]
# #     mock_config_manager.connect_db.side_effect = [(con_mock,cursor_mock)]
# #     with patch('pandas.read_csv') as mock_csv:

# #         minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03','2022-01-01 01:04','2022-01-01 01:05',
# #                                      '2022-01-02 01:01','2022-01-02 01:02','2022-01-02 01:03','2022-01-02 01:04','2022-01-02 01:05',
# #                                      '2022-01-02 01:06','2022-01-02 01:07','2022-01-02 01:08','2022-01-02 01:09','2022-01-02 01:10',
# #                                      '2022-01-03 01:01','2022-01-03 01:02'])
# #         minute_df = pd.DataFrame({'serious_var_1': [float('inf'), float('inf'), float('inf'), float('inf'), float('inf'),
# #                                              -3, -6, -3, -6, 0,
# #                                              -3, -6, -3, -6, -3,
# #                                              90, 82],
# #                         'serious_var_2': [2, 90, 80, 90, 80,
# #                                           90, 90, 2, 7, -2,
# #                                           -2, -2, 3, 7, 7,
# #                                           4,4],})
# #         minute_df.index = minute_timestamps

# #         # Set the desired response for mock_connect.return_value
# #         csv_df = pd.DataFrame({
# #                         'variable_alias': ['0X53G', 'silly_name', 'silly_varriable', 'silly_strings','meh','cop'],
# #                         'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_3', 'serious_var_4','serious_var_5','COP_Boundary'],
# #                         'pretty_name': [None, 'my sweet dude', 'serious_var_3', 'serious_var_4','serious var 5','System COP (Boundary Method)'],
# #                         'low_alarm': [0, -1.7, 3,"what's a number?",12.5,None],
# #                         'high_alarm': [1,2,None,None,76,None],
# #                         'fault_time': [3,3,3,None,3,None],
# #                         'alarm_codes': ['POWRRAT_HPWH:60-80', None, "POWRRAT_HPWH:20-40;POWRRAT_Other:0-100",None,"POWRRAT_Other:20-50",None]})
# #         mock_csv.return_value = csv_df

# #         timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
# #         daily_df = pd.DataFrame({'serious_var_1': [float('inf'), 60, 120],
# #                         'serious_var_2': [2, 2, 90],
# #                         'serious_var_3': [100, 40, 7],
# #                         'serious_var_4': [4, 2, 2],
# #                         'COP_Boundary': [4.6, 2.7, -1]})
# #         daily_df.index = timestamps

# #         # Call the function
# #         event_df = central_alarm_df_creator(minute_df, daily_df, mock_config_manager, power_ratio_period_days=1)

# #         # Check that all expected alarm types are present
# #         event_details = event_df['event_detail'].tolist()

# #         # Check boundary alarms
# #         assert any('Boundary alarm for serious_var_1' in d for d in event_details)
# #         assert any('Boundary alarm for my sweet dude' in d for d in event_details)

# #         # Check power ratio alarms
# #         assert any('Power ratio alarm' in d and 'serious_var_1' in d for d in event_details)
# #         assert any('Power ratio alarm' in d and 'serious_var_3' in d for d in event_details)
# #         assert any('Power ratio alarm' in d and 'serious var 5' in d for d in event_details)

# #         # With ongoing COP data loss, COP alarms should NOT be present
# #         assert not any('Unexpected COP Value detected' in d for d in event_details)

# #         # Check all events are SILENT_ALARM type
# #         assert all(event_df['alarm_type'] == 'SILENT_ALARM')
        
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
                        'variable_name': ['PowerIn_HPWH1', 'serious_var_2', 'PowerIn_HPWH2'],
                        'alarm_codes': ['POWRRAT:60-80', None, "POWRRAT:20-40"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'PowerIn_HPWH1': [None, None, None],
                        'serious_var_2': [None, None, None],
                        'PowerIn_HPWH2': [None, None, None]})
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
                        'variable_name': ['PowerIn_HPWH1', 'serious_var_2', 'PowerIn_HPWH2'],
                        'alarm_codes': ['POWRRAT:60-80', None, "POWRRAT:20-40"]})
        mock_csv.return_value = csv_df

        timestamps = pd.to_datetime(['2022-01-01 00:00','2022-01-02 00:00','2022-01-03 00:00'])
        df = pd.DataFrame({'PowerIn_HPWH1': [0, 0, 0],
                        'serious_var_2': [0, 0, 0],
                        'PowerIn_HPWH2': [0, 0, 0]})
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
        assert 'Boundary alarm' in event_df.iloc[0]['event_detail']
        # start_time_pt should be the actual start time
        assert event_df.iloc[0]['start_time_pt'] == pd.Timestamp('2022-01-01 01:03')

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

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_no_TMNSTPT_codes(mock_config_manager):
    """Test that flag_high_tm_setpoint returns empty dataframe when no TMNSTPT alarm codes exist"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'var_2', 'var_3'],
            'alarm_codes': ['POWRRAT:60-80', None, 'OTHER_CODE:100']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({'PowerIn_HPWH': [100, 100, 100]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'PowerIn_HPWH': [1000]})
        daily_df.index = daily_timestamps

        # Should return empty dataframe when no TMNSTPT codes exist
        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_t_and_sp_alarm_triggered(mock_config_manager):
    """Test T and SP alarm when temperature is high while powered for 3+ minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Tank', 'PowerIn_HP'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                           '2022-01-01 01:04','2022-01-01 01:05'])
        minute_df = pd.DataFrame({
            'Temp_Tank': [135, 135, 135, 135, 135],
            'PowerIn_HP': [5, 5, 5, 5, 5]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Temp_Tank': [135], 'PowerIn_HP': [100]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_setpoint=130.0, default_power_indication=1.0)

        assert len(event_df) == 1
        assert 'High TM Setpoint' in event_df.iloc[0]['event_detail']
        assert 'PowerIn_HP' == event_df.iloc[0]['variable_name']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_t_and_sp_no_alarm(mock_config_manager):
    """Test T and SP with no alarm when conditions not met"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Tank', 'PowerIn_HP'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({
            'Temp_Tank': [120, 120, 120],  # Below setpoint
            'PowerIn_HP': [5, 5, 5]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Temp_Tank': [120], 'PowerIn_HP': [100]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_setpoint=130.0)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_st_setpoint_altered(mock_config_manager):
    """Test ST alarm when setpoint is altered for 10+ minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Setpoint_Tank'],
            'alarm_codes': ['TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                           '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06',
                                           '2022-01-01 01:07','2022-01-01 01:08','2022-01-01 01:09',
                                           '2022-01-01 01:10','2022-01-01 01:11','2022-01-01 01:12'])
        minute_df = pd.DataFrame({
            'Setpoint_Tank': [140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140]  # Altered from 130
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Setpoint_Tank': [140]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_setpoint=130.0)

        assert len(event_df) == 1
        assert 'Setpoint altered' in event_df.iloc[0]['event_detail']
        assert 'Setpoint_Tank' == event_df.iloc[0]['variable_name']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_st_no_alteration(mock_config_manager):
    """Test ST with no alarm when setpoint matches default"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Setpoint_Tank'],
            'alarm_codes': ['TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({
            'Setpoint_Tank': [130, 130, 130]  # Matches default
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Setpoint_Tank': [130]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_setpoint=130.0)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_tp_and_sp_high_ratio(mock_config_manager):
    """Test TP and SP alarm when power ratio exceeds threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_ER', 'PowerIn_Total'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02'])
        minute_df = pd.DataFrame({
            'PowerIn_ER': [100, 100],
            'PowerIn_Total': [150, 150]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_ER': [500],  # 50% of total
            'PowerIn_Total': [1000]
        })
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_power_ratio=0.4)  # 40% threshold

        assert len(event_df) == 1
        assert 'High temperature maintenance power ratio' in event_df.iloc[0]['event_detail']
        assert '40.0%' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_tp_and_sp_normal_ratio(mock_config_manager):
    """Test TP and SP with no alarm when power ratio is normal"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_ER', 'PowerIn_Total'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02'])
        minute_df = pd.DataFrame({
            'PowerIn_ER': [100, 100],
            'PowerIn_Total': [150, 150]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_ER': [300],  # 30% of total
            'PowerIn_Total': [1000]
        })
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_power_ratio=0.4)  # 40% threshold

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_multiple_TMNSTPT_codes(mock_config_manager):
    """Test multiple TMNSTPT codes separated by semicolons"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Tank', 'PowerIn_ER', 'other_var'],
            'alarm_codes': ['TMNSTPT','POWRRAT:60-80;TMNSTPT', 'OTHER_CODE:100']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({
            'Temp_Tank': [135, 135, 135],
            'PowerIn_ER': [5, 5, 5]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Temp_Tank': [135], 'PowerIn_ER': [100]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'High TM Setpoint' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_improper_format_no_underscore(mock_config_manager):
    """Test that improper TMNSTPT format (no underscore) raises exception"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['tank_temp'],
            'alarm_codes': ['TMNSTPT:T:1']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({'tank_temp': [135]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'tank_temp': [135]})
        daily_df.index = daily_timestamps

        with pytest.raises(Exception) as excinfo:
            flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        assert 'Improperly formated alarm code' in str(excinfo.value)

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_multiple_t_codes_same_id(mock_config_manager):
    """Test that multiple T codes with same ID raises exception"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Swing_Outlet', 'Temp_SwingOutlet', 'PowerIn_Swing'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT', 'TMNSTPT']  # Duplicate T with ID 1
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({'Temp_Swing_Outlet': [135], 'Temp_SwingOutlet': [135], 'PowerIn_Swing': [5]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Temp_Swing_Outlet': [135], 'Temp_SwingOutlet': [135], 'PowerIn_Swing': [100]})
        daily_df.index = daily_timestamps

        with pytest.raises(Exception) as excinfo:
            flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        assert 'Improper alarm codes for swing tank setpoint with id' in str(excinfo.value)

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_empty_dataframe(mock_config_manager):
    """Test that empty dataframe returns empty result"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Swing_Outlet'],
            'alarm_codes': ['TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_df = pd.DataFrame({'Temp_Swing_Outlet': []})
        daily_df = pd.DataFrame({'Temp_Swing_Outlet': []})

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_file_not_found(mock_config_manager):
    """Test that file not found returns empty dataframe"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/nonexistent.csv"
    with patch('pandas.read_csv') as mock_csv:
        mock_csv.side_effect = FileNotFoundError("File not found")

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({'tank_temp': [135]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'tank_temp': [135]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_multiple_days_multiple_alarms(mock_config_manager):
    """Test multiple days with multiple alarm types triggered"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Swing_Outlet', 'PowerIn_Swing', 'PowerIn_Total', 'Setpoint_swing'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT', 'TMNSTPT', 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-02 01:01','2022-01-02 01:02','2022-01-02 01:03',
            '2022-01-02 01:04','2022-01-02 01:05','2022-01-02 01:06',
            '2022-01-02 01:07','2022-01-02 01:08','2022-01-02 01:09',
            '2022-01-02 01:10','2022-01-02 01:11'
        ])
        minute_df = pd.DataFrame({
            'Temp_Swing_Outlet': [135, 135, 135, 120, 120, 120, 120, 120, 120, 120, 120, 135, 135, 135],
            'PowerIn_Swing': [5, 5, 5, 2, 2, 2, 2, 2, 2, 2, 2, 5, 5, 5],
            'PowerIn_Total': [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
            'Setpoint_swing': [130, 130, 130, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00', '2022-01-02 00:00'])
        daily_df = pd.DataFrame({
            'Temp_Swing_Outlet': [135, 120],
            'PowerIn_Swing': [500, 600],
            'PowerIn_Total': [1000, 1000],
            'Setpoint_swing': [130, 140]
        })
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_power_ratio=0.4)

        # Should have alarms: Day 1 T+SP alarm, Day 2 T+SP alarm, Day 2 ST alarm
        assert len(event_df) == 6
        assert any('High TM Setpoint' in detail for detail in event_df['event_detail'])

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_TMNSTPT_with_two_parts(mock_config_manager):
    """Test TMNSTPT code with only two parts (no ID)"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Swing_Outlet', 'PowerIn_Swing'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({
            'Temp_Swing_Outlet': [135, 135, 135],
            'PowerIn_Swing': [5, 5, 5]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Temp_Swing_Outlet': [135], 'PowerIn_Swing': [100]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        # Should process correctly with None as ID
        assert len(event_df) == 1

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_custom_t_bound(mock_config_manager):
    """Test T and SP alarm with custom temperature bound specified after colon"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_Swing_Outlet', 'PowerIn_Swing'],
            'alarm_codes': ['TMNSTPT:140', 'TMNSTPT:2.0']  # Custom bounds: 140°F temp, 2.0 kW power
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({
            'Temp_Swing_Outlet': [142, 142, 142],  # Above custom 140 threshold
            'PowerIn_Swing': [5, 5, 5]  # Above custom 2.0 threshold
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'tankTemp_Swing_Outlet_temp': [142], 'PowerIn_Swing': [100]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        # Should trigger alarm using custom bound of 140
        assert len(event_df) == 1
        assert '140.0' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_custom_st_bound(mock_config_manager):
    """Test ST alarm with custom setpoint bound specified after colon"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Setpoint_Swing'],
            'alarm_codes': ['TMNSTPT:125']  # Custom setpoint bound of 125
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
                                           '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06',
                                           '2022-01-01 01:07','2022-01-01 01:08','2022-01-01 01:09',
                                           '2022-01-01 01:10','2022-01-01 01:11','2022-01-01 01:12'])
        minute_df = pd.DataFrame({
            'Setpoint_Swing': [130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130]  # Not 125
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Setpoint_Swing': [130]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        # Should trigger alarm because setpoint is 130, not custom bound of 125
        assert len(event_df) == 1
        assert 'Setpoint altered' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_custom_tp_ratio_bound(mock_config_manager):
    """Test TP and SP alarm with custom power ratio bound specified after colon"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_Swing', 'PowerIn_Total'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT:0.3']  # Custom ratio bound of 30%
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02'])
        minute_df = pd.DataFrame({
            'PowerIn_Swing': [100, 100],
            'PowerIn_Total': [150, 150]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_Swing': [400],  # 40% of total
            'PowerIn_Total': [1000]
        })
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager)

        # Should trigger alarm because 40% > custom 30% threshold
        assert len(event_df) == 1
        assert 'High temperature maintenance power ratio' in event_df.iloc[0]['event_detail']
        assert '30.0%' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_high_tm_setpoint_no_custom_bound_uses_default(mock_config_manager):
    """Test that default bounds are used when no custom bound specified"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_SwingOutlet', 'PowerIn_Swing'],
            'alarm_codes': ['TMNSTPT', 'TMNSTPT']  # No custom bounds
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({
            'Temp_SwingOutlet': [132, 132, 132],  # Above default 130 but below 140
            'PowerIn_Swing': [5, 5, 5]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'Temp_SwingOutlet': [132], 'PowerIn_Swing': [100]})
        daily_df.index = daily_timestamps

        event_df = flag_high_tm_setpoint(minute_df, daily_df, mock_config_manager,
                                            default_setpoint=130.0, default_power_indication=1.0)

        # Should trigger alarm using default bound of 130
        assert len(event_df) == 1
        assert '130.0' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_no_BALVALV_codes(mock_config_manager):
    """Test that flag_recirc_balance_valve returns empty dataframe when no BV alarm codes exist"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['var_1', 'var_2', 'var_3'],
            'alarm_codes': ['POWRRAT:60-80', None, 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'var_1': [1000]})
        daily_df.index = daily_timestamps

        # Should return empty dataframe when no BV codes exist
        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_alarm_triggered(mock_config_manager):
    """Test ER and OUT alarm when recirculation sum exceeds heating output"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_SwingTank1', 'PowerIn_SwingTank2', 'HeatOut_TM'],
            'alarm_codes': ['BALVALV', 'BALVALV', 'BALVALV']
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_SwingTank1': [500],
            'PowerIn_SwingTank2': [500],
            'HeatOut_TM': [900]
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'Recirculation imbalance' in event_df.iloc[0]['event_detail']
        assert '1000.00 kW' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_no_alarm(mock_config_manager):
    """Test ER and OUT with no alarm when recirculation is below 95% of heating output"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_SwingTank1', 'PowerIn_SwingTank2', 'HeatOut_TM'],
            'alarm_codes': ['BALVALV', 'BALVALV', 'BALVALV']
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_SwingTank1': [200],
            'PowerIn_SwingTank2': [100],
            'HeatOut_TM': [1000]  # ER sum (300) <= 1000 * 0.95 (950)
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_tp_custom_bound(mock_config_manager):
    """Test BV alarm with TP (total power) and custom bound specified"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_SwingTank1', 'PowerIn_Total'],
            'alarm_codes': ['BALVALV', 'BALVALV:0.3']  # Custom 30% threshold
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_SwingTank1': [350],
            'PowerIn_Total': [1000]  # ER sum (350) >= 1000 * 0.3 (300)
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert '30.00%' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_tp_default_bound(mock_config_manager):
    """Test that default bound is used for TP when no custom bound specified"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_SwingTank1', 'PowerIn_Total'],
            'alarm_codes': ['BALVALV', 'BALVALV']  # No custom bound, uses default
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_SwingTank1': [450],
            'PowerIn_Total': [1000]  # ER sum (450) >= 1000 * 0.4 (400) with default 40%
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager,
                                              default_power_ratio=0.4)

        assert len(event_df) == 1
        assert '40.00%' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_multiple_days(mock_config_manager):
    """Test BV alarm with OUT across multiple days"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_SwingTank2', 'HeatOut_TM'],
            'alarm_codes': ['BALVALV', 'BALVALV']
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00', '2022-01-02 00:00', '2022-01-03 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_SwingTank2': [960, 300, 980],
            'HeatOut_TM': [800, 800, 800]  # Day 1 and 3 trigger
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert len(event_df) == 2
        assert all('Recirculation imbalance' in detail for detail in event_df['event_detail'])

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_multiple_out_codes(mock_config_manager):
    """Test that multiple OUT codes with same ID sums their values"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_SwingTank2', 'HeatOut_TM1', 'HeatOut_TM2'],
            'alarm_codes': ['BALVALV', 'BALVALV', 'BALVALV']  # Multiple OUT codes
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_SwingTank2': [1500],
            'HeatOut_TM1': [800],
            'HeatOut_TM2': [800]  # OUT sum = 1600, 1600 * 0.95 = 1520, ER (1500) <= 1520
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        # ER sum (1500) is not > OUT sum (1600) * 0.95 (1520), so no alarm
        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_no_er_codes_error(mock_config_manager):
    """Test that no ER codes raises exception"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['HeatOut_TM'],
            'alarm_codes': ['BALVALV']  # No ER codes
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'HeatOut_TM': [1000]
        })
        daily_df.index = daily_timestamps

        with pytest.raises(Exception) as excinfo:
            flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert 'Improper alarm codes for balancing valve' in str(excinfo.value)

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_empty_dataframe(mock_config_manager):
    """Test that empty dataframe returns empty result"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_ER', 'HeatOut_TM'],
            'alarm_codes': ['BALVALV', 'BALVALV']
        })
        mock_csv.return_value = csv_df

        daily_df = pd.DataFrame({'PowerIn_ER': [], 'HeatOut_TM': []})

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_file_not_found(mock_config_manager):
    """Test that file not found returns empty dataframe"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/nonexistent.csv"
    with patch('pandas.read_csv') as mock_csv:
        mock_csv.side_effect = FileNotFoundError("File not found")

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({'heating_output': [1000]})
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_recirc_balance_valve_multiple_er_codes(mock_config_manager):
    """Test BV alarm with multiple ER variables"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_ER1', 'PowerIn_ER2', 'PowerIn_ER3', 'HeatOut_TM'],
            'alarm_codes': ['BALVALV', 'BALVALV', 'BALVALV', 'BALVALV']
        })
        mock_csv.return_value = csv_df

        daily_timestamps = pd.to_datetime(['2022-01-01 00:00'])
        daily_df = pd.DataFrame({
            'PowerIn_ER1': [400],
            'PowerIn_ER2': [350],
            'PowerIn_ER3': [250],
            'HeatOut_TM': [600]
        })
        daily_df.index = daily_timestamps

        event_df = flag_recirc_balance_valve(daily_df, mock_config_manager)

        assert not event_df.empty
        assert len(event_df) == 1

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_no_HPINLET_codes(mock_config_manager):
    """Test that flag_hp_inlet_temp returns empty dataframe when no HPI alarm codes exist"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['var_1', 'var_2', 'var_3'],
            'alarm_codes': ['POWRRAT:60-80', None, 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({'var_1': [100, 100, 100]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_alarm_triggered(mock_config_manager):
    """Test HPI alarm when both power and temp exceed thresholds for fault_time minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHinlet'],
            'alarm_codes': ['HPINLET', 'HPINLET'],
            'pretty_name': ['HP Power', 'HP Inlet Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5, 1.5, 1.5, 1.5],  # Above 1.0
            'Temp_HPWHinlet': [120, 120, 120, 120, 120, 120]  # Above 115
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)  # Using defaults: 1.0, 115.0, 5

        assert len(event_df) == 1
        assert 'High heat pump inlet temperature' in event_df.iloc[0]['event_detail']
        assert 'HP Inlet Temperature' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_no_alarm_temp_low(mock_config_manager):
    """Test no alarm when temperature is below threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWH_inlet'],
            'alarm_codes': ['HPINLET', 'HPINLET']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5, 1.5, 1.5, 1.5],  # Above 1.0 threshold
            'Temp_HPWH_inlet': [110, 110, 110, 110, 110, 110]  # Below 115
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)  # Using defaults

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_no_alarm_power_low(mock_config_manager):
    """Test no alarm when power is below threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWH_Inlet'],
            'alarm_codes': ['HPINLET', 'HPINLET']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [0.5, 0.5, 0.5, 0.5, 0.5, 0.5],  # Below 1.0
            'Temp_HPWH_Inlet': [120, 120, 120, 120, 120, 120]  # Above 115 threshold
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)  # Using defaults

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_custom_bounds(mock_config_manager):
    """Test HPI alarm with custom bounds specified"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHInlet'],
            'alarm_codes': ['HPINLET:1.0', 'HPINLET:130.0']  # Custom thresholds
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06',
            '2022-01-01 01:07','2022-01-01 01:08','2022-01-01 01:09',
            '2022-01-01 01:10','2022-01-01 01:11'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5],  # Above 1.0
            'Temp_HPWHInlet': [135, 135, 135, 135, 135, 135, 135, 135, 135, 135, 135]  # Above 130
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager, fault_time=10)

        assert len(event_df) == 1
        assert '130.0' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_not_enough_consecutive_minutes(mock_config_manager):
    """Test no alarm when conditions met but not for enough consecutive minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHInlet'],
            'alarm_codes': ['HPINLET', 'HPINLET']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04'
        ])
        # Only 4 consecutive minutes, need 5
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5, 1.5],
            'Temp_HPWHInlet': [120, 120, 120, 120]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)  # Using defaults

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_multiple_pow_codes_error(mock_config_manager):
    """Test that multiple POW codes with same ID raises exception"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'PowerIn_HPWH', 'Temp_HPWHInlet'],
            'alarm_codes': ['HPINLET', 'HPINLET', 'HPINLET']  # Duplicate POW codes
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5],
            'PowerIn_HPWH': [1.5],
            'Temp_HPWHInlet': [125]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        with pytest.raises(Exception) as excinfo:
            flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)

        assert 'Improper alarm codes for HPWH Inlet alarm for element with id' in str(excinfo.value)

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_empty_dataframe(mock_config_manager):
    """Test that empty dataframe returns empty result"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHInlet'],
            'alarm_codes': ['HPINLET', 'HPINLET']
        })
        mock_csv.return_value = csv_df

        minute_df = pd.DataFrame({'PowerIn_HPWH': [], 'Temp_HPWHInlet': []})

        daily_df = pd.DataFrame({'dummy': []})

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_file_not_found(mock_config_manager):
    """Test that file not found returns empty dataframe"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/nonexistent.csv"
    with patch('pandas.read_csv') as mock_csv:
        mock_csv.side_effect = FileNotFoundError("File not found")

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({'hp_power': [1.5], 'hp_inlet_temp': [125]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_custom_fault_time(mock_config_manager):
    """Test HPI alarm with custom fault_time parameter"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHInlet'],
            'alarm_codes': ['HPINLET', 'HPINLET']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5],
            'Temp_HPWHInlet': [120, 120, 120]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        # Should trigger with fault_time=3 (custom, different from default 5)
        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager, fault_time=3)

        assert len(event_df) == 1

@patch('ecopipeline.ConfigManager')
def test_flag_hp_inlet_temp_intermittent_condition(mock_config_manager):
    """Test no alarm when condition is intermittent and not consecutive"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHInlet'],
            'alarm_codes': ['HPINLET', 'HPINLET']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06'
        ])
        # Power drops in the middle, breaking consecutive streak
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 0.5, 1.5, 1.5, 1.5],  # Drops below 1.0 at minute 3
            'Temp_HPWHInlet': [120, 120, 120, 120, 120, 120]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_inlet_temp(minute_df, daily_df, mock_config_manager)  # Using defaults

        # Should not trigger because consecutive condition broken at minute 3
        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_backup_use_no_IMBCKUP_codes(mock_config_manager):
    """Test that flag_backup_use returns empty dataframe when no BU alarm codes exist"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['var_1', 'var_2', 'var_3'],
            'alarm_codes': ['POWRRAT:60-80', None, 'TMNSTPT']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03'])
        minute_df = pd.DataFrame({'var_1': [100, 100, 100]})
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_backup_use(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_backup_use_st_alarm_triggered(mock_config_manager):
    """Test BU ST alarm when setpoint is altered for 10+ consecutive minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Setpoint_BU'],
            'alarm_codes': ['IMBCKUP']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06',
            '2022-01-01 01:07','2022-01-01 01:08','2022-01-01 01:09',
            '2022-01-01 01:10','2022-01-01 01:11','2022-01-01 01:12'
        ])
        # Setpoint altered from 130 to 140 for 12 consecutive minutes
        minute_df = pd.DataFrame({
            'Setpoint_BU': [140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_backup_use(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'was altered' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_backup_use_st_alarm_not_triggered(mock_config_manager):
    """Test no ST alarm when setpoint altered for less than 10 consecutive minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Setpoint_BU'],
            'alarm_codes': ['IMBCKUP:130']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05','2022-01-01 01:06',
            '2022-01-01 01:07','2022-01-01 01:08','2022-01-01 01:09'
        ])
        # Setpoint altered for only 9 minutes
        minute_df = pd.DataFrame({
            'Setpoint_BU': [140, 140, 140, 140, 140, 140, 140, 140, 140]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_backup_use(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_backup_use_tp_pow_alarm_triggered(mock_config_manager):
    """Test TP+POW alarm when backup power exceeds ratio of total power"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_BU1', 'PowerIn_Total'],
            'alarm_codes': ['IMBCKUP', 'IMBCKUP:0.1'],  # 10% ratio
            'pretty_name': ['Backup Power 1', 'Total Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({
            'PowerIn_BU1': [15.0],
            'PowerIn_Total': [100.0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({
            'PowerIn_BU1': [15.0],  # 15% of total power
            'PowerIn_Total': [100.0]
        })
        daily_df.index = daily_timestamps

        event_df = flag_backup_use(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'Improper Back Up Use' in event_df.iloc[0]['event_detail']
        assert '10.00%' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_blown_fuse_alarm_triggered(mock_config_manager):
    """Test blown fuse alarm when element is on but drawing unexpectedly low power"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_Elem'],
            'alarm_codes': ['BLWNFSE:30'],  # Expected 30 kW when on
            'pretty_name': ['Element Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_Elem': [5.0, 5.0, 5.0, 5.0, 5.0]  # On (>1.0) but below 28 (30-2)
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_blown_fuse(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'Blown Fuse' in event_df.iloc[0]['event_detail']
        assert 'PowerIn_Elem' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_blown_fuse_no_alarm_normal_power(mock_config_manager):
    """Test no alarm when element is drawing expected power"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_Elem'],
            'alarm_codes': ['BLWNFSE:30'],  # Expected 30 kW when on
            'pretty_name': ['Element Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_Elem': [30.0, 30.0, 30.0, 30.0, 30.0]  # On and at expected power
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_blown_fuse(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_blown_fuse_no_alarm_element_off(mock_config_manager):
    """Test no alarm when element is off (power below threshold)"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_Elem'],
            'alarm_codes': ['BLWNFSE:30'],
            'pretty_name': ['Element Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01','2022-01-01 01:02','2022-01-01 01:03',
            '2022-01-01 01:04','2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_Elem': [0.5, 0.5, 0.5, 0.5, 0.5]  # Below 1.0 threshold, element off
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_blown_fuse(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_alarm_on_turn_on(mock_config_manager):
    """Test alarm when HP turns on but temperature is far from ON threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_ls_df.return_value = pd.DataFrame()
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_tank_1', 'Temp_tank_2'],
            'alarm_codes': ['SOOCHNG', 'SOOCHNG_ON:115', 'SOOCHNG_OFF:140'],
            'pretty_name': ['HP Power', 'Tank Temperature', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [0.5, 0.5, 1.5, 1.5, 1.5],  # Turns on at 01:03
            'Temp_tank_1': [130.0, 130.0, 130.0, 130.0, 130.0],  # Far from 115 threshold
            'Temp_tank_2': [-5, 400.9, 20, 7.0, 5000]  # Should not matter
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'Unexpected SOO change' in event_df.iloc[0]['event_detail']
        assert 'HP turned on' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_no_alarm_temp_within_threshold(mock_config_manager):
    """Test no alarm when HP turns on and temperature is within 5.0 of ON threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_ls_df.return_value = pd.DataFrame()
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG', 'SOOCHNG_ON_normal:115;SOOCHNG_OFF_normal:140'],
            'pretty_name': ['HP Power', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [0.5, 0.5, 1.5, 1.5, 1.5],  # Turns on at 01:03
            'Temp_Tank': [116.0, 116.0, 116.0, 116.0, 116.0]  # Within 5.0 of 115
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_off_threshold(mock_config_manager):
    """Test no alarm when HP turns on and temperature is within 5.0 of ON threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_ls_df.return_value = pd.DataFrame()
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG', 'SOOCHNG_ON_normal:115;SOOCHNG_OFF_normal:140'],
            'pretty_name': ['HP Power', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5, 0.5, 0.5],  # Turns on at 01:03
            'Temp_Tank': [130.0, 131.0, 131.0, 131.0, 131.0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'Unexpected SOO change' in event_df.iloc[0]['event_detail']
        assert 'HP turned off' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_no_power_transition(mock_config_manager):
    """Test no alarm when HP power stays constant (no turn on/off)"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_ls_df.return_value = pd.DataFrame()
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG', 'SOOCHNG_ON_1:115;SOOCHNG_OFF_1:140'],
            'pretty_name': ['HP Power', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5, 1.5, 1.5, 1.5, 1.5],  # Already on, no transition
            'Temp_Tank': [130.0, 130.0, 130.0, 130.0, 130.0]  # Far from threshold but no transition
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_ls_alarm_type_only_during_event(mock_config_manager):
    """Test that when alarm_id is a load shifting type, only data during those events is analyzed"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'loadUp' event from 01:05 to 01:10
    ls_df = pd.DataFrame({
        'event': ['loadUp'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:05']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:10'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        # alarm_id is 'loadUp' (a load shifting type)
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG_POW', 'SOOCHNG_ON_loadUp:115', 'SOOCHNG_OFF_loadUp:140'],
            'pretty_name': ['HP Power', 'Tank Temperature', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',  # Outside load shift
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',  # 01:05 is start of loadUp
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'   # Inside loadUp
        ])
        minute_df = pd.DataFrame({
            # HP turns on at 01:03 (outside load shift) and again at 01:07 (inside load shift)
            'PowerIn_HPWH': [0.5, 0.5, 1.5, 1.5, 0.5, 0.5, 1.5, 1.5, 1.5],
            'Temp_Tank': [130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0]  # Far from 115
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        # Only the turn-on at 01:07 (inside loadUp event) should be analyzed and trigger alarm
        assert len(event_df) == 1
        assert '01:07' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_ls_alarm_type_no_matching_events(mock_config_manager):
    """Test that when alarm_id is a load shifting type but no matching events exist, no alarms trigger"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'shed' event (not 'loadUp')
    ls_df = pd.DataFrame({
        'event': ['shed'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:05']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:10'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        # alarm_id is 'loadUp' but ls_df only has 'shed' events
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG', 'SOOCHNG_ON_loadUp:115;SOOCHNG_OFF_loadUp:140'],
            'pretty_name': ['HP Power', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [0.5, 0.5, 1.5, 1.5, 1.5],  # Turns on at 01:03
            'Temp_Tank': [130.0, 130.0, 130.0, 130.0, 130.0]  # Far from 115, would trigger alarm
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        # No 'loadUp' events in ls_df, so no data to analyze, no alarms
        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_non_ls_type_excludes_ls_periods(mock_config_manager):
    """Test that when alarm_id is NOT a load shifting type, load shift periods are excluded"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'loadUp' event from 01:03 to 01:06
    ls_df = pd.DataFrame({
        'event': ['loadUp'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:03']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:06'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        # alarm_id is '1' (not a load shifting type)
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG_POW', 'SOOCHNG_ON_1:115;SOOCHNG_OFF_1:140'],
            'pretty_name': ['HP Power', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',  # 01:03 is start of loadUp
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',  # 01:06 is end of loadUp
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # HP turns on at 01:03 (inside load shift, should be ignored)
            # HP stays on through load shift and beyond
            'PowerIn_HPWH': [0.5, 0.5, 1.5, 1.5, 1.5, 0.5, 0.5, 0.5, 0.5],
            'Temp_Tank': [130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0]  # Far from 115
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        # The turn-on at 01:03 is during load shift, so it should be excluded
        # No other turn-ons outside load shift, so no alarms
        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_unexpected_soo_change_non_ls_type_alarm_outside_ls_period(mock_config_manager):
    """Test that when alarm_id is NOT a load shifting type, alarms outside load shift periods are detected"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'shed' event from 01:01 to 01:03
    ls_df = pd.DataFrame({
        'event': ['shed'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:01']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:03'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        # alarm_id is '1' (not a load shifting type)
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_Tank'],
            'alarm_codes': ['SOOCHNG_POW', 'SOOCHNG_ON_1:115;SOOCHNG_ON_shed:105;SOOCHNG_OFF_1:140;SOOCHNG_OFF_shed:110'],
            'pretty_name': ['HP Power', 'Tank Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',  # Inside load shift
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',  # Outside load shift
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # HP turns on at 01:05 (outside load shift, should trigger alarm)
            'PowerIn_HPWH': [0.5, 1.5, 0.5, 0.5, 1.5, 1.5, 1.5, 1.5, 1.5],
            'Temp_Tank': [130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0, 130.0]  # Far from 115
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_soo_change(minute_df, daily_df, mock_config_manager)

        # The turn-on at 01:05 is outside load shift, should trigger alarm
        assert len(event_df) == 2
        assert '01:02' in event_df.iloc[0]['event_detail']
        assert '01:05' in event_df.iloc[1]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_shortcycle_alarm_triggered(mock_config_manager):
    """Test alarm when HP runs for less than short_cycle_time minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HP'],
            'alarm_codes': ['SHRTCYC:1.0'],
            'pretty_name': ['HP Power']
        })
        mock_csv.return_value = csv_df

        # HP turns on for only 5 minutes (01:03-01:07), then off
        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HP': [0.5, 0.5, 1.5, 1.5, 1.5, 1.5, 1.5, 0.5, 0.5]  # On for 5 min
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_shortcycle(minute_df, daily_df, mock_config_manager, short_cycle_time=15)

        assert len(event_df) == 1
        assert 'Short cycle' in event_df.iloc[0]['event_detail']
        assert '5 minutes' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_shortcycle_no_alarm_long_run(mock_config_manager):
    """Test no alarm when HP runs for >= short_cycle_time minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HP'],
            'alarm_codes': ['SHRTCYC:1.0'],
            'pretty_name': ['HP Power']
        })
        mock_csv.return_value = csv_df

        # HP turns on for 20 minutes
        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 26)])
        minute_df = pd.DataFrame({
            'PowerIn_HP': [0.5, 0.5] + [1.5] * 20 + [0.5, 0.5, 0.5]  # On for 20 min
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_shortcycle(minute_df, daily_df, mock_config_manager, short_cycle_time=15)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_shortcycle_no_alarm_hp_always_off(mock_config_manager):
    """Test no alarm when HP never turns on"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HP'],
            'alarm_codes': ['SHRTCYC:1.0'],
            'pretty_name': ['HP Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'PowerIn_HP': [0.5, 0.5, 0.5, 0.5, 0.5]  # Always off
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_shortcycle(minute_df, daily_df, mock_config_manager, short_cycle_time=15)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_outlet_temp_alarm_after_warmup(mock_config_manager):
    """Test alarm when outlet temp is low after 10-minute warmup period"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWH_Outlet'],
            'alarm_codes': ['HPOUTLT:1.0', 'HPOUTLT:140'],
            'pretty_name': ['HP Power', 'Outlet Temperature']
        })
        mock_csv.return_value = csv_df

        # HP on for 20 minutes, temp low after warmup (minutes 11-20)
        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 22)])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5] * 21,  # HP on entire time
            'Temp_HPWH_Outlet': [150.0] * 10 + [130.0] * 11  # Warm for 10 min, then low
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_outlet_temp(minute_df, daily_df, mock_config_manager, fault_time=5)

        assert len(event_df) == 1
        assert 'Low heat pump outlet temperature' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_hp_outlet_temp_no_alarm_during_warmup(mock_config_manager):
    """Test no alarm when temp is low only during the 10-minute warmup period"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHOutlet'],
            'alarm_codes': ['HPOUTLT:1.0', 'HPOUTLT:140'],
            'pretty_name': ['HP Power', 'Outlet Temperature']
        })
        mock_csv.return_value = csv_df

        # HP on for 15 minutes, temp low only during warmup (first 10 min), then normal
        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 17)])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5] * 16,  # HP on entire time
            'Temp_HPWHOutlet': [130.0] * 10 + [150.0] * 6  # Low during warmup, then warm
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_outlet_temp(minute_df, daily_df, mock_config_manager, fault_time=5)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_outlet_temp_no_alarm_temp_above_threshold(mock_config_manager):
    """Test no alarm when outlet temp stays above threshold"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'Temp_HPWHOutlet'],
            'alarm_codes': ['HPOUTLT:1.0', 'HPOUTLT:140'],
            'pretty_name': ['HP Power', 'Outlet Temperature']
        })
        mock_csv.return_value = csv_df

        # HP on for 20 minutes, temp always above threshold
        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 22)])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH': [1.5] * 21,
            'Temp_HPWHOutlet': [150.0] * 21  # Always above 140 threshold
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_outlet_temp(minute_df, daily_df, mock_config_manager, fault_time=5)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_hp_outlet_temp_two_hpwhs_tracked_separately(mock_config_manager):
    """Test that two HPWHs are tracked independently — one alarms, one does not"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH1', 'Temp_HPWH1_Outlet', 'PowerIn_HPWH2', 'Temp_HPWH2_Outlet'],
            'alarm_codes': ['HPOUTLT:1.0', 'HPOUTLT:140', 'HPOUTLT:1.0', 'HPOUTLT:140'],
            'pretty_name': ['HP1 Power', 'HP1 Outlet Temperature', 'HP2 Power', 'HP2 Outlet Temperature']
        })
        mock_csv.return_value = csv_df

        # Both HPs on for 20 minutes.
        # HPWH1: temp drops below threshold after warmup -> should alarm
        # HPWH2: temp always above threshold -> should not alarm
        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 22)])
        minute_df = pd.DataFrame({
            'PowerIn_HPWH1': [1.5] * 21,
            'Temp_HPWH1_Outlet': [150.0] * 10 + [130.0] * 11,
            'PowerIn_HPWH2': [1.5] * 21,
            'Temp_HPWH2_Outlet': [150.0] * 21,
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_hp_outlet_temp(minute_df, daily_df, mock_config_manager, fault_time=5)

        assert len(event_df) == 1
        assert 'HP1 Outlet Temperature' in event_df.iloc[0]['event_detail']
        assert 'HP2 Outlet Temperature' not in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_HP_outage_alarm_triggered_low_power_ratio(mock_config_manager):
    """Test alarm when HP power ratio falls below threshold over rolling period"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv, \
         patch('ecopipeline.event_tracking.Alarm.Alarm._append_previous_days_to_df') as mock_append:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'PowerIn_Total'],
            'alarm_codes': ['HPOUTGE:0.3', 'HPOUTGE'],
            'pretty_name': ['HP Power', 'Total Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({'PowerIn_HPWH': [0], 'PowerIn_Total': [100]})
        minute_df.index = minute_timestamps

        # Create 7 days of daily data with low HP power ratio
        daily_timestamps = pd.to_datetime([f'2022-01-0{i}' for i in range(1, 8)])
        daily_df = pd.DataFrame({
            'PowerIn_HPWH': [10, 10, 10, 10, 10, 10, 10],  # 10% of total (below 30% threshold)
            'PowerIn_Total': [100, 100, 100, 100, 100, 100, 100]
        })
        daily_df.index = daily_timestamps

        # Mock _append_previous_days_to_df to return the same dataframe
        mock_append.return_value = daily_df

        event_df = flag_HP_outage(minute_df, daily_df, mock_config_manager,
                                  day_table_name='test_table', ratio_period_days=7)

        assert len(event_df) == 1
        assert 'Possible Heat Pump failure or outage' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_HP_outage_no_alarm_normal_power_ratio(mock_config_manager):
    """Test no alarm when HP power ratio is normal"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv, \
         patch('ecopipeline.event_tracking.Alarm.Alarm._append_previous_days_to_df') as mock_append:
        csv_df = pd.DataFrame({
            'variable_name': ['PowerIn_HPWH', 'PowerIn_Total'],
            'alarm_codes': ['HPOUTGE:0.3', 'HPOUTGE'],
            'pretty_name': ['HP Power', 'Total Power']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime(['2022-01-01 01:01'])
        minute_df = pd.DataFrame({'PowerIn_HPWH': [50], 'PowerIn_Total': [100]})
        minute_df.index = minute_timestamps

        # Create 7 days of daily data with normal HP power ratio
        daily_timestamps = pd.to_datetime([f'2022-01-0{i}' for i in range(1, 8)])
        daily_df = pd.DataFrame({
            'PowerIn_HPWH': [50, 50, 50, 50, 50, 50, 50],  # 50% of total (above 30% threshold)
            'PowerIn_Total': [100, 100, 100, 100, 100, 100, 100]
        })
        daily_df.index = daily_timestamps

        mock_append.return_value = daily_df

        event_df = flag_HP_outage(minute_df, daily_df, mock_config_manager,
                                  day_table_name='test_table', ratio_period_days=7)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_HP_outage_alarm_triggered_nonzero_alrm(mock_config_manager):
    """Test alarm when ALRM variable has non-zero value"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Alarm_HPWH'],
            'alarm_codes': ['HPOUTGE'],
            'pretty_name': ['HP Alarm']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03'
        ])
        minute_df = pd.DataFrame({
            'Alarm_HPWH': [0, 1, 0]  # Non-zero value at 01:02
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_HP_outage(minute_df, daily_df, mock_config_manager,
                                  day_table_name='test_table')

        assert len(event_df) == 1
        assert 'Heat pump alarm triggered' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_HP_outage_no_alarm_alrm_all_zero(mock_config_manager):
    """Test no alarm when ALRM variable is all zeros"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Alarm_HPWH'],
            'alarm_codes': ['HPOUTGE'],
            'pretty_name': ['HP Alarm']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03'
        ])
        minute_df = pd.DataFrame({
            'Alarm_HPWH': [0, 0, 0]  # All zeros
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_HP_outage(minute_df, daily_df, mock_config_manager,
                                  day_table_name='test_table')

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_dhw_unexpected_temp_alarm_above_high_bound(mock_config_manager):
    """Test alarm when DHW temp is above high bound for fault_time consecutive minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_DHW'],
            'alarm_codes': ['TMPRANG:110-130'],
            'pretty_name': ['DHW Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 16)])
        minute_df = pd.DataFrame({
            'Temp_DHW': [120, 120, 120] + [140] * 12  # Above high bound for 12 min
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_temp(minute_df, daily_df, mock_config_manager, fault_time=10)

        assert len(event_df) == 1
        assert 'Temperature out of range' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_dhw_unexpected_temp_alarm_below_low_bound(mock_config_manager):
    """Test alarm when DHW temp is below low bound for fault_time consecutive minutes"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_DHW'],
            'alarm_codes': ['TMPRANG:110-130'],
            'pretty_name': ['DHW Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 16)])
        minute_df = pd.DataFrame({
            'Temp_DHW': [120, 120, 120] + [100] * 12  # Below low bound for 12 min
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_temp(minute_df, daily_df, mock_config_manager, fault_time=10)

        assert len(event_df) == 1
        assert 'Temperature out of range' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_dhw_unexpected_temp_no_alarm_within_range(mock_config_manager):
    """Test no alarm when DHW temp stays within acceptable range"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['Temp_DHW'],
            'alarm_codes': ['TMPRANG:110-130'],
            'pretty_name': ['DHW Temperature']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([f'2022-01-01 01:{i:02d}' for i in range(1, 16)])
        minute_df = pd.DataFrame({
            'Temp_DHW': [115, 120, 125, 118, 122, 119, 121, 124, 116, 120, 125, 118, 122, 119, 121]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_unexpected_temp(minute_df, daily_df, mock_config_manager, fault_time=10)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_alarm_triggered(mock_config_manager):
    """Test alarm when variable doesn't match expected value during load shifting event"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'loadUp' event from 01:03 to 01:07
    ls_df = pd.DataFrame({
        'event': ['loadUp'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:03']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:07'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_loadUp:1'],
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # During loadUp (01:03-01:07), value is 0 instead of expected 1
            'soo_mode': [0, 0, 0, 0, 0, 0, 0, 0, 0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'Load shift mode inconsistency' in event_df.iloc[0]['event_detail']
        assert 'loadUp' in event_df.iloc[0]['event_detail']
        assert 'expected 1' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_no_alarm_value_matches(mock_config_manager):
    """Test no alarm when variable matches expected value during load shifting event"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'shed' event from 01:03 to 01:07
    ls_df = pd.DataFrame({
        'event': ['shed'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:03']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:07'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_shed:2'],
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # During shed (01:03-01:07), value is correctly 2
            'soo_mode': [0, 0, 2, 2, 2, 2, 0, 0, 0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_no_alarm_empty_ls_df(mock_config_manager):
    """Test no alarm when ls_df is empty (no load shifting events)"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    mock_config_manager.get_ls_df.return_value = pd.DataFrame()
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_loadUp:1'],
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05'
        ])
        minute_df = pd.DataFrame({
            'soo_mode': [0, 0, 0, 0, 0]  # Would be wrong if there were events, but there aren't
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_no_alarm_no_matching_mode(mock_config_manager):
    """Test no alarm when ls_df has events but not for the configured mode"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'shed' event, but alarm code is for 'loadUp'
    ls_df = pd.DataFrame({
        'event': ['shed'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:03']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:07'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_loadUp:1'],  # Configured for loadUp, but only shed events exist
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            'soo_mode': [0, 0, 0, 0, 0, 0, 0, 0, 0]  # Would be wrong for loadUp, but no loadUp events
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        assert event_df.empty

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_multiple_modes(mock_config_manager):
    """Test alarm detection with multiple modes configured"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with both 'loadUp' and 'shed' events
    ls_df = pd.DataFrame({
        'event': ['loadUp', 'shed'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:02', '2022-01-01 01:06']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:04', '2022-01-01 01:08'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_loadUp:1;SOOSCHD_shed:2'],
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # Correct during loadUp (01:02-01:04): value is 1
            # Wrong during shed (01:06-01:08): value is 0 instead of 2
            'soo_mode': [0, 1, 1, 0, 0, 0, 0, 0, 0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        # Should only have alarm for shed (loadUp was correct)
        assert len(event_df) == 1
        assert 'shed' in event_df.iloc[0]['event_detail']
        assert 'expected 2' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_normal_mode_alarm(mock_config_manager):
    """Test alarm when variable doesn't match expected value during normal (non-load-shifting) periods"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'loadUp' event from 01:03 to 01:05
    ls_df = pd.DataFrame({
        'event': ['loadUp'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:03']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:05'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_normal:0'],
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # During loadUp (01:03-01:05): value is 1 (doesn't matter for normal check)
            # Outside loadUp: value is 5 at 01:06 (wrong, should be 0)
            'soo_mode': [0, 0, 1, 1, 0, 5, 0, 0, 0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        assert len(event_df) == 1
        assert 'normal operation' in event_df.iloc[0]['event_detail']
        assert 'expected 0' in event_df.iloc[0]['event_detail']

@patch('ecopipeline.ConfigManager')
def test_flag_ls_mode_inconsistancy_normal_mode_no_alarm(mock_config_manager):
    """Test no alarm when variable matches expected value during normal (non-load-shifting) periods"""
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    # ls_df with a 'shed' event from 01:04 to 01:07
    ls_df = pd.DataFrame({
        'event': ['shed'],
        'startDateTime': pd.to_datetime(['2022-01-01 01:04']),
        'endDateTime': pd.to_datetime(['2022-01-01 01:07'])
    })
    mock_config_manager.get_ls_df.return_value = ls_df
    with patch('pandas.read_csv') as mock_csv:
        csv_df = pd.DataFrame({
            'variable_name': ['soo_mode'],
            'alarm_codes': ['SOOSCHD_normal:0'],
            'pretty_name': ['SOO Mode Indicator']
        })
        mock_csv.return_value = csv_df

        minute_timestamps = pd.to_datetime([
            '2022-01-01 01:01', '2022-01-01 01:02', '2022-01-01 01:03',
            '2022-01-01 01:04', '2022-01-01 01:05', '2022-01-01 01:06',
            '2022-01-01 01:07', '2022-01-01 01:08', '2022-01-01 01:09'
        ])
        minute_df = pd.DataFrame({
            # During shed (01:04-01:07): value is 2 (doesn't matter for normal check)
            # Outside shed: value is 0 (correct)
            'soo_mode': [0, 0, 0, 2, 2, 2, 0, 0, 0]
        })
        minute_df.index = minute_timestamps

        daily_timestamps = pd.to_datetime(['2022-01-01'])
        daily_df = pd.DataFrame({'dummy': [0]})
        daily_df.index = daily_timestamps

        event_df = flag_ls_mode_inconsistancy(minute_df, daily_df, mock_config_manager)

        assert event_df.empty
