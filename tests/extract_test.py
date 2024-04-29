import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime
from ecopipeline.extract import *
import numpy as np
from datetime import datetime
from pandas.testing import assert_frame_equal

def test_json_to_df():
    with patch('gzip.open') as mock_gzip:
        with patch('json.load') as mock_json:
            mock_gzip.return_value = []
            mock_json.return_value = [
                {
                    "device": "device_1",
                    "connection": "connection_1",
                    "time": "2023-07-11 10:00:00",
                    "sensors": [
                        {"id": "sensor_1", "data": 10},
                        {"id": "sensor_2", "data": 20},
                        {"id": "sensor_3", "data": 30}
                    ]
                },
                {
                    "device": "device_1",
                    "connection": "connection_1",
                    "time": "2023-07-11 10:01:00",
                    "sensors": [
                        {"id": "sensor_1", "data": 15},
                        {"id": "sensor_2", "data": 25},
                        {"id": "sensor_3", "data": 35}
                    ]
                },
                {
                    "device": "device_1",
                    "connection": "connection_1",
                    "time": "2023-07-11 10:59:59",
                    "sensors": [
                        {"id": "sensor_1", "data": 16},
                        {"id": "sensor_2", "data": 26},
                        {"id": "sensor_3", "data": 36}
                    ]
                }
            ]
            normal_df = pd.DataFrame(
                {
                    'time': ['2023-07-11 10:00:00', '2023-07-11 10:01:00','2023-07-11 11:00:00'],
                    'sensor_1': [10,15,16],
                    'sensor_2': [20,25,26],
                    'sensor_3': [30,35,36]
                }
            )

            result_df = json_to_df(["file/path/to/whatever"],None)
            mock_gzip.assert_called_once_with("file/path/to/whatever")
            normal_df['time'] = pd.to_datetime(normal_df['time'])
            normal_df.set_index('time', inplace=True)
            normal_df.columns.name = 'id'
            assert_frame_equal(result_df, normal_df)

def test_extract_new_mb():
    date = datetime(2023, 9, 1)
    file_names = ["mb-022.64C84441_1.log.csv", "mb-042.652939A5_1.log.csv", "mb-034.65083340_1.log.csv"]
    assert extract_new(date, file_names, True) == ['mb-042.652939A5_1.log.csv', 'mb-034.65083340_1.log.csv']
    assert extract_new(date, file_names, True, "US/Pacific") == ['mb-042.652939A5_1.log.csv', 'mb-034.65083340_1.log.csv']

def test_extract_new():
    date = datetime(2023, 9, 1)
    file_names = ["E45F012D4C0D_20240103220000.gz", "E45F012D4C0D_20240104220000.gz", "E45F012D4C0D_20230103220000.gz"]
    assert extract_new(date, file_names, False) == ["E45F012D4C0D_20240103220000.gz", "E45F012D4C0D_20240104220000.gz"]
    assert extract_new(date, file_names, False, "US/Pacific") == ["E45F012D4C0D_20240103220000.gz", "E45F012D4C0D_20240104220000.gz"]

@pytest.mark.parametrize(
        "file_1_df, file_2_df, expected_df", 
        [
            (
                pd.DataFrame({
                'time(UTC)': ['2023-07-11 10:00:00', '2023-07-11 10:01:00'],
                'sensor_1': [10,15],
                'sensor_2': [20,25],
                'sensor_3': [30,35]
                }),
                pd.DataFrame({
                    'time(UTC)': ['2023-07-11 10:01:00', '2023-07-11 10:05:00','2023-07-12 10:05:00'],
                    'sensor_1': [0.02,12.3, 8.715],
                    'sensor_2': [800,None, None],
                    'sensor_3': [45,45.327, None]
                }),
                pd.DataFrame({
                    'time(UTC)': ['2023-07-11 10:00:00', '2023-07-11 10:01:00','2023-07-11 10:05:00','2023-07-12 10:05:00'],
                    'mb-001_sensor_1': [10,15,None,None],
                    'mb-001_sensor_2': [20,25,None,None],
                    'mb-001_sensor_3': [30,35,None,None],
                    'mb-002_sensor_1': [None,0.02,12.3, 8.715],
                    'mb-002_sensor_2': [None,800,None, None],
                    'mb-002_sensor_3': [None,45,45.327, None]
                })
            ),
            (
                pd.DataFrame({
                'time(UTC)': ['2023-07-11 10:00:01', '2023-07-11 10:01:59'],
                'sensor_1': [10,15],
                'sensor_2': [20,25],
                'sensor_3': [30,35]
                }),
                pd.DataFrame({
                    'time(UTC)': ['2023-07-11 10:01:01', '2023-07-11 10:01:02','2023-07-12 10:05:48'],
                    'sensor_1': [0.02,0.04, 8.715],
                    'sensor_2': [800,None, None],
                    'sensor_3': [45,46, None]
                }),
                pd.DataFrame({
                    'time(UTC)': ['2023-07-11 10:00:00', '2023-07-11 10:01:00','2023-07-12 10:05:00'],
                    'mb-001_sensor_1': [10,15,None],
                    'mb-001_sensor_2': [20,25,None],
                    'mb-001_sensor_3': [30,35,None],
                    'mb-002_sensor_1': [None,0.03, 8.715],
                    'mb-002_sensor_2': [None,800, None],
                    'mb-002_sensor_3': [None,45.5, None]
                })
            )

        ]
)
def test_csv_to_df_mb(file_1_df, file_2_df, expected_df):   
    with patch('pandas.read_csv') as mock_read_csv:
        mock_read_csv.side_effect = [
            file_1_df,
            file_2_df
        ]

        result_df = csv_to_df(["file/path/to/whatever/mb-001.652939A5_1.log.csv", "file/path/to/whatever/mb-002.65083340_1.log.csv"],True)
        # Get the list of call arguments
        calls = mock_read_csv.call_args_list
        assert calls[0] == (("file/path/to/whatever/mb-001.652939A5_1.log.csv",), {})
        assert calls[1] == (("file/path/to/whatever/mb-002.65083340_1.log.csv",), {})

        expected_df['time(UTC)'] = pd.to_datetime(expected_df['time(UTC)'])
        expected_df.set_index('time(UTC)', inplace=True)
        assert_frame_equal(result_df, expected_df)


@patch('ecopipeline.ConfigManager')
def test_small_planet_control_to_df(mock_config_manager):
    mock_config_manager.get_var_names_path.return_value = "fake/path/whatever/Variable_Names.csv"
    with patch('pandas.read_csv') as mock_csv:

        # Set the desired response for mock_connect.return_value
        var_names_df = pd.DataFrame({'variable_alias': ['MOD_reference_0X53G', 'MOD_reference_silly_name', 'MOD_reference_silly_name_changed', 'silly_strings'],
                        'variable_name': ['serious_var_1', 'serious_var_2', 'serious_var_2', 'serious_var_4'],
                        'other_column_1': [None, None, None,None],
                        'other_column_2': [1,2,3,4]})
        # mock_csv.return_value = var_names_df

        mock_csv.side_effect = [
            var_names_df,
            pd.DataFrame({
                'DateEpoch(secs)': ['1713078048', '1713078108'],
                'silly_name': [10,15],
                }),
            pd.DataFrame({
                    'DateEpoch(secs)': ['1713078168', '1713078228'],
                    'silly_name_changed': [20,25],
                }),
            pd.DataFrame({
                    'DateEpoch(secs)': ['1713078048', '1713078108','1713078168', '1713078228'],
                    '0X53G': [1.1,1.1,1.2,1.2],
                }),
        ]
        filenames = ["file/path/to/whatever/MOD_reference_.silly_name.1713078048.csv", "file/path/to/whatever/MOD_reference_.silly_name_changed.1713078168.csv",
                     "file/path/to/whatever/MOD_reference_.0X53G.1713078048.csv"]
        result_df = small_planet_control_to_df(mock_config_manager,filenames)
        # Get the list of call arguments
        calls = mock_csv.call_args_list
        assert calls[0] == (("fake/path/whatever/Variable_Names.csv",), {})
        assert calls[1] == ((filenames[0],), {})
        assert calls[2] == ((filenames[1],), {})

        expected_df = pd.DataFrame({
                    'time_pt': ['2024-04-14 00:00:00', '2024-04-14 00:01:00', '2024-04-14 00:02:00', '2024-04-14 00:03:00'],
                    'serious_var_2': [10.0,15,20,25],
                    'serious_var_1': [1.1,1.1,1.2,1.2],
                })

        expected_df['time_pt'] = pd.to_datetime(expected_df['time_pt'])
        expected_df.set_index('time_pt', inplace=True)
        assert_frame_equal(result_df, expected_df)
