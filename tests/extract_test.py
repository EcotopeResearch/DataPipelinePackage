import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime
from ecopipeline import get_noaa_data, json_to_df, extract_files, get_last_full_day_from_db, get_db_row_from_time, extract_new, csv_to_df, get_sub_dirs
from ecopipeline.config import _config_directory
import numpy as np
from datetime import datetime
import math
import mysql.connector
import gzip
import os
import json
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
                }
            ]
            normal_df = pd.DataFrame(
                {
                    'time': ['2023-07-11 10:00:00', '2023-07-11 10:01:00'],
                    'sensor_1': [10,15],
                    'sensor_2': [20,25],
                    'sensor_3': [30,35]
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

def test_csv_to_df_mb():   
    with patch('pandas.read_csv') as mock_read_csv:
        mock_read_csv.side_effect = [
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
            })
        ]
        normal_df = pd.DataFrame(
            {
                'time(UTC)': ['2023-07-11 10:00:00', '2023-07-11 10:01:00','2023-07-11 10:05:00','2023-07-12 10:05:00'],
                'mb-001_sensor_1': [10,15,None,None],
                'mb-001_sensor_2': [20,25,None,None],
                'mb-001_sensor_3': [30,35,None,None],
                'mb-002_sensor_1': [None,0.02,12.3, 8.715],
                'mb-002_sensor_2': [None,800,None, None],
                'mb-002_sensor_3': [None,45,45.327, None]
            }
        )

        result_df = csv_to_df(["file/path/to/whatever/mb-001.652939A5_1.log.csv", "file/path/to/whatever/mb-002.65083340_1.log.csv"],True)
        # Get the list of call arguments
        calls = mock_read_csv.call_args_list
        assert calls[0] == (("file/path/to/whatever/mb-001.652939A5_1.log.csv",), {})
        assert calls[1] == (("file/path/to/whatever/mb-002.65083340_1.log.csv",), {})

        normal_df['time(UTC)'] = pd.to_datetime(normal_df['time(UTC)'])
        normal_df.set_index('time(UTC)', inplace=True)
        # normal_df.columns.name = 'id'
        assert_frame_equal(result_df, normal_df)
