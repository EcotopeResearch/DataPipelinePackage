import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime
from ecopipeline import get_noaa_data, json_to_df, extract_files, get_last_full_day_from_db, get_db_row_from_time, extract_new, csv_to_df, get_sub_dirs
from ecopipeline.config import _config_directory
import numpy as np
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
