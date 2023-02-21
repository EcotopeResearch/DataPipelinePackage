import unittest
import sys
from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
from pandas.testing import assert_frame_equal
import gzip
import os, json
import datetime as dt
from ecotope_package_cs2306.config import _config_directory, _data_directory, _output_directory
from src.ecotope_package_cs2306 import get_last_line, extract_new, extract_files, json_to_df, get_noaa_data

class Test_Extract(unittest.TestCase):

    def test_extract_files_fail(self):
        test_extract = extract_files(".csv")
        expected = []
        self.assertEqual(expected,test_extract)
    """
    def test_extract_files_pass(self):
        test_extract = extract_files(".gz")
        expected = []

        self.assertEqual(expected,test_extract)
    """

    """
    def test_json_to_df_pass(self):
        test_json_df = json_to_df(extract_files('.gz'))
        expected = pd.DataFrame()
        assert_frame_equal(expected, test_json_df)
        self.assertEqual(expected, test_json_df)
    """
    def test_json_to_df_fail(self):
        test_json_df = json_to_df(extract_files('.csv'))
        expected = pd.DataFrame()
        self.assertEqual(expected, test_json_df)

    def test_get_noaa_data_fail(self):
        test_noaa_data = get_noaa_data([])
        expected = {}
        self.assertDictEqual(expected, test_noaa_data)

    def test_extract_new_fail(self):
        test_new_extract = extract_new(get_last_line(), extract_files('.gz'))
        expected = []
        self.assertDictEqual(expected, test_new_extract)