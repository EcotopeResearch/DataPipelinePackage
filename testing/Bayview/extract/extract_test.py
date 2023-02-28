import unittest
import sys
import configparser
from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
from pandas.testing import assert_frame_equal
import gzip
import os, json
import datetime as dt
from ecotope_package_cs2306.config import _config_directory, _data_directory, _output_directory
from ecotope_package_cs2306 import get_last_line, extract_files, json_to_df, get_noaa_data

class Test_Extract(unittest.TestCase):

    def test_get_last_line(self):
        df = get_last_line()
        self.assertNotEqual(pd.DataFrame(), df)

    def test_extract_files(self):
        test_extract = extract_files(".csv")
        expected = []
        self.assertEqual(expected,test_extract)

        test_extract = extract_files(".gz")
        help = configparser.ConfigParser().get('data', 'directory')
        
        expected = os.listdir(help)

        self.assertEqual(expected[0], test_extract[0])
        self.assertEqual(expected[5], test_extract[5])    

    def test_json_to_df(self):
        test_json_df = json_to_df(extract_files('.csv'))
        expected = pd.DataFrame()
        self.assertEqual(expected, test_json_df)

        test_json_df = json_to_df(extract_files('.gz'))
        expected = 7
        self.assertEqual(expected, expected.shape[1])
       

    def test_get_noaa_data(self):
        test_noaa_data = get_noaa_data([])
        expected = {}
        self.assertDictEqual(expected, test_noaa_data)

        test_noaa_data = get_noaa_data(['KBFI'])
        #self.assertEquals(, test_noaa_data)
        pass
        

       
    if __name__ == '__main__':
        unittest.main()