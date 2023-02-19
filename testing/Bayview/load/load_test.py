#NOTE: I import everything load.py does, but if I'm just importing that to use as a package, not necessary?
import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd
import os
import numpy as np
import datetime
import unittest as ut
from ecotope_package_cs2306.config import _config_directory

"""
Unit testing for each function in load, meant to test all 
plausible cases for each function to ensure scripts are 
working as intended. 

GENERAL UNIT TEST PHILOSOPHY FOR EACH FUNCTION: 
Make a list of all features a function should have
Write a test that ensures that feature is working as intended (for all plausible cases)
"""

#NOTE: I need to import load.py similar to how it will be done in bayview, how should I go about that?

#inherits from TestCase class to add our own tests
class Test_Load(ut.TestCase):
        
    #UNITTEST: getLoginInfo
    def test_getLoginInfo(self):
        return

    #UNITTEST: connectDB
    def test_connectDB(self):
        return

    #UNITTEST: checkTableExists
    def test_checkTableExists(self):
        return

    #UNITTEST: createNewTable
    def test_createNewTable(self):
        return

    #UNITTEST: loadDatabase
    def test_loadDatabase(self):
        return

    def test_fail(self):
        self.assertEqual(3, 5)
    def test_pass(self):
        self.assertEqual(5, 5)


if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    ut.main()