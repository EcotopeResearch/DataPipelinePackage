#NOTE: I import everything load.py does, but if I'm just importing that to use as a package, not necessary?
import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd
import os
import numpy as np
import datetime
import unittest
from ecotope_package_cs2306.config import _config_directory

"""
NOTE: Unit tests can be called individually from the command line if you don't want to run all the tests!
Unit testing for each function in load, meant to test all 
plausible cases for each function to ensure scripts are 
working as intended. 

GENERAL UNIT TEST PHILOSOPHY FOR EACH FUNCTION: 
Make a list of all features a function should have
Write a test that ensures that feature is working as intended (for all plausible cases)
"""

#NOTE: I need to import load.py similar to how it will be done in bayview, how should I go about that?

#inherits from TestCase class to add our own tests
class Test_Load(unittest.TestCase):
    
    #TEST FIXTURES BELOW
    #Fixture being prep needed to perform tests, such as running a database, directories, or cleanup.

    #Are any necessary for this load testing? The database is always running, I suppose I could cleanup
    #by removing what test data I load into the db. That is important.

    #TEST CASES BELOW
    #NOTE: Because of time restraints, currently these tests only test for obviously correct and obviously 
    #incorrect cases, e.g. does getLoginInfo work with both a properly setup and entirely missing config file?

    #UNITTEST: getLoginInfo
    def test_getLoginInfo(self):
        #should we have an assertion for each possible test? do I want a function for each possible test? 
        #how should I be grouping them then, into test suites for each class? good idea. 
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

    #Default tests to make sure unittest working as intended. 
    def test_fail(self):
        self.assertEqual(3, 5)
    def test_pass(self):
        self.assertEqual(5, 5)


if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()