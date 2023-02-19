#NOTE: I import everything load.py does, but if I'm just importing that to use as a package, not necessary?
#Imports from actual package, necessary for testing? Maybe the mysql stuff I guess...
import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import os
import numpy as np
import datetime
#NECESSARY bayview load imports
import pandas as pd
import unittest
from ecotope_package_cs2306 import getLoginInfo, connectDB, checkTableExists, createNewTable, loadDatabase
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

#inherits from TestCase class to add our own tests
class Test_Load(unittest.TestCase):
    
    #TEST FIXTURES BELOW
    #Fixture being prep needed to perform tests, such as running a database, directories, or cleanup.

    #NOTE: Cleanup would be removing whatever tables get loaded as part of testing. It seems like the current
    #load scripts use fixed table names? I could create a new config but idk, seems like a mess atm, I need
    #more information, I can't write all the test cases myself, I'll just do basic ones for now. 


    #TEST CASES BELOW
    #NOTE: Because of time restraints, currently these tests only test for obviously correct and obviously 
    #incorrect cases, e.g. does getLoginInfo work with both a properly setup and entirely missing config file?

    #TEST CASES - INDIVIDUAL TESTS

    #UNITTEST: getLoginInfo
    def test_getLoginInfo(self):
        #this is how it's setup in bayview load? I don't know how else to call it?
        table_list = ["minute", "hour", "day"]
        config_dict = getLoginInfo(table_list)

        login_dict = config_dict['database']
        correct_dict = {
            "user" : "fahrigemil",
            "password" : "aBC12345!",
            "host" : "csp70.cslab.seattleu.edu",
            "database" : "testDB"
        }

        #test makes sure data was loaded correctly
        self.assertDictEqual(login_dict, correct_dict)
        #self.assertDictEqual #whole config dict? well it's massive, so 

    #UNITTEST: connectDB
    def test_connectDB(self):
        #login dictionary assumed correct for purposes of test
        login_dict = {
            "user" : "fahrigemil",
            "password" : "aBC12345!",
            "host" : "csp70.cslab.seattleu.edu",
            "database" : "testDB"
        }

        #when this is successful, it prints "Successfully connected to database."
        db_connection, db_cursor = connectDB(login_dict)

        #how can I assert connections/test that it's valid? Is there a way for a cursor to return false?
        #this prints something into main, so we could just look for that, but I'd like something more?

        db_connection.close
        db_cursor.close    

    #UNITTEST: checkTableExists
    def test_checkTableExists(self):
        return

    #UNITTEST: createNewTable
    def test_createNewTable(self):
        return

    #UNITTEST: loadDatabase
    def test_loadDatabase(self):

        #how can I look at a database and assert that it has stuff? 
        #I'll prolly copy what is done in load to check if there's already stuff?
        #mysql.connector is imported.

        #NOTE: I need a pickle w/processed bayview data, just after extract. 

        return
    
    """
    #Default tests to make sure unittest working as intended. 
    def test_fail(self):
        self.assertEqual(3, 5)
    def test_pass(self):
        self.assertEqual(5, 5)
    """

    #TEST CASE - BIG TEST
    def test_fullLoad(self):
        #Test component interaction here?
        return


if __name__ == '__main__':
    #table_list = ["minute", "hour", "day"]
    #config_dict = getLoginInfo(table_list)
    #print("\n\n", config_dict['database'], "\n\n")

    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()