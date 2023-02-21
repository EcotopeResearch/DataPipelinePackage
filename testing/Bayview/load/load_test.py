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
    
    #CLASS DATA
    login_dict = {
            "user" : "fahrigemil",
            "password" : "aBC12345!",
            "host" : "csp70.cslab.seattleu.edu",
            "database" : "testDB"
        }
    table_list = ["minute", "hour", "day"]
    #need a list of all the columns, unfortunately that list is HUGE
    column_list = []

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
        config_dict = getLoginInfo(self.table_list)

        #currently, this fails, despite it being called exactly how it is in bayview load
        test_login_dict = config_dict['database']

        #test makes sure data was loaded correctly
        self.assertDictEqual(test_login_dict, self.login_dict)
        #self.assertDictEqual #whole config dict? well it's massive, so 

    #UNITTEST: connectDB
    def test_connectDB(self):
        #when this is successful, it prints "Successfully connected to database."
        db_connection, db_cursor = connectDB(self.login_dict)

        #how can I assert connections/test that it's valid? Is there a way for a cursor or connection object to return false?

        db_connection.close
        db_cursor.close    

    #UNITTEST: checkTableExists
    #NOTE: Helper function in bayview run
    def test_checkTableExists(self):
        #NOTE: I NEED a cursor to test this, so connectDB needs to be working for this to work
        db_connection, db_cursor = connectDB(self.login_dict)

        #Bool should be true #MAKE SURE IT'S A TABLE THAT'S ALWAYS THERE!!
        trueBool = checkTableExists(db_cursor, "test_table_DON'T_REMOVE") 
        #Bool should be false
        falseBool = checkTableExists(db_cursor, "this_table_doesn't_exist")

        self.assertEqual(trueBool, True)
        self.assertEqual(falseBool, False)

    #UNITTEST: createNewTable
    #NOTE: Helper function in bayview run
    def test_createNewTable(self):
        #NOTE: I NEED a cursor for this, so connect_DB must be working for this test to pass
        db_connection, db_cursor = connectDB(self.login_dict)

        #test by creating a table called "test_table_DON'T_REMOVE"
        #run a second time to make sure that it works w/the table already being created? or is the other check for that?

        #try creating a table that doesn't exist yet, a normal test
        trueBool = createNewTable(db_cursor, "new_table", ["col1", "col2", "col3"])

        #try creating a table that exists, error checkingx
        falseBool = createNewTable(db_cursor, self.table_list[0], self.column_list)

        self.assertEqual(trueBool, True)
        self.assertEqual(falseBool, False)

    #UNITTEST: loadDatabase
    def test_loadDatabase(self):
        #how can I look at a database and assert that it has stuff? 
        #I'll prolly copy what is done in load to check if there's already stuff?
        #mysql.connector is imported.
        db_connection, db_cursor = connectDB(self.login_dict)

        #NOTE: I need a pickle w/processed bayview data, just after extract. 
        #NOTE: I'm going to wait until I can work with someone more familiar with load and such to make this.

        #The basic idea will be to fetch the # of rows in the tables of minute, hourly, and daily, and make sure they
        #match with what they should be. If that's all good, we call that a successful test. 

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