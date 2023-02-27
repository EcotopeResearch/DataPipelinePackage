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
from datetime import datetime
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
    login_dict = {'database': {'user': 'fahrigemil', 'password': 'aBC12345!', 'host': 'csp70.cslab.seattleu.edu', 'database': 'testDB'},
                  'day': {'table_name': 'bayview_day', 
                             'sensor_list': ['sensor1', 'sensor2', 'sensor3', 'sensor4', 'sensor5']}}
    incorrect_password_login_dict = {'database': {'user': 'fahrigemil', 'password': 'wrong', 'host': 'csp70.cslab.seattleu.edu', 'database': 'testDB'}}
    test_headers = ["day"]
    test_config_path = "test_config.ini"
    load_data = pd.DataFrame(np.random.randint(1, 10, size=(5, 5)).astype(float), columns=login_dict["day"]["sensor_list"], 
                             index=[datetime(2022, 1, i) for i in range(1, 6)])
    new_data = pd.DataFrame(np.random.randint(1, 10, size=(3, 5)).astype(float), columns=login_dict["day"]["sensor_list"], 
                    index=[datetime(2022, 1, i) for i in range(6, 9)])
    

    """
    def test_correctheader_getLoginInfo(self):
        config_dict = getLoginInfo(self.test_headers, self.test_config_path)
        self.assertDictEqual(config_dict, self.login_dict)

    def test_incorrectheader_getLoginInfo(self):
        self.assertRaises(configparser.NoSectionError, getLoginInfo, ["bad"], self.test_config_path)
        
    def test_incorrectpassword_connectDB(self):
        cxn, cursor = connectDB(self.incorrect_password_login_dict)
        self.assertEqual(cxn, None)
        self.assertEqual(cursor, None)

    def test_connectDB(self):
        cxn, cursor = connectDB(self.login_dict["database"])
        self.assertNotEqual(cxn, None)
        self.assertNotEqual(cursor, None)
        cxn.close()
        cursor.close()

    def test_empty_table_loadDatabase(self):
        cxn, cursor = connectDB(self.login_dict["database"])
        loadDatabase(cursor, self.load_data, self.login_dict, "day")
        cxn.commit()

        cursor.execute("select * from bayview_day")
        table_data = pd.DataFrame(cursor.fetchall())
        cursor.execute(f"select column_name from information_schema.columns where table_schema = 'testDB' and table_name = 'bayview_day'")
        column_names = cursor.fetchall()
        column_names = [name[0] for name in column_names]
        table_data.columns = column_names
        table_data = table_data.set_index(["time"])
        table_data = table_data.rename_axis(None)

        self.assertEqual(table_data.equals(self.load_data), True)

        cursor.execute("drop table bayview_day")

        cxn.close()
        cursor.close()
    """

    def test_existing_table_loadDatabase(self):
        cxn, cursor = connectDB(self.login_dict["database"])

        loadDatabase(cursor, self.load_data, self.login_dict, "day")
        cxn.commit()

        loadDatabase(cursor, self.new_data, self.login_dict, "day")
        cxn.commit()

        cursor.execute("select * from bayview_day")
        table_data = pd.DataFrame(cursor.fetchall())
        cursor.execute(f"select column_name from information_schema.columns where table_schema = 'testDB' and table_name = 'bayview_day'")
        column_names = cursor.fetchall()
        column_names = [name[0] for name in column_names]
        table_data.columns = column_names
        table_data = table_data.set_index(["time"])
        table_data = table_data.rename_axis(None)

        self.assertEqual(table_data.equals(pd.concat([self.load_data, self.new_data])), True)

        cursor.execute("drop table bayview_day")

        cxn.close()
        cursor.close()


    

    """
    #UNITTEST: connectDB
    def test_connectDB(self):
        #when this is successful, it prints "Successfully connected to database."
        db_connection, db_cursor = connectDB(self.login_dict)
        #NOTE: Test behavior when wrong login credentials passed in 

        #how can I assert connections/test that it's valid? Is there a way for a cursor or connection object to return false?

        db_connection.close
        db_cursor.close    
    """
    """
    #UNITTEST: checkTableExists
    #NOTE: Helper function in bayview run
    def test_checkTableExists(self):
        #NOTE: I NEED a cursor to test this, so connectDB needs to be working for this to work
        db_connection, db_cursor = connectDB(self.login_dict)

        #Bool should be true #MAKE SURE IT'S A TABLE THAT'S ALWAYS THERE!!
        trueBool = checkTableExists(db_cursor, "existing_table") 
        #Bool should be false
        falseBool = checkTableExists(db_cursor, "this_table_doesn't_exist")

        self.assertEqual(trueBool, True)
        self.assertEqual(falseBool, False)

        db_connection.close
        db_cursor.close

    #UNITTEST: createNewTable
    #NOTE: Helper function in bayview run
    def test_createNewTable(self):
        #NOTE: I NEED a cursor for this, so connect_DB must be working for this test to pass
        db_connection, db_cursor = connectDB(self.login_dict)

        #test by creating a table called "test_table_DON'T_REMOVE"
        #run a second time to make sure that it works w/the table already being created? or is the other check for that?

        #try creating a table that doesn't exist yet, a normal test
        trueBool = createNewTable(db_cursor, "new_table", ["col1", "col2", "col3"])

        #try creating a table that exists, error checking
        falseBool = createNewTable(db_cursor, "existing_table", ["col1", "col2", "col3"])

        self.assertEqual(trueBool, True)
        self.assertEqual(falseBool, False)

        db_connection.close
        db_cursor.close
    """

    """
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

        db_connection.close
        db_cursor.close
        return
    """


if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()