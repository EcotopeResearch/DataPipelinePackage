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
from ecopipeline import get_login_info, connect_db, load_database
from ecopipeline.config import _config_directory

#TODO: Implement SQL mocks to work with this testing! Right now there are errors and failures could be avoided,
#lot less work than preparing the DB every time we want to test, and tests would break if multiple people ran them 
#even if we did that properly.

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
    incorrect_login_dict = {'database': {'user': 'fahrigemil', 'password': 'wrong', 'host': 'csp70.cslab.seattleu.edu', 'database': 'testDB'}}
    test_headers = ["day"]
    test_config_path = "test_config.ini"
    load_data = pd.DataFrame(np.random.randint(1, 10, size=(5, 5)).astype(float), columns=login_dict["day"]["sensor_list"], 
                             index=[datetime(2022, 1, i) for i in range(1, 6)])
    new_data = pd.DataFrame(np.random.randint(1, 10, size=(3, 5)).astype(float), columns=login_dict["day"]["sensor_list"], 
                    index=[datetime(2022, 1, i) for i in range(6, 9)])
    bad_columns_data = pd.DataFrame(np.random.randint(1, 10, size=(3, 5)).astype(float), columns=["bad1", "bad2", "bad3", "bad4", "bad5"], 
                    index=[datetime(2022, 1, i) for i in range(6, 9)])
    bad_dtype_data = pd.DataFrame(np.random.randint(1, 10, size=(3, 5)).astype(float), columns=login_dict["day"]["sensor_list"], 
                    index=[datetime(2022, 1, i) for i in range(6, 9)])
    bad_dtype_data.at["2022-01-06", "sensor2"] = "bad"
    
    """ #CURRENTLY HAS ERRORS!
    def test_correctheader_getLoginInfo(self):
        config_dict = get_login_info(self.test_headers, self.test_config_path)
        self.assertDictEqual(config_dict, self.login_dict)
    """

    """ #CURRENTLY HAS ERRORS!
    def test_incorrectheader_getLoginInfo(self):
        self.assertRaises(configparser.NoSectionError, get_login_info, ["bad"], self.test_config_path)
    """
    
    """ #CURRENTLY HAS ERRORS!
    def test_incorrectpassword_connectDB(self):
        cxn, cursor = connect_db(self.incorrect_login_dict)
        self.assertEqual(cxn, None)
        self.assertEqual(cursor, None)
    """

    def test_connectDB(self):
        cxn, cursor = connect_db(self.login_dict["database"])
        self.assertNotEqual(cxn, None)
        self.assertNotEqual(cursor, None)
        cxn.close()
        cursor.close()

    """ #CURRENTLY HAS ERRORS!
    def test_empty_table_loadDatabase(self):
        cxn, cursor = connect_db(self.login_dict["database"])
        load_database(cursor, self.load_data, self.login_dict, "day")
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

        cxn.commit()
        cxn.close()
        cursor.close()
    """

    """ #CURRENTLY HAS ERRORS!
    def test_existing_table_loadDatabase(self):
        cxn, cursor = connect_db(self.login_dict["database"])

        load_database(cursor, self.load_data, self.login_dict, "day")
        cxn.commit()

        load_database(cursor, self.new_data, self.login_dict, "day")
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

        cxn.commit()
        cxn.close()
        cursor.close()
    """
    
    def test_bad_dataFrame_loadDatabase(self):
        cxn, cursor = connect_db(self.login_dict["database"])
        self.assertRaises(mysql.connector.errors.ProgrammingError, load_database, cursor, self.bad_columns_data, self.login_dict, "day")
        cursor.execute("drop table bayview_day")

        cxn.commit()
        cxn.close()
        cursor.close()

    def test_bad_type_loadDatabase(self):
        cxn, cursor = connect_db(self.login_dict["database"])
        self.assertRaises(KeyError, load_database, cursor, self.load_data, self.login_dict, "bad")

        cxn.close()
        cursor.close()

    """ #CURRENTLY HAS ERRORS!
    def test_bad_data_loadDatabase(self):
        cxn, cursor = connect_db(self.login_dict["database"])
        self.assertRaises(mysql.connector.errors.DatabaseError, load_database, cursor, self.bad_dtype_data, self.login_dict, "day")

        cxn.close()
        cursor.close()
    """


if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()