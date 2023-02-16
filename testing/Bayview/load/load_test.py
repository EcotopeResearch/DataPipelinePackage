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
"""

#UNITTEST: getLoginInfo -- ImportDocuments and have test here!!
def UNIT_TEST_getLoginInfo(table_headers: list, config_info : str = _config_directory) -> dict:
    """
    Function will and return config.ini in a config var.
    Output: Login information
    """
    return

#UNITTEST: connectDB
def UNIT_TEST_connectDB():
    return