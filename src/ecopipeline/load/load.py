import configparser
import mysql.connector
import mysql.connector.cursor
import sys
import pandas as pd
import os
import math
pd.set_option('display.max_columns', None)
import mysql.connector.errors as mysqlerrors
from ecopipeline import ConfigManager
import datetime
import numpy as np

data_map = {'int64':'float',
            'int32':'float',
            'float64': 'float',
            'M8[ns]':'datetime',
            'datetime64[ns]':'datetime',
            'object':'varchar(25)',
            'bool': 'boolean'}

def check_table_exists(cursor : mysql.connector.cursor.MySQLCursor, table_name: str, dbname: str) -> int:
    """
    Check if the given table name already exists in database.

    Parameters
    ---------- 
    cursor : mysql.connector.cursor.MySQLCursor
        Database cursor object and the table name.
    table_name : str 
        Name of the table
    dbname : str
        Name of the database

    Returns
    ------- 
    int: 
        The number of tables in the database with the given table name.
        This can directly be used as a boolean!
    """

    cursor.execute(f"SELECT count(*) "
                   f"FROM information_schema.TABLES "
                   f"WHERE (TABLE_SCHEMA = '{dbname}') AND (TABLE_NAME = '{table_name}')")

    num_tables = cursor.fetchall()[0][0]
    return num_tables


def create_new_table(cursor : mysql.connector.cursor.MySQLCursor, table_name: str, table_column_names: list, table_column_types: list, primary_key: str = "time_pt") -> bool:
    """
    Creates a new table in the mySQL database.

    Parameters
    ---------- 
    cursor : mysql.connector.cursor.MySQLCursor
        A cursor object and the name of the table to be created.
    table_name : str
        Name of the table
    table_column_names : list
        list of columns names in the table must be passed.

    Returns
    ------- 
    bool: 
        A boolean value indicating if a table was sucessfully created. 
    """
    if(len(table_column_names) != len(table_column_types)):
        raise Exception("Cannot create table. Type list and Field Name list are different lengths.")

    create_table_statement = f"CREATE TABLE {table_name} (\n{primary_key} datetime,\n"

    for i in range(len(table_column_names)):
        create_table_statement += f"{table_column_names[i]} {table_column_types[i]} DEFAULT NULL,\n"

    create_table_statement += f"PRIMARY KEY ({primary_key})\n"

    create_table_statement += ");"
    cursor.execute(create_table_statement)

    return True


def find_missing_columns(cursor : mysql.connector.cursor.MySQLCursor, dataframe: pd.DataFrame, config_dict: dict, table_name: str):
    """
    Finds the column names which are not in the database table currently but are present
    in the pandas DataFrame to be written to the database. If communication with database
    is not possible, an empty list will be returned meaning no column will be added. 

    Parameters
    ---------- 
    cursor : mysql.connector.cursor.MySQLCursor 
        A cursor object and the name of the table to be created.
    dataframe : pd.DataFrame
        the pandas DataFrame to be written into the mySQL server. 
    config_info : dict
        The dictionary containing the configuration information 
    data_type : str
        the header name corresponding to the table you wish to write data to.  

    Returns
    ------- 
    list: 
        list of column names which must be added to the database table for the pandas 
        DataFrame to be properly written into the database. 
    """

    try:
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '"
                            f"{config_dict['database']}' AND table_name = '"
                            f"{table_name}'")
    except mysqlerrors.DatabaseError:
        print("Check if the mysql table to be written to exists.")
        return []
    
    current_table_names = list(cursor.fetchall())
    current_table_names = [name[0] for name in current_table_names]
    df_names = list(dataframe.columns)
    
    cols_to_add = [sensor_name for sensor_name in df_names if sensor_name not in current_table_names]
    data_types = [dataframe[column].dtype.name for column in cols_to_add]
    
    data_types = [data_map[data_type] for data_type in data_types]
    
    return cols_to_add, data_types


def create_new_columns(cursor : mysql.connector.cursor.MySQLCursor, table_name: str, new_columns: list, data_types: str):
    """
    Create the new, necessary column in the database. Catches error if communication with mysql database
    is not possible.

    Parameters
    ----------  
    cursor : mysql.connector.cursor.MySQLCursor
        A cursor object and the name of the table to be created.
    config_info : dict
        The dictionary containing the configuration information.
    data_type : str
        the header name corresponding to the table you wish to write data to.  
    new_columns : list
        list of columns that must be added to the database table.

    Returns
    ------- 
    bool:
        boolean indicating if the the column were successfully added to the database. 
    """
    alter_table_statements = [f"ALTER TABLE {table_name} ADD COLUMN {column} {data_type} DEFAULT NULL;" for column, data_type in zip(new_columns, data_types)]

    for sql_statement in alter_table_statements:
        try:
            cursor.execute(sql_statement)
        except mysqlerrors.DatabaseError:
            print("Error communicating with the mysql database.")
            return False

    return True

def load_overwrite_database(cursor : mysql.connector.cursor.MySQLCursor, dataframe: pd.DataFrame, config_info: dict, data_type: str, primary_key: str = "time_pt", table_name: str = None):
    """
    Loads given pandas DataFrame into a MySQL table overwriting any conflicting data. Uses an UPSERT strategy to ensure any gaps in data are filled.
    Note: will not overwrite values with NULL. Must have a new value to overwrite existing values in database

    Parameters
    ----------  
    cursor : mysql.connector.cursor.MySQLCursor
        A cursor object connected to the database where the data will land
    dataframe: pd.DataFrame
        The pandas DataFrame to be written into the mySQL server.
    config_info: dict
        The dictionary containing the configuration information in the data upload. This can be aquired through the get_login_info() function in this package
    data_type: str
        The header name corresponding to the table you wish to write data to.  

    Returns
    ------- 
    bool: 
        A boolean value indicating if the data was successfully written to the database. 
    """
    # Drop empty columns
    dataframe = dataframe.dropna(axis=1, how='all')

    dbname = config_info['database']
    if table_name == None:
        table_name = config_info[data_type]["table_name"]   
    
    if(len(dataframe.index) <= 0):
        print(f"Attempted to write to {table_name} but dataframe was empty.")
        return True

    print(f"Attempting to write data for {dataframe.index[0]} to {dataframe.index[-1]} into {table_name}")
    
    # Get string of all column names for sql insert
    sensor_names = primary_key
    sensor_types = ["datetime"]
    for column in dataframe.columns:
        sensor_names += "," + column    
        sensor_types.append(data_map[dataframe[column].dtype.name])

    # create SQL statement
    insert_str = "INSERT INTO " + table_name + " (" + sensor_names + ") VALUES ("
    for column in dataframe.columns:
        insert_str += "%s, "
    insert_str += "%s)"
    
    # last_time = datetime.datetime.strptime('20/01/1990', "%d/%m/%Y") # arbitrary past date
    existing_rows_list = []

    # create db table if it does not exist, otherwise add missing columns to existing table
    if not check_table_exists(cursor, table_name, dbname):
        if not create_new_table(cursor, table_name, sensor_names.split(",")[1:], sensor_types[1:], primary_key=primary_key): #split on colums and remove first column aka time_pt
            print(f"Could not create new table {table_name} in database {dbname}")
            return False
    else:
        try:
            # find existing times in database for upsert statement
            cursor.execute(
                f"SELECT {primary_key} FROM {table_name} WHERE {primary_key} >= '{dataframe.index.min()}'")
            # Fetch the results into a DataFrame
            existing_rows = pd.DataFrame(cursor.fetchall(), columns=[primary_key])

            # Convert the primary_key column to a list
            existing_rows_list = existing_rows[primary_key].tolist()

        except mysqlerrors.Error:
            print(f"Table {table_name} has no data.")

        missing_cols, missing_types = find_missing_columns(cursor, dataframe, config_info, table_name)
        if len(missing_cols):
            if not create_new_columns(cursor, table_name, missing_cols, missing_types):
                print("Unable to add new columns due to database error.")
    
    updatedRows = 0
    for index, row in dataframe.iterrows():
        time_data = row.values.tolist()
        #remove nans and infinites
        time_data = [None if (x is None or pd.isna(x)) else x for x in time_data]
        time_data = [None if (x == float('inf') or x == float('-inf')) else x for x in time_data]

        if index in existing_rows_list:
            statement, values = _generate_mysql_update(row, index, table_name, primary_key)
            if statement != "":
                cursor.execute(statement, values)
                updatedRows += 1
        else:
            cursor.execute(insert_str, (index, *time_data))

    print(f"Successfully wrote {len(dataframe.index)} rows to table {table_name} in database {dbname}. {updatedRows} existing rows were overwritten.")
    return True

def _generate_mysql_update(row, index, table_name, primary_key):
    statement = f"UPDATE {table_name} SET "
    statment_elems = []
    values = []
    for column, value in row.items():
        if not value is None and not pd.isna(value) and not (value == float('inf') or value == float('-inf')):
            statment_elems.append(f"{column} = %s")
            values.append(value)

    if values:
        statement += ", ".join(statment_elems)
        statement += f" WHERE {primary_key} = '{index}';"
    else:
        statement = ""

    return statement, values