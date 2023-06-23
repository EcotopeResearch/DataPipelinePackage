import configparser
import mysql.connector
import sys
import pandas as pd
import os
import math
from ecopipeline.config import _config_directory
pd.set_option('display.max_columns', None)
import mysql.connector.errors as mysqlerrors
import datetime
import numpy as np

data_map = {'int64':'float',
                'float64': 'float',
                'M8[ns]':'datetime',
                'datetime64[ns]':'datetime',
                'object':'varchar(25)',
                'bool': 'boolean'}

def get_login_info(table_headers: list, config_info : str = _config_directory) -> dict:
    """
    Reads the config.ini file stored in the config_info file path.   

    Args: 
        table_headers (list): A list of table headers. These headers must correspond to the 
            section headers in the config.ini file. Your list must contain the section
            header for each table you wish to write into. The first header must correspond 
            to the login information of the database. The other are the tables which you wish
            to write to. 
        config_info (str): A path to the config.ini file must also be passed.

    Returns: 
        dict: A dictionary containing all relevant information is returned. This
            includes information used to create a connection with a mySQL server and
            information (table names and column names) used to load the data into 
            tables. 
    """

    if not os.path.exists(config_info):
        print(f"File path '{config_info}' does not exist.")
        sys.exit()

    configure = configparser.ConfigParser()
    configure.read(config_info)

    db_connection_info = {
        "database": {'user': configure.get('database', 'user'),
                     'password': configure.get('database', 'password'),
                     'host': configure.get('database', 'host'),
                     'database': configure.get('database', 'database')}
    }

    db_table_info = {header: {"table_name": configure.get(header, 'table_name')} for header in table_headers} 
    
    db_connection_info.update(db_table_info)

    print(f"Successfully fetched configuration information from file path {config_info}.")
    return db_connection_info
    

def connect_db(config_info: dict):
    """
    Create a connection with the mySQL server. 

    Args: 
        config_info (dict): The dictionary containing the credential information. This is
            contained in the 'database' section of the dictionary. 

    Returns: 
        mysql.connector.cursor.MySQLCursor: A connection and cursor object. THe cursor can be used to execute
            mySQL queries and the connection object can be used to save those changes. 
    """

    connection = None

    try:
        connection = mysql.connector.connect(**config_info)
    except mysql.connector.Error:
        print("Unable to connect to database with given credentials.")
        return None, None

    print(f"Successfully connected to database.")
    return connection, connection.cursor()


def check_table_exists(cursor, table_name: str, dbname: str) -> int:
    """
    Check if the given table name already exists in database.

    Args: 
        cursor: Database cursor object and the table name.
        table_name (str): Name of the table
        dbname (str): Name of the database

    Returns: 
        int: The number of tables in the database with the given table name.
            This can directly be used as a boolean!
    """

    cursor.execute(f"SELECT count(*) "
                   f"FROM information_schema.TABLES "
                   f"WHERE (TABLE_SCHEMA = '{dbname}') AND (TABLE_NAME = '{table_name}')")

    num_tables = cursor.fetchall()[0][0]
    print(num_tables)
    return num_tables


def create_new_table(cursor, table_name: str, table_column_names: list, table_column_types: list) -> bool:
    """
    Creates a new table in the mySQL database.

    Args: 
        cursor: A cursor object and the name of the table to be created.
        table_name (str): Name of the table
        table_column_names (list): list of columns names in the table must be passed.

    Returns: 
        bool: A boolean value indicating if a table was sucessfully created. 
    """
    if(len(table_column_names) != len(table_column_types)):
        raise Exception("Cannot create table. Type list and Field Name list are different lengths.")

    create_table_statement = f"CREATE TABLE {table_name} (\ntime_pt datetime,\n"

    for i in range(len(table_column_names)):
        create_table_statement += f"{table_column_names[i]} {table_column_types[i]} DEFAULT NULL,\n"

    create_table_statement += f"PRIMARY KEY (time_pt)\n"

    create_table_statement += ");"
    cursor.execute(create_table_statement)

    return True


def find_missing_columns(cursor, dataframe: pd.DataFrame, config_dict: dict, table_name: str):
    """
    Finds the column names which are not in the database table currently but are present
    in the pandas DataFrame to be written to the database. If communication with database
    is not possible, an empty list will be returned meaning no column will be added. 

    Args: 
        cursor: A cursor object and the name of the table to be created.
        dataframe (pd.DataFrame): the pandas DataFrame to be written into the mySQL server. 
        config_info (dict): The dictionary containing the configuration information 
        data_type (str): the header name corresponding to the table you wish to write data to.  

    Returns: 
        list: list of column names which must be added to the database table for the pandas 
        DataFrame to be properly written into the database. 
    """

    try:
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '"
                            f"{config_dict['database']['database']}' AND table_name = '"
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


def create_new_columns(cursor, table_name: str, new_columns: list, data_types: str):
    """
    Create the new, necessary column in the database. Catches error if communication with mysql database
    is not possible.

    Args: 
        cursor: A cursor object and the name of the table to be created.
        config_info (dict): The dictionary containing the configuration information.
        data_type (str): the header name corresponding to the table you wish to write data to.  
        new_columns (list): list of columns that must be added to the database table.

    Returns: 
        bool: boolean indicating if the the column were successfully added to the database. 
    """
    alter_table_statements = [f"ALTER TABLE {table_name} ADD COLUMN {column} {data_type} DEFAULT NULL;" for column, data_type in zip(new_columns, data_types)]

    for sql_statement in alter_table_statements:
        try:
            cursor.execute(sql_statement)
        except mysqlerrors.DatabaseError:
            print("Error communicating with the mysql database.")
            return False

    return True

def load_overwrite_database(cursor, dataframe: pd.DataFrame, config_info: dict, data_type: str):
    """
    Loads given pandas DataFrame into a mySQL table overwriting any conflicting data.

    Args: 
        cursor: A cursor object
        dataframe (pd.DataFrame): the pandas DataFrame to be written into the mySQL server. 
        config_info (dict): The dictionary containing the configuration information 
        data_type (str): the header name corresponding to the table you wish to write data to.  

    Returns: 
        bool: A boolean value indicating if the data was successfully written to the database. 
    """
    # Drop empty columns
    dataframe = dataframe.dropna(axis=1, how='all')

    dbname = config_info['database']['database']
    table_name = config_info[data_type]["table_name"]   
    
    if(len(dataframe.index) <= 0):
        print("Attempted to write to {table_name} but dataframe was empty.")
        return True

    print(f"Attempting to write data for {dataframe.index[0]} to {dataframe.index[-1]} into {table_name}")
    
    # Get string of all column names for sql insert
    sensor_names = "time_pt"
    sensor_types = ["datetime"]
    for column in dataframe.columns:
        sensor_names += "," + column    
        sensor_types.append(data_map[dataframe[column].dtype.name])

    # create SQL statement
    insert_str = "INSERT INTO " + table_name + " (" + sensor_names + ") VALUES ("
    for column in dataframe.columns:
        insert_str += "%s, "
    insert_str += "%s)"
    
    last_time = datetime.datetime.strptime('20/01/1990', "%d/%m/%Y") # arbitrary past date

    if not check_table_exists(cursor, table_name, dbname):
        if not create_new_table(cursor, table_name, sensor_names.split(",")[1:], sensor_types[1:]): #split on colums and remove first column aka time_pt
            print(f"Could not create new table {table_name} in database {dbname}")
            return False
    
    else: 
        try:
            cursor.execute(
                f"SELECT * FROM {table_name} ORDER BY time_pt DESC LIMIT 1")
            last_row_data = pd.DataFrame(cursor.fetchall())
            if len(last_row_data.index) != 0:
                last_time = last_row_data[0][0]
        except mysqlerrors.Error:
            print(f"Table {table_name} does has no data.")

        missing_cols, missing_types = find_missing_columns(cursor, dataframe, config_info, table_name)
        if len(missing_cols):
            if not create_new_columns(cursor, table_name, missing_cols, missing_types):
                print("Unable to add new column due to database error.")
    
    updatedRows = 0
    for index, row in dataframe.iterrows():
        time_data = row.values.tolist()
        #remove nans and infinites
        time_data = [None if (x is None or pd.isna(x)) else x for x in time_data]
        time_data = [None if (x == float('inf') or x == float('-inf')) else x for x in time_data]

        if(index <= last_time):
            statement, values = _generate_mysql_update(row, index, table_name)
            if statement != "":
                print("statement:", statement)
                cursor.execute(statement, values)
                updatedRows += 1
        else:
            cursor.execute(insert_str, (index, *time_data))

    print(f"Successfully wrote {len(dataframe.index)} rows to table {table_name} in database {dbname}. {updatedRows} existing rows were overwritten.")
    return True

def _generate_mysql_update(row, index, table_name):
    statement = f"UPDATE {table_name} SET "
    statment_elems = []
    values = []
    for column, value in row.items():
        if not value is None and not pd.isna(value) and not (value == float('inf') or value == float('-inf')):
            statment_elems.append(f"{column} = %s")
            values.append(value)

    if values:
        statement += ", ".join(statment_elems)
        statement += f" WHERE time_pt = '{index}';"
    else:
        statement = ""

    return statement, values