import configparser
import mysql.connector
import sys
import pandas as pd
import os
from ecotope_package_cs2306.config import _config_directory
pd.set_option('display.max_columns', None)

def get_login_info(table_headers: list, config_info : str = _config_directory) -> dict:
    """
    Reads the config.ini file stored in the config_info file path.   

    Args: 
        table_headers (list): A list of table headers. These headers must correspond to the 
            section headers in the config.ini file. Your list must contain the section
            header for each table you wish to write into. 
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

    db_table_info = {header: {"table_name": configure.get(header, 'table_name'), 
                  "sensor_list": list(configure.get(header, 'sensor_list').split(','))} for header in table_headers}
    
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
    return num_tables


def create_new_table(cursor, table_name: str, table_column_names: list) -> bool:
    """
    Creates a new table in the mySQL database.

    Args: 
        cursor: A cursor object and the name of the table to be created.
        table_name (str): Name of the table
        table_column_names (list): list of columns names in the table must be passed.

    Returns: 
        bool: A boolean value indicating if a table was sucessfully created. 
    """

    create_table_statement = f"CREATE TABLE {table_name} (\ntime datetime,\n"

    for sensor in table_column_names:
        create_table_statement += f"{sensor} float default 0.0,\n"

    create_table_statement += f"PRIMARY KEY (time)\n"

    create_table_statement += ");"
    cursor.execute(create_table_statement)

    return True


def load_database(cursor, dataframe: pd.DataFrame, config_info: dict, data_type: str):
    """
    Loads given pandas DataFrame into a mySQL table.

    Args: 
        cursor: A cursor object
        dataframe (pd.DataFrame): the pandas DataFrame to be written into the mySQL server. 
        config_info (dict): The dictionary containing the configuration information 
        data_type (str): the header name corresponding to the table you wish to write data to.  

    Returns: 
        bool: A boolean value indicating if the data was successfully written to the database. 
    """

    dbname = config_info['database']['database']
    table_name = config_info[data_type]["table_name"]
    sensor_names = config_info[data_type]['sensor_list']

    if not check_table_exists(cursor, table_name, dbname):
        if not create_new_table(cursor, table_name, config_info[data_type]['sensor_list']):
            print(f"Could not create new table {table_name} in database {dbname}")
            return False

    date_values = dataframe.index
    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(dataframe.columns)).replace("[", "").replace("]", "").replace("'", "")
        sensor_data = str(list(time_data.values)).replace("[", "").replace("]", "")

        query = f"INSERT INTO {table_name} (time, {sensor_names})\n" \
                f"VALUES ('{date}', {sensor_data})"

        cursor.execute(query)

    print(f"Successfully wrote data frame to table {table_name} in database {dbname}.")
    return True