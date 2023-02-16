import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd
import os
import numpy as np
import datetime
from ecotope_package_cs2306.config import _config_directory
pd.set_option('display.max_columns', None)

def getLoginInfo(table_headers: list, config_info : str = _config_directory) -> dict:
    """
    Function will and return config.ini in a config var.

    Output: Login information
    """

    config_info = "config.ini"

    if not os.path.exists(config_info):
        print(f"File path '{config_info}' does not exist.")
        sys.exit()

    configure = configparser.ConfigParser()
    configure.read(config_info)
    #TODO: Please Generalize -Carlos
    db_connection_info = {
        "database": {'user': configure.get('database', 'user'),
                     'password': configure.get('database', 'password'),
                     'host': configure.get('database', 'host'),
                     'database': configure.get('database', 'database')}
    }

    db_table_info = {header: {"table_name": configure.get('minute', 'table_name'), 
                  "sensor_list": configure.get('minute', 'sensor_list').split(',')} for header in table_headers}
    
    db_connection_info.update(db_table_info)

    print(f"Successfully fetched configuration information from file path {config_info}.")
    return db_connection_info
    

def connectDB(config_info: dict):
    """
    Function will use login information to try and connect to the database and return a
    connection object to make a cursor.
    Input: _getLoginInfo
    Output: Connection object
    """

    connection = None

    try:
        connection = mysql.connector.connect(**config_info)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            sys.exit()
        else:
            print(err)
            sys.exit()

    print(f"Successfully connected to database.")
    return connection, connection.cursor()


def checkTableExists(cursor, table_name: str, dbname: str) -> int:
    """
    Check if given table exists in database.

    :param cursor: Database cursor object.
    :param config_info: configuration dictionary containing config info.
    :return: Boolean value representing if table exists in database.
    """

    cursor.execute(f"SELECT count(*) "
                   f"FROM information_schema.TABLES "
                   f"WHERE (TABLE_SCHEMA = '{dbname}') AND (TABLE_NAME = '{table_name}')")

    num_tables = cursor.fetchall()[0][0]
    return num_tables


def createNewTable(cursor, table_name: str, table_column_names: list) -> bool:
    """
    Creates a new table to store data in the given dataframe.

    :param cursor: Database cursor object.
    :param dataframe: Pandas data frame.
    :param table_name: Name of table in database.
    :return: Boolean value representing if new table was created.
    """

    create_table_statement = f"CREATE TABLE {table_name} (\ntime datetime,\n"

    for sensor in table_column_names:
        create_table_statement += f"{sensor} float default 0.0,\n"

    create_table_statement += f"PRIMARY KEY (time)\n"

    create_table_statement += ");"
    cursor.execute(create_table_statement)

    return True


def loadDatabase(cursor, dataframe, config_info: dict, data_type: str):
    """
    Loads data stored in passed dataframe into mySQL database using.

    :param data_type: One of ["pump", "weather", "load_shift", "cop"]
    :param cursor: Database cursor object.
    :param dataframe: Pandas dataframe object.
    :param config_info: configuration dictionary containing config info.
    :return: Boolean value representing if data was written to database.
    """

    dbname = config_info['database']['database']
    table_name = config_info[data_type]["table_name"]
    sensor_names = config_info[data_type]['sensor_list']

    if not checkTableExists(cursor, table_name, dbname):
        if not createNewTable(cursor, table_name, config_info[data_type]['sensor_list']):
            print(f"Could not create new table {table_name} in database {dbname}")
            sys.exit()

    date_values = dataframe.index
    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(dataframe.columns)).replace("[", "").replace("]", "").replace("'", "")
        sensor_data = str(list(time_data.values)).replace("[", "").replace("]", "")

        query = f"INSERT INTO {table_name} (time, {sensor_names})\n" \
                f"VALUES ('{date}', {sensor_data})"

        cursor.execute(query)

    print(f"Successfully wrote data frame to table {table_name} in database {dbname}.")


if __name__ == '__main__':
    config_file_path = "Configuration/config.ini"
    df_path = "input/ecotope_wide_data.csv"

    table_list = ["minute", "hour", "day"]
    # get database connection information and desired table name to write data into
    config_dict = getLoginInfo(table_list, config_file_path)
    print(config_dict)

    # establish connection to database
    # db_connection, db_cursor = connectDB(config_info=config_dict["database"])

    """
    ecotope_data = pd.read_pickle("C:/Users/emilx/Downloads/post_process.pkl")
    ecotope_data.replace(np.NaN, 0, inplace=True)
    weather_data = pd.read_pickle("C:/Users/emilx/Downloads/noaa.pkl")
    weather_data.set_index(["time"], inplace=True)
    weather_data = weather_data["2023-01-10 16:00:00-08:00":"2023-01-11 15:00:00-08:00"]
    weather_data.drop(["conditions"], axis=1, inplace=True)
    weather_data.fillna(0, inplace=True)
    """
    # print(weather_data)
    # print(ecotope_data.columns)

    # load data stored in data frame to database
    # loadDatabase(cursor=db_cursor, dataframe=weather_data, config_info=config_dict, data_type="weather")
    # loadDatabase(cursor=db_cursor, dataframe=ecotope_data, config_info=config_dict, data_type="minute")

    # commit changes to database and close connections
    # db_connection.commit()
    # db_connection.close()
    # db_cursor.close()
