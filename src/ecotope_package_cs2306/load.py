import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd
import os
import numpy as np
import datetime

pd.set_option('display.max_columns', None)

def set_config(cfg : str = "Configuration/config.ini"):
    """
    Accessor function to set input directory in the format {directory}/
    Defaults to input/
    Input: String of relative directory
    """
    global _config_directory 
    _config_directory = cfg
    return _config_directory
set_config()
def getLoginInfo(config_info : str = _config_directory) -> dict:
    """
    Function will and return config.ini in a config var.

    Output: Login information
    """

    if not os.path.exists(config_info):
        print(f"File path '{config_info}' does not exist.")
        sys.exit()

    configure = configparser.ConfigParser()
    configure.read(config_info)
    config = {
        "database": {'user': configure.get('database', 'user'),
                     'password': configure.get('database', 'password'),
                     'host': configure.get('database', 'host'),
                     'database': configure.get('database', 'database')},
        "pump": {"table_name": configure.get('pump', 'table_name'),
                       "sensor_list": configure.get('pump', 'sensor_list').split(','),
                       "foreign_key": configure.get('pump', 'foreign_key').split(" ") if configure.get('pump', 'foreign_key') != 'None' else False,
                       "ref_table": configure.get('pump', 'referenced_table').split(" ") if configure.get('pump', 'referenced_table') != 'None' else False,
                       "ref_column": configure.get('pump', 'referenced_column').split(" ") if configure.get('pump', 'referenced_column') != 'None' else False},
        "weather": {"table_name": configure.get('weather', 'table_name'),
                        "sensor_list": configure.get('weather', 'sensor_list').split(','),
                        "foreign_key": configure.get('weather', 'foreign_key').split(" ") if configure.get('weather', 'foreign_key') != 'None' else [],
                        "ref_table": configure.get('weather', 'referenced_table').split(" ") if configure.get('weather', 'referenced_table') != 'None' else [],
                        "ref_column": configure.get('weather', 'referenced_column').split(" ") if configure.get('weather', 'referenced_column') != 'None' else []}
    }

    print(
        f"Successfully fetched configuration information from file path {config_info}.")
    return config
    

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


def createNewTable(cursor, table_name: str, table_column_names: list, foreign_info: dict) -> bool:
    """
    Creates a new table to store data in the given dataframe.

    :param cursor: Database cursor object.
    :param dataframe: Pandas data frame.
    :param table_name: Name of table in database.
    :return: Boolean value representing if new table was created.
    """

    num_foreign_keys = len(foreign_info)

    create_table_statement = f"CREATE TABLE {table_name} (\ntime datetime,\n"

    if num_foreign_keys:
        for foreign_key_info in foreign_info:
            fk_name, _, _ = foreign_key_info
            create_table_statement += f"{fk_name} datetime,\n"

    for sensor in table_column_names:
        create_table_statement += f"{sensor} float,\n"

    if num_foreign_keys:
        for foreign_key_info in foreign_info:
            fk_name, ref_table, ref_column = foreign_key_info
            create_table_statement += f"FOREIGN KEY ({fk_name}) REFERENCES {ref_table}({ref_column}),\n"
        create_table_statement += f"PRIMARY KEY (time)\n"
    else:
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

    num_foreign_keys = len(config_info[data_type]['foreign_key'])
    foreign_info = list()
    if num_foreign_keys:
        for fk in range(num_foreign_keys):
            foreign_info.append((config_info[data_type]['foreign_key'][fk], config_info[data_type]['ref_table'][fk], config_info[data_type]['ref_column'][fk]))

    if not checkTableExists(cursor, table_name, dbname):
        if not createNewTable(cursor, table_name, config_info[data_type]['sensor_list'], foreign_info):
            print(f"Could not create new table {table_name} in database {dbname}")
            sys.exit()

    date_values = dataframe.index
    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(dataframe.columns)).replace("[", "").replace("]", "").replace("'", "")
        sensor_data = str(list(time_data.values)).replace("[", "").replace("]", "")

        if num_foreign_keys:
            query = f"INSERT INTO {table_name} (time, "
            for fk in range(num_foreign_keys-1):
                query += f"{config_info[data_type]['foreign_key'][fk]}"
            query += f"{config_info[data_type]['foreign_key'][num_foreign_keys-1]}, {sensor_names})\n"
            query += f"VALUES ('{date}', '{date.replace(minute=0, second=0)}', {sensor_data})"

        else:
            query = f"INSERT INTO {table_name} (time, {sensor_names})\n" \
                    f"VALUES ('{date}', {sensor_data})"

        cursor.execute(query)

    print(f"Successfully wrote data frame to table {table_name} in database {dbname}.")


if __name__ == '__main__':
    config_file_path = "Configuration/config.ini"
    df_path = "input/ecotope_wide_data.csv"

    # get database connection information and desired table name to write data into
    config_dict = getLoginInfo(config_file_path)

    # establish connection to database
    db_connection, db_cursor = connectDB(config_info=config_dict["database"])

    ecotope_data = pd.read_pickle("C:/Users/emilx/Downloads/post_process.pkl")
    ecotope_data.replace(np.NaN, 0, inplace=True)
    weather_data = pd.read_pickle("C:/Users/emilx/Downloads/noaa.pkl")
    weather_data.set_index(["time"], inplace=True)
    weather_data = weather_data["2023-01-10 16:00:00-08:00":"2023-01-11 15:00:00-08:00"]
    weather_data.drop(["conditions"], axis=1, inplace=True)
    weather_data.fillna(0, inplace=True)

    # print(weather_data)
    # print(ecotope_data)

    # load data stored in data frame to database
    loadDatabase(cursor=db_cursor, dataframe=weather_data, config_info=config_dict, data_type="weather")
    loadDatabase(cursor=db_cursor, dataframe=ecotope_data, config_info=config_dict, data_type="pump")

    # commit changes to database and close connections
    db_connection.commit()
    db_connection.close()
    db_cursor.close()
