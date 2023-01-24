import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd
import os
import numpy as np
import datetime

pd.set_option('display.max_columns', None)


def getLoginInfo(file_path: str) -> dict:
    """
    Function will read login information from config.ini and return it in a config var.

    Input: config file
    Output: Login information
    """

    if not os.path.exists(file_path):
        print(f"File path '{file_path}' does not exist.")
        sys.exit()

    configure = configparser.ConfigParser()
    configure.read(file_path)
    config = {
        "database": {'user': configure.get('database', 'user'),
                     'password': configure.get('database', 'password'),
                     'host': configure.get('database', 'host'),
                     'database': configure.get('database', 'database')},
        "sensor_table": {"table_name": configure.get('sensor_table', 'table_name')},
        "weather_table": {"table_name": configure.get('weather_table', 'table_name')}
    }

    print(f"Successfully fetched configuration information from file path {file_path}.")
    return config


def connectDB(config_info: dict):
    """
    Function will use login information to try and connect to the database and return a
    connection object to make a cursor.
    Input: _getLoginInfo
    Output: Connection object
    """

    dbname = config_info['database']

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

    print(f"Successfully connected to {dbname}.")
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


def createNewTable(cursor, dataframe: str, table_name: str, table_name_weather: str=None) -> bool:
    """
    Creates a new table to store data in the given dataframe.

    :param cursor: Database cursor object.
    :param dataframe: Pandas data frame.
    :param table_name: Name of table in database.
    :return: Boolean value representing if new table was created.
    """

    sensors = dataframe.columns

    create_table_statement = f"CREATE TABLE {table_name} (\ntime_pt datetime,\n"

    if table_name_weather is not None:
        create_table_statement += f"time_hour_pt datetime,\n"

    for sensor in sensors:
        create_table_statement += f"{sensor} float,\n"

    if table_name_weather is not None:
        create_table_statement += f"PRIMARY KEY (time_pt),\n"
        create_table_statement += f"FOREIGN KEY (time_hour_pt) REFERENCES {table_name_weather}(time_pt)\n"
    else:
        create_table_statement += f"PRIMARY KEY (time_pt)\n"

    create_table_statement += ");"
    cursor.execute(create_table_statement)

    return True


def createUnknownColumns(cursor, column_names: list, table_name):
    for column in column_names:
        cursor.execute(f"select * from INFORMATION_SCHEMA.COLUMNS where table_name = "
                          f"'{table_name}' and column_name = '{column}'")
        column_exists = len(db_cursor.fetchall())

        if not column_exists:
            db_cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} FLOAT NOT NULL;")


def loadDatabase(cursor, dataframe: str, config_info: dict, is_sensor: bool):
    """
    Loads data stored in passed dataframe into mySQL database using.

    :param cursor: Database cursor object.
    :param dataframe: Pandas dataframe object.
    :param config_info: configuration dictionary containing config info.
    :return: Boolean value representing if data was written to database.
    """

    dbname = config_info['database']['database']

    if is_sensor:
        table_name = config_info['sensor_table']['table_name']

        if not checkTableExists(cursor, table_name, dbname):
            if not createNewTable(cursor, dataframe, table_name, config_info["weather_table"]['table_name']):
                print(f"Could not create new table {table_name} in database {dbname}")
                sys.exit()

    else:
        table_name = config_info['weather_table']['table_name']

        if not checkTableExists(cursor, table_name, dbname):
            if not createNewTable(cursor, dataframe, table_name, None):
                print(f"Could not create new table {table_name} in database {dbname}")
                sys.exit()

    createUnknownColumns(db_cursor, dataframe.columns, table_name)

    date_values = dataframe.index
    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(time_data.index.get_level_values(0))).replace("'", "").replace("[", "").replace("]", "")
        sensor_data = str(list(time_data.values)).replace("[", "").replace("]", "")

        if is_sensor:
            query = f"INSERT INTO {table_name} (time_pt, time_hour_pt, {sensor_names})\n" \
                    f"VALUES ('{date}', '{date.replace(minute=0, second=0)}', {sensor_data})"
        else:
            query = f"INSERT INTO {table_name} (time_pt, {sensor_names})\n" \
                    f"VALUES ('{date}', {sensor_data})"

        cursor.execute(query)

    print(f"Successfully wrote data frame to table {table_name} in database {dbname}.")


if __name__ == '__main__':
    config_file_path = "config.ini"
    df_path = "input/ecotope_wide_data.csv"

    # get database connection information and desired table name to write data into
    config_dict = getLoginInfo(config_file_path=config_file_path)

    # establish connection to database
    db_connection, db_cursor = connectDB(config_info=config_dict['database'])

    ecotope_data = pd.read_csv(df_path)
    ecotope_data.set_index("time", inplace=True)
    ecotope_data.index = pd.to_datetime(ecotope_data.index)

    weather_data = pd.read_csv("output/727935-24234.csv").head(1)
    weather_data.index = pd.to_datetime(weather_data['time'])
    weather_data.drop(['conditions', 'time'], axis=1, inplace=True)
    weather_data.replace(np.nan, 0.0, inplace=True)
    weather_data.index = [datetime.datetime(year=2022, month=10, day=13, hour=17)]

    # load data stored in data frame to database
    loadDatabase(cursor=db_cursor, dataframe=ecotope_data, config_info=config_dict, is_sensor=True)

    # commit changes to database and close connections
    db_connection.commit()
    db_connection.close()
    db_cursor.close()
