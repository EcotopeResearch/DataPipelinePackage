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
        "pump": {"table_name": configure.get('pump', 'table_name'),
                       "sensor_list": configure.get('pump', 'sensor_list').replace("[", "").replace("]", "").replace(" ", "").replace('"', "").split(',')},
        "weather": {"table_name": configure.get('weather', 'table_name'),
                          "sensor_list": configure.get('weather', 'sensor_list').replace("[", "").replace("]", "").replace(" ", "").replace('"', "").split(',')}
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


def createNewTable(cursor, table_name: str, table_column_names: list, foreign_table_name: str) -> bool:
    """
    Creates a new table to store data in the given dataframe.

    :param cursor: Database cursor object.
    :param dataframe: Pandas data frame.
    :param table_name: Name of table in database.
    :return: Boolean value representing if new table was created.
    """

    create_table_statement = f"CREATE TABLE {table_name} (\ntime_pt datetime,\n"

    if foreign_table_name is not None:
        create_table_statement += f"time_hour_pt datetime,\n"

    for sensor in table_column_names:
        create_table_statement += f"{sensor} float,\n"

    if foreign_table_name is not None:
        create_table_statement += f"PRIMARY KEY (time_pt),\n"
        create_table_statement += f"FOREIGN KEY (time_hour_pt) REFERENCES {foreign_table_name}(time_pt)\n"
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


def loadDatabase(cursor, dataframe: str, config_info: dict, data_type: str):
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

    weather_table_name = None
    foreign_key = False
    if data_type == "pump":
        foreign_key = True
        weather_table_name = config_info['weather']['table_name']

    if not checkTableExists(cursor, table_name, dbname):
        if not createNewTable(cursor, table_name, config_info[data_type]['sensor_list'], weather_table_name):
            print(f"Could not create new table {table_name} in database {dbname}")
            sys.exit()

    date_values = dataframe.index
    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(dataframe.columns)).replace("[", "").replace("]", "").replace("'", "")
        sensor_data = str(list(time_data.values)).replace("[", "").replace("]", "")

        if foreign_key:
            query = f"INSERT INTO {table_name} (time_pt, time_hour_pt, {sensor_names})\n" \
                    f"VALUES ('{date}', '{date.replace(minute=0, second=0)}', {sensor_data})"
        else:
            query = f"INSERT INTO {table_name} (time_pt, {sensor_names})\n" \
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

    """
    ecotope_data = pd.read_csv(df_path)
    ecotope_data.set_index("time", inplace=True)
    ecotope_data.index = pd.to_datetime(ecotope_data.index)

    weather_data = pd.read_csv("output/727935-24234.csv").head(1)
    weather_data.index = pd.to_datetime(weather_data['time'])
    weather_data.drop(['conditions', 'time'], axis=1, inplace=True)
    weather_data.replace(np.nan, 0.0, inplace=True)
    weather_data.index = [datetime.datetime(year=2022, month=10, day=13, hour=17)]
    """

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
    loadDatabase(cursor=db_cursor, dataframe=ecotope_data, config_info=config_dict, data_type="pump")

    # commit changes to database and close connections
    db_connection.commit()
    db_connection.close()
    db_cursor.close()
