import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd
import os

config_file_path = "config.ini"


def getLoginInfo(config_file_path: str) -> dict:
    """
    Function will read login information from config.ini and return it in a config var.

    Input: config file
    Output: Login information
    """

    file_path = f"Configuration/{config_file_path}"
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
        "table": {"tablename": configure.get('table', 'tablename'),
                  "sitename": configure.get('table', 'sitename')}

    }

    print(f"Successfully fetched configuration information from file path {config_file_path}.")
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


def checkTableExists(cursor, config_info: dict) -> int:
    """
    Check if given table exists in database.

    :param cursor: Database cursor object.
    :param config_info: configuration dictionary containing config info.
    :return: Boolean value representing if table exists in database.
    """

    tablename = config_info['table']['tablename']
    dbname = config_info['database']['database']

    cursor.execute(f"SELECT count(*) "
                   f"FROM information_schema.TABLES "
                   f"WHERE (TABLE_SCHEMA = '{dbname}') AND (TABLE_NAME = '{tablename}')")

    num_tables = cursor.fetchall()[0][0]
    return num_tables


def createNewTable(cursor, dataframe: str, config_info: dict) -> bool:
    """
    Creates a new table to store data in the given dataframe.

    :param cursor: Database cursor object.
    :param dataframe: Pandas data frame.
    :param tablename: Name of table in database.
    :return: Boolean value representing if new table was created.
    """

    table_name = config_info['table']['tablename']
    sensors = dataframe.columns

    create_table_statement = f"CREATE TABLE {table_name} (\n" \
                             f"time_pt datetime,\n" \
                             f"site char(7),\n"

    for sensor in sensors[:-1]:
        create_table_statement += f"{sensor} float,\n"

    create_table_statement += f"{sensors[len(sensors) - 1]} float\n"
    create_table_statement += ");"

    cursor.execute(create_table_statement)

    return True


def loadDatabase(cursor, dataframe: str, config_info: dict):
    """
    Loads data stored in passed dataframe into mySQL database using.

    :param cursor: Database cursor object.
    :param dataframe: Pandas dataframe object.
    :param config_info: configuration dictionary containing config info.
    :return: Boolean value representing if data was written to database.
    """

    tablename = config_info['table']['tablename']
    sitename = config_info['table']['sitename']
    dbname = config_info['database']['database']

    if not checkTableExists(cursor, config_info):
        if not createNewTable(cursor, dataframe, config_info):
            print(f"Could not create new table {tablename} in database {dbname}")
            sys.exit()

    date_values = dataframe.index
    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(time_data.index.get_level_values(0))).replace("'", "").replace("[", "").replace("]", "")
        sensor_data = str(list(time_data.values)).replace("[", "").replace("]", "")

        query = f"INSERT INTO {tablename} (time_pt, site, {sensor_names})\n" \
                f"VALUES ('{date}', '{sitename}', {sensor_data})"

        cursor.execute(query)

    print(f"Successfully wrote data frame to table {tablename} in database {dbname}.")


if __name__ == '__main__':
    # get database connection information and desired table name to write data into
    config_dict = getLoginInfo(config_file_path=config_file_path)

    # establish connection to database
    db_connection, db_cursor = connectDB(config_info=config_dict['database'])

    df_path = "input/ecotope_wide_data.csv"
    ecotope_data = pd.read_csv(df_path)
    ecotope_data.set_index("time", inplace=True)

    # load data stored in data frame to database
    loadDatabase(cursor=db_cursor, dataframe=ecotope_data, config_info=config_dict)

    # commit changes to database and close connections
    db_connection.commit()
    db_connection.close()
    db_cursor.close()
