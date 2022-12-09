import configparser
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd

CONFIG_ERROR_CODE = -1
DB_ERROR_CODE = -2
TABLE_ERROR_CODE = -3
config_file_path = "config.ini"


def getLoginInfo(config_file_path): 
    """
    Function will read login information from config.ini and return it in a config var.

    Input: config file
    Output: Login information
    """
    configure = configparser.ConfigParser()
    configure.read(config_file_path)
    config = {
        "database": {'user': configure.get('database', 'user'),
                     'password': configure.get('database', 'password'),
                     'host': configure.get('database', 'host'),
                     'database': configure.get('database', 'database')},
        "table": {"tablename": configure.get('table', 'tablename'),
                  "sitename": configure.get('table', 'sitename')}

    }
    return config


def connectDB(config):
    """
    Function will use login information to try and connect to the database and return a
    connection object to make a cursor.
    Input: _getLoginInfo
    Output: Connection object
    """
    connection = mysql.connector.connect(**config)
    return connection, connection.cursor()


def checkTableExists(cursor, tablename, dbname):
    """
    Check if given table exists in database.

    :param cursor: Database cursor object.
    :param tablename: Name of table in database.
    :param dbname: Name of database.
    :return: Boolean value representing if table exists in database.
    """

    cursor.execute(f"SELECT count(*) "
                   f"FROM information_schema.TABLES "
                   f"WHERE (TABLE_SCHEMA = '{dbname}') AND (TABLE_NAME = '{tablename}')")

    num_tables = cursor.fetchall()[0][0]
    return num_tables


def createNewTable(cursor, dataframe, tablename):
    """
    Creates a new table to store data in the given dataframe.

    :param cursor: Database cursor object.
    :param dataframe: Pandas data frame.
    :param tablename: Name of table in database.
    :return: Boolean value representing if new table was created.
    """

    sensors = dataframe.index.get_level_values(1).unique()

    create_table_statement = f"CREATE TABLE {tablename} (\n" \
                             f"time_pt datetime,\n" \
                             f"site char(7),\n"

    for sensor in sensors[:-1]:
        create_table_statement += f"{sensor} float,\n"

    create_table_statement += f"{sensors[len(sensors) - 1]} float\n"
    create_table_statement += ");"

    cursor.execute(create_table_statement)

    return True


def loadDatabase(cursor, dataframe, dbname, tablename, sitename):
    """
    Loads data stored in passed dataframe into mySQL database using.

    :param cursor: Database cursor object.
    :param dataframe: Pandas dataframe object.
    :param tablename: Name of table in database.
    :param dbname: Name of database.
    :param sitename: Name of site.
    :return: Boolean value representing if data was written to database.
    """

    if not checkTableExists(cursor, tablename, dbname):
        if not createNewTable(cursor, dataframe, tablename):
            return TABLE_ERROR_CODE

    date_values = dataframe.index.get_level_values(0).unique()

    for date in date_values:
        time_data = dataframe.loc[date]
        sensor_names = str(list(time_data.index.get_level_values(0))).replace("'", "").replace("[", "").replace("]", "")
        sensor_data = str(list(time_data['data'].values)).replace("[", "").replace("]", "")

        query = f"INSERT INTO {tablename} (time_pt, site, {sensor_names})\n" \
                f"VALUES ('{date}', '{sitename}', {sensor_data})"
        cursor.execute(query)

    return True


if __name__ == '__main__':
    # get database connection information and desired table name to write data into
    config_dict = getLoginInfo(config_file_path)

    # check if configuration file was properly read
    if config_dict == CONFIG_ERROR_CODE:
        print(f"File path '{config_file_path}' does not exist.")
        sys.exit()

    else:
        print(f"Successfully fetched configuration information from file path {config_file_path}.")

    # establish connection to database
    db_connection, db_cursor = connectDB(config_dict['database'])

    # check if connection was established to database
    if db_connection == DB_ERROR_CODE:
        error = db_cursor
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password.")
        else:
            print(error)
        sys.exit()

    else:
        print(f"Successfully connected to {config_dict['database']['database']}.")

    # load sample database
    df = pd.read_csv('ecotope_data.csv')
    df.time = pd.to_datetime(df.time)
    df.set_index(['time', 'id'], inplace=True)

    # load data stored in data frame to database
    status = loadDatabase(db_cursor, df, config_dict['database']['database'], config_dict['table']['tablename'],
                          config_dict['table']['sitename'])

    # check if data was written to database successfully
    if status:
        print(f"Successfully wrote data frame to table {config_dict['table']['tablename']} in database "
              f"{config_dict['database']['database']}.")

    elif status == TABLE_ERROR_CODE:
        print(f"Could not create new table {config_dict['table']['tablename']} in database "
              f"{config_dict['database']['database']}")

    else:
        print(f"Unable to write data frame to table {config_dict['table']['tablename']} in database "
              f"{config_dict['database']['database']}.")

    # commit changes to database and close connection
    db_connection.commit()
    db_cursor.close()
