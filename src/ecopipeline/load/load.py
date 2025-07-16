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
from datetime import datetime, timedelta
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


def create_new_table(cursor : mysql.connector.cursor.MySQLCursor, table_name: str, table_column_names: list, table_column_types: list, primary_key: str = "time_pt", has_primary_key : bool = True) -> bool:
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
    primary_key: str
        The name of the primary index of the table. Should be a datetime. If has_primary_key is set to False, this will just be a column not a key.
    has_primary_key : bool
        Set to False if the table should not establish a primary key. Defaults to True

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
    if has_primary_key:
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
    except mysqlerrors.DatabaseError as e:
        print("Check if the mysql table to be written to exists.", e)
        return [], []
    
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
        except mysqlerrors.DatabaseError as e:
            print(f"Error communicating with the mysql database: {e}")
            return False

    return True

def load_overwrite_database(config : ConfigManager, dataframe: pd.DataFrame, config_info: dict, data_type: str, 
                            primary_key: str = "time_pt", table_name: str = None, auto_log_data_loss : bool = False,
                            config_key : str = "minute"):
    """
    Loads given pandas DataFrame into a MySQL table overwriting any conflicting data. Uses an UPSERT strategy to ensure any gaps in data are filled.
    Note: will not overwrite values with NULL. Must have a new value to overwrite existing values in database

    Parameters
    ----------  
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    dataframe: pd.DataFrame
        The pandas DataFrame to be written into the mySQL server.
    config_info: dict
        The dictionary containing the configuration information in the data upload. This can be aquired through the get_login_info() function in this package
    data_type: str
        The header name corresponding to the table you wish to write data to. 
    primary_key : str
        The name of the primary key in the database to upload to. Default as 'time_pt'
    table_name : str
        overwrites table name from config_info if needed
    auto_log_data_loss : bool
        if set to True, a data loss event will be reported if no data exits in the dataframe 
        for the last two days from the current date OR if an error occurs
    config_key : str 
        The key in the config.ini file that points to the minute table data for the site. The name of this table is also the site name.
        

    Returns
    ------- 
    bool: 
        A boolean value indicating if the data was successfully written to the database. 
    """
    # Database Connection
    db_connection, cursor = config.connect_db()
    try:

        # Drop empty columns
        dataframe = dataframe.dropna(axis=1, how='all')

        dbname = config_info['database']
        if table_name == None:
            table_name = config_info[data_type]["table_name"]   
        
        if(len(dataframe.index) <= 0):
            print(f"Attempted to write to {table_name} but dataframe was empty.")
            ret_value = True
        else:

            print(f"Attempting to write data for {dataframe.index[0]} to {dataframe.index[-1]} into {table_name}")
            if auto_log_data_loss and dataframe.index[-1] < datetime.now() - timedelta(days=3):
                report_data_loss(config, config.get_site_name(config_key))
            
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
            
            # last_time = datetime.strptime('20/01/1990', "%d/%m/%Y") # arbitrary past date
            existing_rows_list = []

            # create db table if it does not exist, otherwise add missing columns to existing table
            if not check_table_exists(cursor, table_name, dbname):
                if not create_new_table(cursor, table_name, sensor_names.split(",")[1:], sensor_types[1:], primary_key=primary_key): #split on colums and remove first column aka time_pt
                    ret_value = False
                    raise Exception(f"Could not create new table {table_name} in database {dbname}")
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

            db_connection.commit()
            print(f"Successfully wrote {len(dataframe.index)} rows to table {table_name} in database {dbname}. {updatedRows} existing rows were overwritten.")
            ret_value = True
    except Exception as e:
        print(f"Unable to load data into database. Exception: {e}")
        if auto_log_data_loss:
            report_data_loss(config, config.get_site_name(config_key))
        ret_value = False

    db_connection.close()
    cursor.close()
    return ret_value


def load_event_table(config : ConfigManager, event_df: pd.DataFrame, site_name : str = None):
    """
    Loads given pandas DataFrame into a MySQL table overwriting any conflicting data. Uses an UPSERT strategy to ensure any gaps in data are filled.
    Note: will not overwrite values with NULL. Must have a new value to overwrite existing values in database

    Parameters
    ----------  
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    event_df: pd.DataFrame
        The pandas DataFrame to be written into the mySQL server. Must have columns event_type and event_detail 
    site_name : str
        the name of the site to correspond the events with. If left blank will default to minute table name

    Returns
    ------- 
    bool: 
        A boolean value indicating if the data was successfully written to the database. 
    """
    # Drop empty columns
    event_df = event_df.dropna(axis=1, how='all')

    dbname = config.get_db_name()
    table_name = "site_events"   
    
    if(len(event_df.index) <= 0):
        print(f"Attempted to write to {table_name} but dataframe was empty.")
        return True

    print(f"Attempting to write data for {event_df.index[0]} to {event_df.index[-1]} into {table_name}")
    
    # Get string of all column names for sql insert
    if site_name is None:
        site_name = config.get_site_name()
    column_names = f"start_time_pt,site_name"
    column_types = ["datetime","varchar(25)","datetime",
                    "ENUM('MISC_EVENT','DATA_LOSS','DATA_LOSS_COP','SITE_VISIT','SYSTEM_MAINTENANCE','EQUIPMENT_MALFUNCTION','PARTIAL_OCCUPANCY','INSTALLATION_ERROR','ALARM','SILENT_ALARM','MV_COMMISSIONED','PLANT_COMMISSIONED','INSTALLATION_ERROR_COP','SOO_PERIOD','SOO_PERIOD_COP','SYSTEM_TESTING')",
                    "varchar(800)"]
    column_list = ['end_time_pt','event_type', 'event_detail']
    if not set(column_list).issubset(event_df.columns):
        raise Exception(f"event_df should contain a dataframe with columns start_time_pt, end_time_pt, event_type, and event_detail. Instead, found dataframe with columns {event_df.columns}")

    for column in column_list:
        column_names += "," + column

    # create SQL statement
    insert_str = "INSERT INTO " + table_name + " (" + column_names + ", variable_name, last_modified_date, last_modified_by) VALUES (%s,%s,%s,%s,%s,%s,'"+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"','automatic_upload')"

    if not 'variable_name' in event_df.columns:
        event_df['variable_name'] = None
    # add aditional columns for db creation
    full_column_names = column_names.split(",")[1:]
    full_column_names.append('last_modified_date')
    full_column_names.append('last_modified_by')
    full_column_names.append('variable_name')
    full_column_types = column_types[1:]
    full_column_types.append('datetime')
    full_column_types.append('varchar(60)')
    full_column_types.append('varchar(70)')


    existing_rows = pd.DataFrame({
        'start_time_pt' : [],
        'end_time_pt' : [],
        'event_type' : [],
        'variable_name' : [],
        'last_modified_by' : []
    })

    connection, cursor = config.connect_db() 

    # create db table if it does not exist, otherwise add missing columns to existing table
    if not check_table_exists(cursor, table_name, dbname):
        if not create_new_table(cursor, table_name, full_column_names, full_column_types, primary_key='start_time_pt', has_primary_key=False): #split on colums and remove first column aka time_pt
            print(f"Could not create new table {table_name} in database {dbname}")
            return False
    else:
        try:
            # find existing times in database for upsert statement
            cursor.execute(
                f"SELECT id, start_time_pt, end_time_pt, event_detail, event_type, variable_name, last_modified_by FROM {table_name} WHERE start_time_pt >= '{event_df.index.min()}' AND site_name = '{site_name}'")
            # Fetch the results into a DataFrame
            existing_rows = pd.DataFrame(cursor.fetchall(), columns=['id','start_time_pt', 'end_time_pt', 'event_detail', 'event_type', 'variable_name', 'last_modified_by'])
            existing_rows['start_time_pt'] = pd.to_datetime(existing_rows['start_time_pt'])
            existing_rows['end_time_pt'] = pd.to_datetime(existing_rows['end_time_pt'])

        except mysqlerrors.Error as e:
            print(f"Retrieving data from {table_name} caused exception: {e}")
    
    updatedRows = 0
    ignoredRows = 0
    try:
        for index, row in event_df.iterrows():
            time_data = [index,site_name,row['end_time_pt'],row['event_type'],row['event_detail'],row['variable_name']]
            #remove nans and infinites
            time_data = [None if (x is None or pd.isna(x)) else x for x in time_data]
            time_data = [None if (x == float('inf') or x == float('-inf')) else x for x in time_data]
            filtered_existing_rows = existing_rows[
                (existing_rows['start_time_pt'] == index) &
                (existing_rows['event_type'] == row['event_type'])
            ]
            if not time_data[-1] is None and not filtered_existing_rows.empty:
                # silent alarm only
                filtered_existing_rows = filtered_existing_rows[(filtered_existing_rows['variable_name'] == row['variable_name']) &
                                                                (filtered_existing_rows['event_detail'].str[:20] == row['event_detail'][:20])] # the [:20] part is a bug fix for partial days for silent alarms 

            if not filtered_existing_rows.empty:
                first_matching_row = filtered_existing_rows.iloc[0]  # Retrieves the first row
                statement, values = _generate_mysql_update_event_table(row, first_matching_row['id'])
                if statement != "" and first_matching_row['last_modified_by'] == 'automatic_upload':
                    cursor.execute(statement, values)
                    updatedRows += 1
                else:
                    ignoredRows += 1
            else:
                cursor.execute(insert_str, time_data)
        connection.commit()
        print(f"Successfully wrote {len(event_df.index) - ignoredRows} rows to table {table_name} in database {dbname}. {updatedRows} existing rows were overwritten.")
    except Exception as e:
        # Print the exception message
        print(f"Caught an exception when uploading to site_events table: {e}")
    connection.close()
    cursor.close()
    return True

def report_data_loss(config : ConfigManager, site_name : str = None):
    """
    Logs data loss event in event database (assumes one exists) as a DATA_LOSS_COP event to 
    note that COP calculations have been effected

    Parameters
    ----------  
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    site_name : str
        the name of the site to correspond the events with. If left blank will default to minute table name

    Returns
    ------- 
    bool: 
        A boolean value indicating if the data was successfully written to the database. 
    """
    # Drop empty columns

    dbname = config.get_db_name()
    table_name = "site_events"
    if site_name is None:
        site_name = config.get_site_name()
    error_string = "Error processing data. Please check logs to resolve."

    print(f"logging DATA_LOSS_COP into {table_name}")

    # create SQL statement
    insert_str = "INSERT INTO " + table_name + " (start_time_pt, site_name, event_detail, event_type, last_modified_date, last_modified_by) VALUES "
    insert_str += f"('{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}','{site_name}','{error_string}','DATA_LOSS_COP','{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}','automatic_upload')"

    existing_rows = pd.DataFrame({
        'id' : []
    })

    connection, cursor = config.connect_db() 

    # create db table if it does not exist, otherwise add missing columns to existing table
    if not check_table_exists(cursor, table_name, dbname):
        print(f"Cannot log data loss. {table_name} does not exist in database {dbname}")
        return False
    else:
        try:
            # find existing times in database for upsert statement
            cursor.execute(
                f"SELECT id FROM {table_name} WHERE end_time_pt IS NULL AND site_name = '{site_name}' AND event_type = 'DATA_LOSS_COP'")
            # Fetch the results into a DataFrame
            existing_rows = pd.DataFrame(cursor.fetchall(), columns=['id'])

        except mysqlerrors.Error as e:
            print(f"Retrieving data from {table_name} caused exception: {e}")
    try:
        
        if existing_rows.empty:
            cursor.execute(insert_str)
            connection.commit()
            print("Successfully logged data loss.")
        else:
            print("Data loss already logged.")
    except Exception as e:
        # Print the exception message
        print(f"Caught an exception when uploading to site_events table: {e}")
    connection.close()
    cursor.close()
    return True

def load_data_statistics(config : ConfigManager, daily_stats_df : pd.DataFrame, config_daily_indicator : str = "day", custom_table_name : str = None):
    """
    Logs data statistics for the site in a table with name "{daily table name}_stats"

    Parameters
    ----------  
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    daily_stats_df : pd.DataFrame
        dataframe created by the create_data_statistics_df() function in ecopipeline.transform
    config_daily_indicator : str
        the indicator of the daily_table name in the config.ini file of the data pipeline
    custom_table_name : str
        custom table name for data statistics. Overwrites the name "{daily table name}_stats" to your custom name. 
        In this sense config_daily_indicator's pointer is no longer used. 

    Returns
    ------- 
    bool: 
        A boolean value indicating if the data was successfully written to the database. 
    """
    table_name = custom_table_name
    if table_name is None:
        table_name = f"{config.get_table_name(config_daily_indicator)}_stats"
    return load_overwrite_database(config, daily_stats_df, config.get_db_table_info([]), config_daily_indicator, table_name=table_name)

def _generate_mysql_update_event_table(row, id):
    statement = f"UPDATE site_events SET "
    statment_elems = []
    values = []
    for column, value in row.items():
        if not value is None and not pd.isna(value) and not (value == float('inf') or value == float('-inf')):
            statment_elems.append(f"{column} = %s")
            values.append(value)

    if values:
        statement += ", ".join(statment_elems)
        statement += f", last_modified_by = 'automatic_upload', last_modified_date = '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"
        statement += f" WHERE id = {id};"
        # statement += f" WHERE start_time_pt = '{start_time_pt}' AND end_time_pt = '{end_time_pt}' AND event_type = '{event_type}' AND site_name = '{site_name}';"
    else:
        statement = ""

    return statement, values

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