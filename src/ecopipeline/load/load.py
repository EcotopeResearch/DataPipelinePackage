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
from ecopipeline.load.Loader import Loader
from ecopipeline.load.AlarmLoader import AlarmLoader
from datetime import datetime, timedelta
import numpy as np

data_map = {'int64':'float',
            'int32':'float',
            'float64': 'float',
            'M8[ns]':'datetime',
            'datetime64[ns]':'datetime',
            'object':'varchar(25)',
            'bool': 'boolean'}

def central_load_function(config : ConfigManager, df : pd.DataFrame, hourly_df : pd.DataFrame, daily_df : pd.DataFrame, alarm_df : pd.DataFrame):
    """
    Dispatch all pipeline DataFrames to their respective database tables.

    Loads minute, hourly, and daily sensor data using :class:`~ecopipeline.load.Loader.Loader`,
    and alarm data using :class:`~ecopipeline.load.AlarmLoader.AlarmLoader`.
    Each DataFrame is only written when it is non-``None`` and non-empty.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    df : pd.DataFrame
        Minute-resolution sensor data. May be ``None``.
    hourly_df : pd.DataFrame
        Hourly-resolution sensor data. May be ``None``.
    daily_df : pd.DataFrame
        Daily-resolution sensor data. May be ``None``.
    alarm_df : pd.DataFrame
        Alarm records produced by the transform stage. May be ``None``.
    """
    print("++++++++++++ LOAD ++++++++++++")
    dbname = config.get_db_name()

    if alarm_df is not None and not alarm_df.empty:
        alarm_loader = AlarmLoader()
        alarm_loader.load_database(config, alarm_df, "alarm", dbname)

    if df is not None and not df.empty:
        minute_loader = Loader()
        minute_table = config.get_table_name("minute")
        minute_loader.load_database(config, df, minute_table, dbname)

    if hourly_df is not None and not hourly_df.empty:
        hourly_loader = Loader()
        hourly_table = config.get_table_name("hour")
        hourly_loader.load_database(config, hourly_df, hourly_table, dbname)

    if daily_df is not None and not daily_df.empty:
        daily_loader = Loader()
        daily_table = config.get_table_name("day")
        daily_loader.load_database(config, daily_df, daily_table, dbname)


def check_table_exists(cursor : mysql.connector.cursor.MySQLCursor, table_name: str, dbname: str) -> int:
    """
    Check whether a table exists in the database.

    Parameters
    ----------
    cursor : mysql.connector.cursor.MySQLCursor
        An active database cursor.
    table_name : str
        Name of the table to check.
    dbname : str
        Name of the database to search within.

    Returns
    -------
    int
        The count of tables matching ``table_name`` in ``dbname``.
        Evaluates to ``True`` when non-zero, so it can be used directly
        as a boolean.
    """

    cursor.execute(f"SELECT count(*) "
                   f"FROM information_schema.TABLES "
                   f"WHERE (TABLE_SCHEMA = '{dbname}') AND (TABLE_NAME = '{table_name}')")

    num_tables = cursor.fetchall()[0][0]
    return num_tables


def create_new_table(cursor : mysql.connector.cursor.MySQLCursor, table_name: str, table_column_names: list, table_column_types: list, primary_key: str = "time_pt", has_primary_key : bool = True) -> bool:
    """
    Create a new table in the MySQL database.

    Parameters
    ----------
    cursor : mysql.connector.cursor.MySQLCursor
        An active database cursor.
    table_name : str
        Name of the table to create.
    table_column_names : list
        Ordered list of column names (excluding the primary-key column).
    table_column_types : list
        Ordered list of MySQL type strings corresponding to
        ``table_column_names``. Must be the same length as
        ``table_column_names``.
    primary_key : str, optional
        Name of the primary-key column. Defaults to ``'time_pt'``.
    has_primary_key : bool, optional
        If ``False``, the ``primary_key`` column is added as a plain column
        rather than a PRIMARY KEY. Defaults to ``True``.

    Returns
    -------
    bool
        ``True`` if the table was successfully created.

    Raises
    ------
    Exception
        If ``table_column_names`` and ``table_column_types`` are different
        lengths.
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
    Identify DataFrame columns that are absent from the database table.

    If communication with the database fails, empty lists are returned so
    that the caller can continue without adding any columns.

    Parameters
    ----------
    cursor : mysql.connector.cursor.MySQLCursor
        An active database cursor.
    dataframe : pd.DataFrame
        The DataFrame whose columns are compared against the table schema.
    config_dict : dict
        Configuration dictionary containing at least a ``'database'`` key
        with the name of the MySQL database.
    table_name : str
        Name of the table to inspect.

    Returns
    -------
    list
        Column names present in ``dataframe`` but absent from the table.
    list
        Corresponding MySQL type strings for each missing column.
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
    Add new columns to an existing database table.

    Issues one ``ALTER TABLE … ADD COLUMN`` statement per column.
    Stops and returns ``False`` on the first database error.

    Parameters
    ----------
    cursor : mysql.connector.cursor.MySQLCursor
        An active database cursor.
    table_name : str
        Name of the table to alter.
    new_columns : list
        Ordered list of column names to add.
    data_types : str
        Ordered list of MySQL type strings corresponding to ``new_columns``.

    Returns
    -------
    bool
        ``True`` if all columns were added successfully; ``False`` if a
        database error occurred.
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
    Load a pandas DataFrame into a MySQL table using an UPSERT strategy.

    Existing rows are updated rather than replaced; NULL values in the
    incoming DataFrame will not overwrite existing non-NULL values in the
    database.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    dataframe : pd.DataFrame
        The DataFrame to be written into the MySQL server. The index must be
        the primary-key column (e.g. a datetime index named ``time_pt``).
    config_info : dict
        Configuration dictionary for the upload, obtainable via
        ``get_login_info()``. Must contain a ``'database'`` key and a nested
        dict keyed by ``data_type`` with a ``'table_name'`` entry (used when
        ``table_name`` is ``None``).
    data_type : str
        Key within ``config_info`` that identifies the target table section.
    primary_key : str, optional
        Column name used as the primary key. Defaults to ``'time_pt'``.
    table_name : str, optional
        Overrides the table name derived from ``config_info[data_type]``.
        Defaults to ``None``.
    auto_log_data_loss : bool, optional
        If ``True``, a DATA_LOSS_COP event is recorded when no data exists
        in the DataFrame for the last three days, or when an exception
        occurs. Defaults to ``False``.
    config_key : str, optional
        Key in ``config.ini`` that points to the minute-table data for the
        site; also used as the site name when reporting data loss. Defaults
        to ``'minute'``.

    Returns
    -------
    bool
        ``True`` if the data were successfully written; ``False`` otherwise.
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
    Load event records into the ``site_events`` MySQL table.

    Uses an UPSERT strategy so that existing automatically-uploaded rows can
    be updated while manually modified rows are left unchanged. If the
    DataFrame contains an ``alarm_type`` column the call is transparently
    redirected to :func:`load_alarms`.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    event_df : pd.DataFrame
        DataFrame of events to load. The index must be ``start_time_pt``.
        Required columns: ``end_time_pt``, ``event_type``, ``event_detail``.
        Optional column: ``variable_name``.
    site_name : str, optional
        Name of the site to associate events with. Defaults to
        ``config.get_site_name()``.

    Returns
    -------
    bool
        ``True`` if the data were successfully written; ``False`` if the
        table could not be created.

    Raises
    ------
    Exception
        If ``event_df`` is missing any of the required columns
        (``end_time_pt``, ``event_type``, ``event_detail``).
    """
    if event_df.empty:
        print("No events to load. DataFrame is empty.")
        return True
    if site_name is None:
        site_name = config.get_site_name()
    if 'alarm_type' in event_df.columns:
        print("Alarm dataframe detected... redirecting dataframe to load_alarms() function...")
        return load_alarms(config, event_df, site_name)
    # define constants
    proj_cop_filters = ['MV_COMMISSIONED','PLANT_COMMISSIONED','DATA_LOSS_COP','SYSTEM_MAINTENANCE','SYSTEM_TESTING']
    optim_cop_filters = ['MV_COMMISSIONED','PLANT_COMMISSIONED','DATA_LOSS_COP','INSTALLATION_ERROR_COP',
                            'PARTIAL_OCCUPANCY','SOO_PERIOD_COP','SYSTEM_TESTING','EQUIPMENT_MALFUNCTION',
                            'SYSTEM_MAINTENANCE']
    # Drop empty columns
    event_df = event_df.dropna(axis=1, how='all')

    dbname = config.get_db_name()
    table_name = "site_events"   
    
    if(len(event_df.index) <= 0):
        print(f"Attempted to write to {table_name} but dataframe was empty.")
        return True

    print(f"Attempting to write data for {event_df.index[0]} to {event_df.index[-1]} into {table_name}")
    
    # Get string of all column names for sql insert
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
    insert_str = "INSERT INTO " + table_name + " (" + column_names + ", variable_name, summary_filtered, optim_filtered, last_modified_date, last_modified_by) VALUES (%s, %s, %s,%s,%s,%s,%s,%s,'"+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"','automatic_upload')"

    if not 'variable_name' in event_df.columns:
        event_df['variable_name'] = None
    # add aditional columns for db creation
    full_column_names = column_names.split(",")[1:]
    full_column_names.append('last_modified_date')
    full_column_names.append('last_modified_by')
    full_column_names.append('variable_name')
    full_column_names.append('summary_filtered')
    full_column_names.append('optim_filtered')

    full_column_types = column_types[1:]
    full_column_types.append('datetime')
    full_column_types.append('varchar(60)')
    full_column_types.append('varchar(70)')
    full_column_types.append('tinyint(1)')
    full_column_types.append('tinyint(1)')

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
            time_data = [index,site_name,row['end_time_pt'],row['event_type'],row['event_detail'],row['variable_name'], row['event_type'] in proj_cop_filters, row['event_type'] in optim_cop_filters]
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
    Log a DATA_LOSS_COP event in the ``site_events`` table.

    Records that COP calculations have been affected by a data loss
    condition. If an open DATA_LOSS_COP event already exists for the
    given site, no duplicate is inserted.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    site_name : str, optional
        Name of the site to associate the event with. Defaults to the
        site name returned by ``config.get_site_name()``.

    Returns
    -------
    bool
        ``True`` if the event was logged (or already existed); ``False``
        if the ``site_events`` table does not exist.
    """
    # Drop empty columns

    dbname = config.get_db_name()
    table_name = "site_events"
    if site_name is None:
        site_name = config.get_site_name()
    error_string = "Error processing data. Please check logs to resolve."

    print(f"logging DATA_LOSS_COP into {table_name}")

    # create SQL statement
    insert_str = "INSERT INTO " + table_name + " (start_time_pt, site_name, event_detail, event_type, summary_filtered, optim_filtered, last_modified_date, last_modified_by) VALUES "
    insert_str += f"('{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}','{site_name}','{error_string}','DATA_LOSS_COP', true, true, '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}','automatic_upload')"

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
    Write daily data-quality statistics to the database.

    The destination table is named ``{daily_table_name}_stats`` unless
    ``custom_table_name`` is provided.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    daily_stats_df : pd.DataFrame
        DataFrame produced by ``create_data_statistics_df()`` in
        ``ecopipeline.transform``.
    config_daily_indicator : str, optional
        Key used to look up the daily table name in ``config.ini``.
        Defaults to ``'day'``.
    custom_table_name : str, optional
        Overrides the auto-generated ``{daily_table_name}_stats`` destination
        table name. When provided, ``config_daily_indicator`` is only used to
        supply ``config_info``; it no longer determines the table name.
        Defaults to ``None``.

    Returns
    -------
    bool
        ``True`` if the data were successfully written; ``False`` otherwise.
    """
    table_name = custom_table_name
    if table_name is None:
        table_name = f"{config.get_table_name(config_daily_indicator)}_stats"
    return load_overwrite_database(config, daily_stats_df, config.get_db_table_info([]), config_daily_indicator, table_name=table_name)

def _generate_mysql_update_event_table(row, id):
    """Build a parameterised MySQL UPDATE statement for a single ``site_events`` row."""
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
    """Build a parameterised MySQL UPDATE statement for a single DataFrame row."""
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


def load_alarms(config: ConfigManager, alarm_df: pd.DataFrame, site_name: str = None) -> bool:
    """
    Load alarm data into the ``alarm`` and ``alarm_inst`` tables.

    Processes the output of ``central_alarm_df_creator()``. For each alarm
    instance in the DataFrame the function:

    1. Checks whether a matching alarm record (same ``site_name``,
       ``alarm_type``, ``variable_name``) already exists within a three-day
       gap tolerance.
    2. Creates a new alarm record if none is found, or extends the date range
       of the nearest existing record.
    3. Inserts alarm instances into ``alarm_inst`` using certainty-based
       overlap resolution:

       - **Higher certainty** new alarm: the existing instance is split around
         the new one so each segment retains the highest available certainty.
       - **Lower certainty** new alarm: only the non-overlapping portions of
         the new alarm are inserted.
       - **Same certainty**: the existing instance is extended to encompass
         both time periods.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    alarm_df : pd.DataFrame
        DataFrame output from ``central_alarm_df_creator()``. Required columns:
        ``start_time_pt``, ``end_time_pt``, ``alarm_type``, ``variable_name``.
        Optional column: ``certainty`` (defaults to ``3`` if absent).
        Certainty scale: ``3`` = high, ``2`` = medium, ``1`` = low.
    site_name : str, optional
        Name of the site to associate alarms with. Defaults to
        ``config.get_site_name()``.

    Returns
    -------
    bool
        ``True`` if all alarms were loaded successfully; ``False`` if an
        exception occurred (transaction is rolled back).

    Raises
    ------
    Exception
        If ``alarm_df`` is missing any of the required columns.
    Exception
        If an alarm ID cannot be retrieved after insertion.
    """
    if alarm_df.empty:
        print("No alarms to load. DataFrame is empty.")
        return True

    # Validate required columns
    required_columns = ['start_time_pt', 'end_time_pt', 'alarm_type', 'variable_name']
    missing_columns = [col for col in required_columns if col not in alarm_df.columns]
    if missing_columns:
        raise Exception(f"alarm_df is missing required columns: {missing_columns}")

    # Sort by start_time_pt to process alarms in chronological order
    alarm_df = alarm_df.sort_values(by='start_time_pt').reset_index(drop=True)

    if site_name is None:
        site_name = config.get_site_name()

    dbname = config.get_db_name()
    alarm_table = "alarm"
    alarm_inst_table = "alarm_inst"

    connection, cursor = config.connect_db()

    try:
        # Check if tables exist
        if not check_table_exists(cursor, alarm_table, dbname):
            create_table_statement = """
                CREATE TABLE alarm (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    var_names_id VARCHAR(40),
                    start_time_pt DATETIME NOT NULL,
                    end_time_pt DATETIME NULL,
                    site_name VARCHAR(20),
                    alarm_type VARCHAR(20),
                    variable_name VARCHAR(70),
                    silenced BOOLEAN,
                    closing_event_id INT NULL,
                    snooze_until DATETIME NULL,
                    FOREIGN KEY (closing_event_id) REFERENCES site_events(id),
                    UNIQUE INDEX unique_alarm (site_name, alarm_type, variable_name, start_time_pt, end_time_pt)
                );
                """
            cursor.execute(create_table_statement)
        if not check_table_exists(cursor, alarm_inst_table, dbname):
            create_table_statement = """
            CREATE TABLE alarm_inst (
                inst_id INT AUTO_INCREMENT PRIMARY KEY,
                id INT,
                start_time_pt DATETIME NOT NULL,
                end_time_pt DATETIME NOT NULL,
                certainty INT NOT NULL,
                FOREIGN KEY (id) REFERENCES alarm(id)
            );
            """
            cursor.execute(create_table_statement)

        # Get existing alarms for this site
        cursor.execute(
            f"SELECT id, alarm_type, variable_name, start_time_pt, end_time_pt FROM {alarm_table} WHERE site_name = %s",
            (site_name,)
        )
        existing_alarms = cursor.fetchall()
        # Create lookup dict: (alarm_type, variable_name) -> list of (alarm_id, start_time, end_time)
        # Using a list because there can be multiple alarms with same type/variable but different date ranges
        alarm_lookup = {}
        for row in existing_alarms:
            key = (row[1], row[2])  # (alarm_type, variable_name)
            if key not in alarm_lookup:
                alarm_lookup[key] = []
            alarm_lookup[key].append({
                'id': row[0],
                'start_time': row[3],
                'end_time': row[4]
            })

        # SQL statements
        insert_alarm_sql = f"""
            INSERT INTO {alarm_table} (var_names_id, start_time_pt, end_time_pt, site_name, alarm_type, variable_name, silenced)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        update_alarm_dates_sql = f"""
            UPDATE {alarm_table} SET start_time_pt = %s, end_time_pt = %s WHERE id = %s
        """
        insert_inst_sql = f"""
            INSERT INTO {alarm_inst_table} (id, start_time_pt, end_time_pt, certainty)
            VALUES (%s, %s, %s, %s)
        """
        update_inst_sql = f"""
            UPDATE {alarm_inst_table} SET start_time_pt = %s, end_time_pt = %s WHERE inst_id = %s
        """
        delete_inst_sql = f"""
            DELETE FROM {alarm_inst_table} WHERE inst_id = %s
        """

        new_alarms = 0
        updated_alarms = 0
        new_instances = 0
        updated_instances = 0
        max_gap_days = 3

        for _, row in alarm_df.iterrows():
            start_time = row['start_time_pt']
            end_time = row['end_time_pt']
            alarm_type = row['alarm_type']
            variable_name = row['variable_name']
            certainty = row.get('certainty', 3)  # Default to high certainty if not specified

            lookup_key = (alarm_type, variable_name)
            alarm_id = None

            if lookup_key in alarm_lookup:
                # Find matching alarm based on date range logic
                for alarm_record in alarm_lookup[lookup_key]:
                    alarm_start = alarm_record['start_time']
                    alarm_end = alarm_record['end_time']

                    # Case 1: Alarm dates encapsulate row dates - just use this alarm
                    if alarm_start <= start_time and alarm_end >= end_time:
                        alarm_id = alarm_record['id']
                        break

                    # Calculate gap between date ranges
                    if end_time < alarm_start:
                        gap = (alarm_start - end_time).days
                    elif start_time > alarm_end:
                        gap = (start_time - alarm_end).days
                    else:
                        gap = 0  # Overlapping

                    # Case 2: Overlapping or within 3 days - extend the alarm dates
                    if gap <= max_gap_days:
                        alarm_id = alarm_record['id']
                        new_start = min(alarm_start, start_time)
                        new_end = max(alarm_end, end_time)

                        # Only update if dates actually changed
                        if new_start != alarm_start or new_end != alarm_end:
                            cursor.execute(update_alarm_dates_sql, (new_start, new_end, alarm_id))
                            # Update the lookup cache
                            alarm_record['start_time'] = new_start
                            alarm_record['end_time'] = new_end
                            updated_alarms += 1
                        break

                # Case 3: No matching alarm found (gap > 3 days for all existing alarms)
                # Will create a new alarm below

            if alarm_id is None:
                # Create new alarm record
                cursor.execute(insert_alarm_sql, (
                    "No ID",  # TODO add actual ID?
                    start_time,
                    end_time,
                    site_name,
                    alarm_type,
                    variable_name,
                    False  # silenced = False by default
                ))
                # Retrieve the ID from database to handle concurrent inserts safely
                cursor.execute(
                    f"""SELECT id FROM {alarm_table}
                        WHERE site_name = %s AND alarm_type = %s AND variable_name = %s
                        AND start_time_pt = %s AND end_time_pt = %s""",
                    (site_name, alarm_type, variable_name, start_time, end_time)
                )
                result = cursor.fetchone()
                if result is None:
                    raise Exception(f"Failed to retrieve alarm ID after insert for {alarm_type}/{variable_name}")
                alarm_id = result[0]
                # Add to lookup cache
                if lookup_key not in alarm_lookup:
                    alarm_lookup[lookup_key] = []
                alarm_lookup[lookup_key].append({
                    'id': alarm_id,
                    'start_time': start_time,
                    'end_time': end_time
                })
                new_alarms += 1

            # Get existing alarm instances for this alarm_id that might overlap
            cursor.execute(
                f"""SELECT inst_id, start_time_pt, end_time_pt, certainty
                    FROM {alarm_inst_table}
                    WHERE id = %s AND start_time_pt <= %s AND end_time_pt >= %s""",
                (alarm_id, end_time, start_time)
            )
            existing_instances = cursor.fetchall()

            # Track segments of the new alarm to insert (may be split by higher-certainty existing alarms)
            new_segments = [(start_time, end_time, certainty)]

            for existing in existing_instances:
                existing_inst_id, existing_start, existing_end, existing_certainty = existing

                # Process each new segment against this existing instance
                updated_segments = []
                for seg_start, seg_end, seg_certainty in new_segments:
                    # Check if there's overlap
                    if seg_end <= existing_start or seg_start >= existing_end:
                        # No overlap, keep segment as is
                        updated_segments.append((seg_start, seg_end, seg_certainty))
                        continue

                    # There is overlap - handle based on certainty comparison
                    if existing_certainty < seg_certainty:
                        # Case 1: New alarm has higher certainty - split existing around new
                        # Part before new alarm (if any)
                        if existing_start < seg_start:
                            cursor.execute(update_inst_sql, (existing_start, seg_start, existing_inst_id))
                            updated_instances += 1
                            # Insert the part after new alarm (if any)
                            if existing_end > seg_end:
                                cursor.execute(insert_inst_sql, (alarm_id, seg_end, existing_end, existing_certainty))
                                new_instances += 1
                        elif existing_end > seg_end:
                            # No part before, but there's a part after
                            cursor.execute(update_inst_sql, (seg_end, existing_end, existing_inst_id))
                            updated_instances += 1
                        else:
                            # Existing is completely encompassed by new - delete it
                            cursor.execute(delete_inst_sql, (existing_inst_id,))
                        # Keep the new segment as is
                        updated_segments.append((seg_start, seg_end, seg_certainty))

                    elif existing_certainty > seg_certainty:
                        # Case 2: Existing has higher certainty - trim new segment to non-overlapping parts
                        # Part before existing (if any)
                        if seg_start < existing_start:
                            updated_segments.append((seg_start, existing_start, seg_certainty))
                        # Part after existing (if any)
                        if seg_end > existing_end:
                            updated_segments.append((existing_end, seg_end, seg_certainty))
                        # The overlapping part of new segment is discarded

                    else:
                        # Case 3: Same certainty - merge to encompass both
                        merged_start = min(seg_start, existing_start)
                        merged_end = max(seg_end, existing_end)
                        cursor.execute(update_inst_sql, (merged_start, merged_end, existing_inst_id))
                        updated_instances += 1
                        # Remove this segment from new_segments (it's been merged into existing)
                        # Don't add to updated_segments

                new_segments = updated_segments

            # Insert any remaining new segments
            for seg_start, seg_end, seg_certainty in new_segments:
                if seg_start < seg_end:  # Only insert valid segments
                    cursor.execute(insert_inst_sql, (alarm_id, seg_start, seg_end, seg_certainty))
                    new_instances += 1

        connection.commit()
        print(f"Successfully loaded alarms: {new_alarms} new alarm records, {updated_alarms} updated alarm records, {new_instances} new instances, {updated_instances} updated instances.")
        return True

    except Exception as e:
        print(f"Error loading alarms: {e}")
        connection.rollback()
        return False

    finally:
        cursor.close()
        connection.close()