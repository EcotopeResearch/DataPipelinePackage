import configparser
import mysql.connector
import mysql.connector.cursor
import sys
import pandas as pd
import os
import math
import mysql.connector.errors as mysqlerrors
from ecopipeline import ConfigManager
from datetime import datetime, timedelta
import numpy as np

class Loader:
    """
    Base class for loading pandas DataFrames into a MySQL database.

    Provides UPSERT-based loading, table creation, column management, and
    data-loss reporting utilities used by all concrete loader subclasses.

    Attributes
    ----------
    data_map : dict
        Mapping from pandas dtype name strings to MySQL column type strings.
    """

    def __init__(self):
        self.data_map = {
            'int64':'float',
            'int32':'float',
            'float64': 'float',
            'M8[ns]':'datetime',
            'datetime64[ns]':'datetime',
            'object':'varchar(25)',
            'bool': 'boolean'
        }

    def load_database(self, config : ConfigManager, dataframe: pd.DataFrame, table_name: str, dbname : str, auto_log_data_loss : bool = False, primary_key : str = 'time_pt'):
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
            The pandas DataFrame to be written into the MySQL server. The index
            must be the primary-key column (e.g. a datetime index named ``time_pt``).
        table_name : str
            Name of the destination table in the database.
        dbname : str
            Name of the MySQL database that contains ``table_name``.
        auto_log_data_loss : bool, optional
            If ``True``, a DATA_LOSS_COP event is recorded when no data exists
            in the DataFrame for the last three days, or when an exception
            occurs. Defaults to ``False``.
        primary_key : str, optional
            Column name used as the primary key in the destination table.
            Defaults to ``'time_pt'``.

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
            
            if(len(dataframe.index) <= 0):
                print(f"Attempted to write to {table_name} but dataframe was empty.")
                ret_value = True
            else:

                print(f"Attempting to write data for {dataframe.index[0]} to {dataframe.index[-1]} into {table_name}")
                if auto_log_data_loss and dataframe.index[-1] < datetime.now() - timedelta(days=3):
                    self.report_data_loss(config, config.get_site_name())
                
                # Get string of all column names for sql insert
                sensor_names = primary_key
                sensor_types = ["datetime"]
                for column in dataframe.columns:
                    sensor_names += "," + column    
                    sensor_types.append(self.data_map[dataframe[column].dtype.name])

                # create SQL statement
                insert_str = "INSERT INTO " + table_name + " (" + sensor_names + ") VALUES ("
                for column in dataframe.columns:
                    insert_str += "%s, "
                insert_str += "%s)"
                
                existing_rows_list = []

                # create db table if it does not exist, otherwise add missing columns to existing table
                if not self.check_table_exists(cursor, table_name, dbname):
                    if not self.create_new_table(cursor, table_name, sensor_names.split(",")[1:], sensor_types[1:], primary_key=primary_key): #split on colums and remove first column aka time_pt
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

                    missing_cols, missing_types = self.find_missing_columns(cursor, dataframe, dbname, table_name)
                    if len(missing_cols):
                        if not self.create_new_columns(cursor, table_name, missing_cols, missing_types):
                            print("Unable to add new columns due to database error.")
                
                updatedRows = 0
                for index, row in dataframe.iterrows():
                    time_data = row.values.tolist()
                    #remove nans and infinites
                    time_data = [None if (x is None or pd.isna(x)) else x for x in time_data]
                    time_data = [None if (x == float('inf') or x == float('-inf')) else x for x in time_data]

                    if index in existing_rows_list:
                        statement, values = self._generate_mysql_update(row, index, table_name, primary_key)
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
                self.report_data_loss(config, config.get_site_name())
            ret_value = False

        db_connection.close()
        cursor.close()
        return ret_value
    
    def report_data_loss(self, config : ConfigManager, site_name : str = None):
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
        if not self.check_table_exists(cursor, table_name, dbname):
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

    def check_table_exists(self, cursor : mysql.connector.cursor.MySQLCursor, table_name: str, dbname: str) -> int:
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
    
    def create_new_table(self, cursor : mysql.connector.cursor.MySQLCursor, table_name: str, table_column_names: list, table_column_types: list, primary_key: str = "time_pt", has_primary_key : bool = True) -> bool:
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
            ``table_column_names``.  Must be the same length as
            ``table_column_names``.
        primary_key : str, optional
            Name of the primary-key column. Defaults to ``'time_pt'``.
        has_primary_key : bool, optional
            If ``False``, the ``primary_key`` column is added as a plain
            column rather than a PRIMARY KEY. Defaults to ``True``.

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
    
    def find_missing_columns(self, cursor : mysql.connector.cursor.MySQLCursor, dataframe: pd.DataFrame, dbname: str, table_name: str):
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
        dbname : str
            Name of the database that contains ``table_name``.
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
                                f"{dbname}' AND table_name = '"
                                f"{table_name}'")
        except mysqlerrors.DatabaseError as e:
            print("Check if the mysql table to be written to exists.", e)
            return [], []
        
        current_table_names = list(cursor.fetchall())
        current_table_names = [name[0] for name in current_table_names]
        df_names = list(dataframe.columns)
        
        cols_to_add = [sensor_name for sensor_name in df_names if sensor_name not in current_table_names]
        data_types = [dataframe[column].dtype.name for column in cols_to_add]
        
        data_types = [self.data_map[data_type] for data_type in data_types]
        
        return cols_to_add, data_types


    def create_new_columns(self, cursor : mysql.connector.cursor.MySQLCursor, table_name: str, new_columns: list, data_types: str):
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
            Ordered list of MySQL type strings corresponding to
            ``new_columns``.

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
    
    def _generate_mysql_update(self, row, index, table_name, primary_key):
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