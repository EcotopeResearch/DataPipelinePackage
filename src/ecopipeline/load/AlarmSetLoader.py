import mysql.connector.cursor
import mysql.connector.errors as mysqlerrors
import pandas as pd
from ecopipeline import ConfigManager
from ecopipeline.load.Loader import Loader


class AlarmSetLoader(Loader):
    """
    Loader subclass for writing alarm-set configuration data to the
    ``alarm_set`` MySQL table.

    Unlike :class:`~ecopipeline.load.AlarmLoader.AlarmLoader`, this loader
    does not merge or reconcile overlapping records. Each call to
    :meth:`load_database` replaces all existing rows for the site with the
    contents of the given DataFrame.
    """

    def create_new_table(self, cursor: mysql.connector.cursor.MySQLCursor, table_name: str,
                         table_column_names: list = None, table_column_types: list = None,
                         primary_key: str = None, has_primary_key: bool = False) -> bool:
        """
        Create the ``alarm_set`` table.

        Parameters
        ----------
        cursor : mysql.connector.cursor.MySQLCursor
            An active database cursor.
        table_name : str
            Name of the table to create.
        table_column_names : list, optional
            Ignored; the alarm_set schema is fixed. Retained for interface
            compatibility with the parent class.
        table_column_types : list, optional
            Ignored; the alarm_set schema is fixed. Retained for interface
            compatibility with the parent class.
        primary_key : str, optional
            Ignored; the alarm_set schema has no primary key. Retained for
            interface compatibility with the parent class.
        has_primary_key : bool, optional
            Ignored; the alarm_set schema has no primary key. Retained for
            interface compatibility with the parent class.

        Returns
        -------
        bool
            Always returns ``True`` after executing the DDL statement.
        """
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                site_name VARCHAR(20),
                alarm_type VARCHAR(20),
                vars VARCHAR(120)
            )
        """)

        return True

    def load_database(self, config: ConfigManager, alarm_set_df: pd.DataFrame, table_name: str = "alarm_set",
                      dbname: str = None, site_name: str = None) -> bool:
        """
        Replace the ``alarm_set`` rows for a site with the contents of a DataFrame.

        For each call, all existing rows in ``alarm_set`` matching the site
        are deleted, then every row of ``alarm_set_df`` is inserted.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object that holds configuration data for the pipeline.
        alarm_set_df : pd.DataFrame
            DataFrame of alarm-set definitions to load. Required columns:
            ``alarm_type``, ``variables``.
        table_name : str, optional
            Name of the destination table. Defaults to ``'alarm_set'``.
        dbname : str, optional
            Name of the MySQL database. Defaults to ``config.get_db_name()``.
        site_name : str, optional
            Site name to associate rows with. Defaults to
            ``config.get_site_name()``.

        Returns
        -------
        bool
            ``True`` if all rows were loaded successfully; ``False`` if an
            exception occurred (transaction is rolled back).

        Raises
        ------
        Exception
            If ``alarm_set_df`` is missing any of the required columns.
        """
        if alarm_set_df.empty:
            print("No alarm sets to load. DataFrame is empty.")
            return True

        required_columns = ['alarm_type', 'variables']
        missing_columns = [col for col in required_columns if col not in alarm_set_df.columns]
        if missing_columns:
            raise Exception(f"alarm_set_df is missing required columns: {missing_columns}")

        if dbname is None:
            dbname = config.get_db_name()

        if site_name is None:
            site_name = config.get_site_name()

        connection, cursor = config.connect_db()
        try:
            if not self.check_table_exists(cursor, table_name, dbname):
                self.create_new_table(cursor, table_name)

            cursor.execute(f"DELETE FROM {table_name} WHERE site_name = %s", (site_name,))

            insert_sql = f"""
                INSERT INTO {table_name} (site_name, alarm_type, vars)
                VALUES (%s, %s, %s)
            """
            for _, row in alarm_set_df.iterrows():
                cursor.execute(insert_sql, (site_name, row['alarm_type'], row['variables']))

            connection.commit()
            print(f"Successfully loaded {len(alarm_set_df.index)} alarm set rows for site {site_name}.")
            return True

        except Exception as e:
            print(f"Error loading alarm sets: {e}")
            connection.rollback()
            return False

        finally:
            cursor.close()
            connection.close()
