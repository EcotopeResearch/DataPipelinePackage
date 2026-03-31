import mysql.connector.cursor
import mysql.connector.errors as mysqlerrors
import pandas as pd
from ecopipeline import ConfigManager
from ecopipeline.load.Loader import Loader


class AlarmLoader(Loader):
    """
    Loader subclass for writing alarm data to the ``alarm`` and ``alarm_inst``
    MySQL tables.

    Overrides :class:`Loader` table-existence checks and table-creation logic
    so that the paired ``alarm`` / ``alarm_inst`` tables are always created and
    validated together.
    """

    def check_table_exists(self, cursor: mysql.connector.cursor.MySQLCursor, table_name: str, dbname: str) -> bool:
        """
        Return ``True`` only when both the alarm table and its ``_inst`` companion exist.

        Both tables must be present before loading can proceed; if either is
        missing they should both be (re)created together.

        Parameters
        ----------
        cursor : mysql.connector.cursor.MySQLCursor
            An active database cursor.
        table_name : str
            Base name of the alarm table (e.g. ``'alarm'``).
        dbname : str
            Name of the database to search within.

        Returns
        -------
        bool
            ``True`` if both ``table_name`` and ``{table_name}_inst`` exist;
            ``False`` otherwise.
        """
        alarm_exists = super().check_table_exists(cursor, table_name, dbname)
        alarm_inst_exists = super().check_table_exists(cursor, f"{table_name}_inst", dbname)
        return bool(alarm_exists and alarm_inst_exists)

    def create_new_table(self, cursor: mysql.connector.cursor.MySQLCursor, table_name: str,
                         table_column_names: list = None, table_column_types: list = None,
                         primary_key: str = "time_pt", has_primary_key: bool = True) -> bool:
        """
        Create both the alarm table and its ``_inst`` companion table.

        Uses ``CREATE TABLE IF NOT EXISTS`` so the method is safe to call even
        when only one of the two tables is missing.

        Parameters
        ----------
        cursor : mysql.connector.cursor.MySQLCursor
            An active database cursor.
        table_name : str
            Base name for the alarm table. The instance table is derived as
            ``{table_name}_inst``.
        table_column_names : list, optional
            Ignored; the alarm schema is fixed. Retained for interface
            compatibility with the parent class.
        table_column_types : list, optional
            Ignored; the alarm schema is fixed. Retained for interface
            compatibility with the parent class.
        primary_key : str, optional
            Ignored; the alarm schema uses a fixed auto-increment ``id``.
            Retained for interface compatibility with the parent class.
        has_primary_key : bool, optional
            Ignored; the alarm schema always defines a primary key. Retained
            for interface compatibility with the parent class.

        Returns
        -------
        bool
            Always returns ``True`` after executing the DDL statements.
        """
        alarm_inst_table = f"{table_name}_inst"

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
            )
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {alarm_inst_table} (
                inst_id INT AUTO_INCREMENT PRIMARY KEY,
                id INT,
                start_time_pt DATETIME NOT NULL,
                end_time_pt DATETIME NOT NULL,
                certainty INT NOT NULL,
                FOREIGN KEY (id) REFERENCES {table_name}(id)
            )
        """)

        return True

    def load_database(self, config: ConfigManager, alarm_df: pd.DataFrame, table_name: str, dbname: str,
                      auto_log_data_loss: bool = False, primary_key: str = "time_pt",
                      site_name: str = None) -> bool:
        """
        Load alarm data into the alarm and alarm_inst tables.

        For each alarm instance in the DataFrame the method:

        1. Checks whether a matching alarm record (same ``site_name``,
           ``alarm_type``, ``variable_name``) already exists within a
           three-day gap tolerance.
        2. Creates a new alarm record if none is found, or extends the
           date range of the nearest existing record.
        3. Inserts alarm instances into ``{table_name}_inst`` using
           certainty-based overlap resolution:

           - **Higher certainty** new alarm: the existing instance is split
             around the new one so each segment retains the highest available
             certainty.
           - **Lower certainty** new alarm: only the non-overlapping portions
             of the new alarm are inserted.
           - **Same certainty**: the existing instance is extended to
             encompass both time periods.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object that holds configuration data for the
            pipeline.
        alarm_df : pd.DataFrame
            DataFrame of alarms to load. Required columns: ``start_time_pt``,
            ``end_time_pt``, ``alarm_type``, ``variable_name``. Optional
            column: ``certainty`` (defaults to ``3`` if absent).
            Certainty scale: ``3`` = high, ``2`` = medium, ``1`` = low.
        table_name : str
            Base name of the alarm table. The companion instance table is
            derived as ``{table_name}_inst``.
        dbname : str
            Name of the MySQL database.
        auto_log_data_loss : bool, optional
            Unused in this subclass; retained for interface compatibility.
            Defaults to ``False``.
        primary_key : str, optional
            Unused in this subclass; retained for interface compatibility.
            Defaults to ``'time_pt'``.
        site_name : str, optional
            Site name to associate alarms with. Defaults to
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

        required_columns = ['start_time_pt', 'end_time_pt', 'alarm_type', 'variable_name']
        missing_columns = [col for col in required_columns if col not in alarm_df.columns]
        if missing_columns:
            raise Exception(f"alarm_df is missing required columns: {missing_columns}")

        alarm_df = alarm_df.sort_values(by='start_time_pt').reset_index(drop=True)

        if site_name is None:
            site_name = config.get_site_name()

        alarm_inst_table = f"{table_name}_inst"

        connection, cursor = config.connect_db()
        try:
            if not self.check_table_exists(cursor, table_name, dbname):
                self.create_new_table(cursor, table_name)

            # SQL statements
            insert_alarm_sql = f"""
                INSERT INTO {table_name} (var_names_id, start_time_pt, end_time_pt, site_name, alarm_type, variable_name, silenced)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            update_alarm_dates_sql = f"""
                UPDATE {table_name} SET start_time_pt = %s, end_time_pt = %s WHERE id = %s
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

            # Build lookup: (alarm_type, variable_name) -> list of {id, start_time, end_time}
            cursor.execute(
                f"SELECT id, alarm_type, variable_name, start_time_pt, end_time_pt FROM {table_name} WHERE site_name = %s",
                (site_name,)
            )
            alarm_lookup = {}
            for row in cursor.fetchall():
                key = (row[1], row[2])
                alarm_lookup.setdefault(key, []).append({
                    'id': row[0],
                    'start_time': row[3],
                    'end_time': row[4]
                })

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
                certainty = row.get('certainty', 3)

                lookup_key = (alarm_type, variable_name)
                alarm_id = None

                if lookup_key in alarm_lookup:
                    for alarm_record in alarm_lookup[lookup_key]:
                        alarm_start = alarm_record['start_time']
                        alarm_end = alarm_record['end_time']

                        # Case 1: existing alarm encapsulates new instance
                        if alarm_start <= start_time and alarm_end >= end_time:
                            alarm_id = alarm_record['id']
                            break

                        # Gap between date ranges
                        if end_time < alarm_start:
                            gap = (alarm_start - end_time).days
                        elif start_time > alarm_end:
                            gap = (start_time - alarm_end).days
                        else:
                            gap = 0  # overlapping

                        # Case 2: overlapping or within 3 days — extend the alarm
                        if gap <= max_gap_days:
                            alarm_id = alarm_record['id']
                            new_start = min(alarm_start, start_time)
                            new_end = max(alarm_end, end_time)
                            if new_start != alarm_start or new_end != alarm_end:
                                cursor.execute(update_alarm_dates_sql, (new_start, new_end, alarm_id))
                                alarm_record['start_time'] = new_start
                                alarm_record['end_time'] = new_end
                                updated_alarms += 1
                            break

                if alarm_id is None:
                    cursor.execute(insert_alarm_sql, (
                        "No ID", start_time, end_time, site_name, alarm_type, variable_name, False
                    ))
                    cursor.execute(
                        f"""SELECT id FROM {table_name}
                            WHERE site_name = %s AND alarm_type = %s AND variable_name = %s
                            AND start_time_pt = %s AND end_time_pt = %s""",
                        (site_name, alarm_type, variable_name, start_time, end_time)
                    )
                    result = cursor.fetchone()
                    if result is None:
                        raise Exception(f"Failed to retrieve alarm ID after insert for {alarm_type}/{variable_name}")
                    alarm_id = result[0]
                    alarm_lookup.setdefault(lookup_key, []).append({
                        'id': alarm_id,
                        'start_time': start_time,
                        'end_time': end_time
                    })
                    new_alarms += 1

                # Get overlapping existing instances for certainty-based merging
                cursor.execute(
                    f"""SELECT inst_id, start_time_pt, end_time_pt, certainty
                        FROM {alarm_inst_table}
                        WHERE id = %s AND start_time_pt <= %s AND end_time_pt >= %s""",
                    (alarm_id, end_time, start_time)
                )
                existing_instances = cursor.fetchall()

                new_segments = [(start_time, end_time, certainty)]

                for existing in existing_instances:
                    existing_inst_id, existing_start, existing_end, existing_certainty = existing
                    updated_segments = []

                    for seg_start, seg_end, seg_certainty in new_segments:
                        if seg_end <= existing_start or seg_start >= existing_end:
                            updated_segments.append((seg_start, seg_end, seg_certainty))
                            continue

                        if existing_certainty < seg_certainty:
                            # New has higher certainty — split existing around new
                            if existing_start < seg_start:
                                cursor.execute(update_inst_sql, (existing_start, seg_start, existing_inst_id))
                                updated_instances += 1
                                if existing_end > seg_end:
                                    cursor.execute(insert_inst_sql, (alarm_id, seg_end, existing_end, existing_certainty))
                                    new_instances += 1
                            elif existing_end > seg_end:
                                cursor.execute(update_inst_sql, (seg_end, existing_end, existing_inst_id))
                                updated_instances += 1
                            else:
                                cursor.execute(delete_inst_sql, (existing_inst_id,))
                            updated_segments.append((seg_start, seg_end, seg_certainty))

                        elif existing_certainty > seg_certainty:
                            # Existing has higher certainty — trim new to non-overlapping parts
                            if seg_start < existing_start:
                                updated_segments.append((seg_start, existing_start, seg_certainty))
                            if seg_end > existing_end:
                                updated_segments.append((existing_end, seg_end, seg_certainty))

                        else:
                            # Same certainty — merge into existing
                            merged_start = min(seg_start, existing_start)
                            merged_end = max(seg_end, existing_end)
                            cursor.execute(update_inst_sql, (merged_start, merged_end, existing_inst_id))
                            updated_instances += 1

                    new_segments = updated_segments

                for seg_start, seg_end, seg_certainty in new_segments:
                    if seg_start < seg_end:
                        cursor.execute(insert_inst_sql, (alarm_id, seg_start, seg_end, seg_certainty))
                        new_instances += 1

            connection.commit()
            print(f"Successfully loaded alarms: {new_alarms} new alarm records, {updated_alarms} updated alarm records, "
                  f"{new_instances} new instances, {updated_instances} updated instances.")
            return True

        except Exception as e:
            print(f"Error loading alarms: {e}")
            connection.rollback()
            return False

        finally:
            cursor.close()
            connection.close()
