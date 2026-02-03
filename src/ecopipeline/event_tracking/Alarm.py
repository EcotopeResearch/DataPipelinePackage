import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta

class Alarm:
    def __init__(self, bounds_df : pd.DataFrame, alarm_tag : str = None, type_default_dict : dict = {},
                 two_part_tag : bool = True, range_bounds : bool = False, alarm_db_type : str = 'SILENT_ALARM',
                 daily_only : bool = False):
        self.daily_only = daily_only
        self.alarm_tag = alarm_tag
        self.two_part_tag = two_part_tag
        self.range_bounds = range_bounds
        self.type_default_dict = type_default_dict
        self.alarm_db_type = alarm_db_type
        self.triggered_alarms = {
                'start_time_pt' : [],
                'end_time_pt' : [],
                'alarm_type' : [],
                'event_detail' : [],
                'variable_name' : [],
                'certainty' : []
            }
        self.bounds_df = self._process_bounds_df_alarm_codes(bounds_df)

    def find_alarms(self, df: pd.DataFrame, daily_data : pd.DataFrame, config : ConfigManager) -> pd.DataFrame:
        """
        Parameters
        ----------
        df: pd.DataFrame
            Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
            are out of order or have gaps, the function may return erroneous alarms.
        daily_df: pd.DataFrame
            Post-transformed dataframe for daily data.
        config : ecopipeline.ConfigManager
            The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
            called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
            The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
            name of each variable in the dataframe that requires alarming and the appropriate alarm codes.
        Returns
        -------
        pd.DataFrame:
            Pandas dataframe with alarm events
        """
        if self.bounds_df.empty:
            return self._convert_silent_alarm_dict_to_df({}) # no alarms to look into 
        if self.daily_only:
            if daily_data.empty:
                print(f"cannot flag {self.alarm_tag} alarms. Dataframe is empty")
                return pd.DataFrame()
        elif df.empty:
            print(f"cannot flag {self.alarm_tag} alarms. Dataframe is empty")
            return pd.DataFrame()
        self.specific_alarm_function(df, daily_data, config)
        return self._convert_silent_alarm_dict_to_df(self.triggered_alarms)
    
    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        self.triggered_alarms = {}

    def _add_an_alarm(self, start_time : datetime, end_time : datetime, var_name : str, alarm_string : str, add_one_minute_to_end : bool = True, certainty : str = "high"):
        certainty_dict = {
            "high" : 3,
            "med" : 2,
            "low" : 1
        }
        if certainty not in certainty_dict.keys():
            raise Exception(f"{certainty} is not a valid certainty key. Valid keys are {certainty_dict.keys()}")
        else: 
            certainty = certainty_dict[certainty]
        if add_one_minute_to_end:
            end_time = end_time + timedelta(minutes=1)
        self.triggered_alarms['start_time_pt'].append(start_time)
        self.triggered_alarms['end_time_pt'].append(end_time)
        self.triggered_alarms['alarm_type'].append(self.alarm_db_type)
        self.triggered_alarms['event_detail'].append(alarm_string)
        self.triggered_alarms['variable_name'].append(var_name)
        self.triggered_alarms['certainty'].append(certainty)
    
    def _convert_silent_alarm_dict_to_df(self, alarm_dict : dict) -> pd.DataFrame:

        alarm_df = pd.DataFrame(alarm_dict)
        alarm_df = self._compress_alarm_df(alarm_df)
        return alarm_df

    def _compress_alarm_df(self, alarm_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compresses consecutive alarms of the same variable_name and alarm_type into single rows.
        If one alarm's start_time_pt is within one minute of another alarm's end_time_pt,
        they are merged into one row with the earliest start_time_pt and latest end_time_pt.

        Parameters
        ----------
        alarm_df : pd.DataFrame
            DataFrame with columns: start_time_pt, end_time_pt, alarm_type, variable_name, event_detail

        Returns
        -------
        pd.DataFrame
            Compressed DataFrame with consecutive alarms merged
        """
        if alarm_df.empty:
            return alarm_df

        # Sort entire DataFrame by start_time_pt before processing
        alarm_df = alarm_df.sort_values('start_time_pt').reset_index(drop=True)

        compressed_rows = []

        # Group by variable_name and alarm_type
        for (var_name, alarm_type), group in alarm_df.groupby(['variable_name', 'alarm_type'], sort=False):
            # Group is already sorted since we sorted the whole DataFrame above
            group = group.reset_index(drop=True)

            current_start = None
            current_end = None
            current_detail = None
            current_certainty = None

            for _, row in group.iterrows():
                row_start = row['start_time_pt']
                row_end = row['end_time_pt']

                if current_start is None:
                    # First row in group
                    current_start = row_start
                    current_end = row_end
                    current_detail = row['event_detail']
                    current_certainty = row['certainty']
                elif row_start <= current_end + timedelta(minutes=1):
                    # This row is within 1 minute of current end - merge it after checking 
                    row_certainty = row['certainty']
                    if row_certainty > current_certainty:
                        if row_start > current_start:
                            compressed_rows.append({
                                'start_time_pt': current_start,
                                'end_time_pt': row_start,
                                'alarm_type': alarm_type,
                                'event_detail': current_detail,
                                'variable_name': var_name,
                                'certainty': current_certainty
                            })
                        if row_end >= current_end:
                            current_start = row_start
                            current_end = row_end
                            current_detail = row['event_detail']
                            current_certainty = row_certainty
                        else:
                            #encompassed
                            compressed_rows.append({
                                    'start_time_pt': row_start,
                                    'end_time_pt': row_end,
                                    'alarm_type': alarm_type,
                                    'event_detail': row['event_detail'],
                                    'variable_name': var_name,
                                    'certainty': row_certainty
                                })
                            current_start = row_end

                    elif row_certainty < current_certainty:
                        if row_end > current_end:
                            compressed_rows.append({
                                    'start_time_pt': current_start,
                                    'end_time_pt': current_end,
                                    'alarm_type': alarm_type,
                                    'event_detail': current_detail,
                                    'variable_name': var_name,
                                    'certainty': current_certainty
                                })
                            current_start = current_end
                            current_end = row_end
                            current_detail = row['event_detail']
                            current_certainty = row_certainty
                        
                    else:
                        current_end = max(current_end, row_end)
                else:
                    # Gap is more than 1 minute - save current and start new
                    compressed_rows.append({
                        'start_time_pt': current_start,
                        'end_time_pt': current_end,
                        'alarm_type': alarm_type,
                        'event_detail': current_detail,
                        'variable_name': var_name,
                        'certainty': current_certainty
                    })
                    current_start = row_start
                    current_end = row_end
                    current_detail = row['event_detail']
                    current_certainty = row['certainty']

            # Don't forget the last accumulated row
            if current_start is not None:
                compressed_rows.append({
                    'start_time_pt': current_start,
                    'end_time_pt': current_end,
                    'alarm_type': alarm_type,
                    'event_detail': current_detail,
                    'variable_name': var_name,
                    'certainty': current_certainty
                })
        return pd.DataFrame(compressed_rows)
    
    def _process_bounds_df_alarm_codes(self, og_bounds_df : pd.DataFrame) -> pd.DataFrame:
        # Should only do for alarm codes of format: [TAG]_[TYPE]_[OPTIONAL_ID]:[BOUND]
        bounds_df = og_bounds_df.copy()
        required_columns = ["variable_name", "alarm_codes"]
        for required_column in required_columns:
            if not required_column in bounds_df.columns:
                raise Exception(f"{required_column} is not present in Variable_Names.csv")
        if not 'pretty_name' in bounds_df.columns:
            bounds_df['pretty_name'] = bounds_df['variable_name']
        else:
            bounds_df['pretty_name'] = bounds_df['pretty_name'].fillna(bounds_df['variable_name'])

        bounds_df = bounds_df.loc[:, ["variable_name", "alarm_codes", "pretty_name"]]
        bounds_df.dropna(axis=0, thresh=2, inplace=True)

        # Check if all alarm_codes are null or if dataframe is empty
        if bounds_df.empty or bounds_df['alarm_codes'].isna().all():
            return pd.DataFrame()
        
        bounds_df = bounds_df[bounds_df['alarm_codes'].str.contains(self.alarm_tag, na=False)]

        # Split alarm_codes by semicolons and create a row for each STS code
        expanded_rows = []
        for idx, row in bounds_df.iterrows():
            alarm_codes = str(row['alarm_codes']).split(';')
            tag_codes = [code.strip() for code in alarm_codes if code.strip().startswith(self.alarm_tag)]

            if tag_codes:  # Only process if there are STS codes
                for tag_code in tag_codes:
                    new_row = row.copy()
                    if ":" in tag_code:
                        tag_parts = tag_code.split(':')
                        if len(tag_parts) > 2:
                            raise Exception(f"Improperly formated alarm code : {tag_code}")
                        if self.range_bounds:
                            bounds = tag_parts[1]
                            bound_range = bounds.split('-')
                            if len(bound_range) != 2:
                                raise Exception(f"Improperly formated alarm code : {tag_code}. Expected bound range in form '[number]-[number]' but recieved '{bounds}'.")
                            new_row['bound'] = bound_range[0]
                            new_row['bound2'] = bound_range[1]
                        else:    
                            new_row['bound'] = tag_parts[1]
                        tag_code = tag_parts[0]
                    else:
                        new_row['bound'] = None
                        if self.range_bounds:
                            new_row['bound2'] = None
                    new_row['alarm_codes'] = tag_code

                    expanded_rows.append(new_row)

        if expanded_rows:
            bounds_df = pd.DataFrame(expanded_rows)
        else:
            return pd.DataFrame()# no tagged alarms to look into
        
        alarm_code_parts = []
        for idx, row in bounds_df.iterrows():
            parts = row['alarm_codes'].split('_')
            if self.two_part_tag:
                if len(parts) == 2:
                    alarm_code_parts.append([parts[1], "No ID"])
                elif len(parts) == 3:
                    alarm_code_parts.append([parts[1], parts[2]])
                else:
                    raise Exception(f"improper {self.alarm_tag} alarm code format for {row['variable_name']}")
            else:
                if len(parts) == 1:
                    alarm_code_parts.append(["default", "No ID"])
                elif len(parts) == 2:
                    alarm_code_parts.append(["default", parts[1]])
                else:
                    raise Exception(f"improper {self.alarm_tag} alarm code format for {row['variable_name']}")
        if alarm_code_parts:
            bounds_df[['alarm_code_type', 'alarm_code_id']] = pd.DataFrame(alarm_code_parts, index=bounds_df.index)

            # Replace None bounds with appropriate defaults based on alarm_code_type
            for idx, row in bounds_df.iterrows():
                if pd.isna(row['bound']) or row['bound'] is None:
                    if row['alarm_code_type'] in self.type_default_dict.keys():
                        if self.range_bounds:
                            bounds_df.at[idx, 'bound'] = self.type_default_dict[row['alarm_code_type']][0]
                            bounds_df.at[idx, 'bound2'] = self.type_default_dict[row['alarm_code_type']][1]
                        else:
                            bounds_df.at[idx, 'bound'] = self.type_default_dict[row['alarm_code_type']]
            # Coerce bound column to float
            bounds_df['bound'] = pd.to_numeric(bounds_df['bound'], errors='coerce').astype(float)
            if self.range_bounds:
                bounds_df['bound2'] = pd.to_numeric(bounds_df['bound2'], errors='coerce').astype(float)

        return bounds_df
    
    def _append_previous_days_to_df(self, daily_df: pd.DataFrame, config : ConfigManager, ratio_period_days : int, day_table_name : str, primary_key : str = "time_pt") -> pd.DataFrame:
        db_connection, cursor = config.connect_db()
        period_start = daily_df.index.min() - timedelta(ratio_period_days)
        try:
            # find existing times in database for upsert statement
            cursor.execute(
                f"SELECT * FROM {day_table_name} WHERE {primary_key} < '{daily_df.index.min()}' AND {primary_key} >= '{period_start}'")
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            old_days_df = pd.DataFrame(result, columns=column_names)
            old_days_df = old_days_df.set_index(primary_key)
            daily_df = pd.concat([daily_df, old_days_df])
            daily_df = daily_df.sort_index(ascending=True)
        except mysqlerrors.Error:
            print(f"Table {day_table_name} has no data.")

        db_connection.close()
        cursor.close()
        return daily_df
