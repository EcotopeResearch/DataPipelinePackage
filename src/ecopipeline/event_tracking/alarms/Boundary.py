import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class Boundary(Alarm):
    """
    Detects when variable values fall outside their expected low/high boundary range for a sustained period.
    An alarm triggers when a value stays below the low_alarm threshold or above the high_alarm threshold
    for fault_time consecutive minutes.

    Variable_Names.csv columns:
    variable_name - Name of the variable to monitor
    low_alarm - Lower bound threshold. Alarm triggers if value stays below this for fault_time minutes.
    high_alarm - Upper bound threshold. Alarm triggers if value stays above this for fault_time minutes.
    fault_time - (Optional) Number of consecutive minutes for this variable. Overrides default_fault_time.
    pretty_name - (Optional) Display name for the variable in alarm messages. Defaults to variable_name.

    Parameters
    ----------
    default_fault_time : int
        Number of consecutive minutes that a value must be outside bounds before triggering an alarm (default 15).
        Can be overridden per-variable using the fault_time column in Variable_Names.csv.
    """
    def __init__(self, bounds_df : pd.DataFrame, default_fault_time : int = 15):
        self.default_fault_time = default_fault_time
        super().__init__(bounds_df, 'BOUNDRY',{})

    def _process_bounds_df_alarm_codes(self, og_bounds_df : pd.DataFrame) -> pd.DataFrame:
        bounds_df = og_bounds_df.copy()
        required_columns = ["variable_name", "high_alarm", "low_alarm"]
        for required_column in required_columns:
            if not required_column in bounds_df.columns:
                raise Exception(f"{required_column} is not present in Variable_Names.csv")
        if not 'pretty_name' in bounds_df.columns:
            bounds_df['pretty_name'] = bounds_df['variable_name']
        else:
            bounds_df['pretty_name'] = bounds_df['pretty_name'].fillna(bounds_df['variable_name'])
        if not 'fault_time' in bounds_df.columns:
            bounds_df['fault_time'] = self.default_fault_time
        
        bounds_df = bounds_df.loc[:, ["variable_name", "high_alarm", "low_alarm", "fault_time", "pretty_name"]]
        bounds_df.dropna(axis=0, thresh=2, inplace=True)
        bounds_df.set_index(['variable_name'], inplace=True)
        # ensure that lower and upper bounds are numbers
        bounds_df['high_alarm'] = pd.to_numeric(bounds_df['high_alarm'], errors='coerce').astype(float)
        bounds_df['low_alarm'] = pd.to_numeric(bounds_df['low_alarm'], errors='coerce').astype(float)
        bounds_df['fault_time'] = pd.to_numeric(bounds_df['fault_time'], errors='coerce').astype('Int64')
        bounds_df = bounds_df[bounds_df.index.notnull()]
        return bounds_df

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        idx = df.index
        full_days = pd.to_datetime(pd.Series(idx).dt.normalize().unique())
        for bound_var, bounds in self.bounds_df.iterrows():
            if bound_var in df.columns:
                lower_mask = df[bound_var] < bounds["low_alarm"]
                upper_mask = df[bound_var] > bounds["high_alarm"]
                if pd.isna(bounds['fault_time']):
                    bounds['fault_time'] = self.default_fault_time
                for day in full_days:
                    if bounds['fault_time'] < 1 :
                        print(f"Could not process alarm for {bound_var}. Fault time must be greater than or equal to 1 minute.")
                    self._check_and_add_alarm(lower_mask, day, bounds["fault_time"], bound_var, bounds['pretty_name'])
                    self._check_and_add_alarm(upper_mask, day, bounds["fault_time"], bound_var, bounds['pretty_name'])
    
    def _check_and_add_alarm(self, mask : pd.Series, day, fault_time : int, var_name : str, pretty_name : str):
        next_day = day + pd.Timedelta(days=1)
        filtered_df = mask.loc[(mask.index >= day) & (mask.index < next_day)]
        consecutive_condition = filtered_df.rolling(window=fault_time).min() == 1
        if consecutive_condition.any():
            group = (consecutive_condition != consecutive_condition.shift()).cumsum()

            # Iterate through each streak and add an alarm for each
            for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                streak_indices = consecutive_condition[group == group_id].index
                # streak_length = len(streak_indices)

                # Adjust start time because first (fault_time-1) minutes don't count in window
                start_time = streak_indices[0] - pd.Timedelta(minutes=fault_time-1)
                end_time = streak_indices[-1]

                alarm_string = f"Boundary alarm for {pretty_name}"

                # if start_time in alarms_dict:
                self._add_an_alarm(start_time, end_time, var_name, alarm_string)