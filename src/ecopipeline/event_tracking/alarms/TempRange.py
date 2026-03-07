import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class TempRange(Alarm):
    """
    Detects when a temperature value falls outside an acceptable range for
    too long. An alarm is triggered if the temperature is above the high bound or below the low bound
    for `fault_time` consecutive minutes.

    VarNames syntax:
    TMPRANG_[OPTIONAL ID]:###-### - Indicates a temperature variable. ###-### is the acceptable temperature range
        (e.g., TMPRANG:110-130 means temperature should stay between 110 and 130 degrees).

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the DHW alarm codes (e.g., DHW:110-130, DHW_1:115-125).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_high_temp : float
        Default high temperature bound when no custom range is specified in the alarm code (default 130). Temperature above this triggers alarm.
    default_low_temp : float
        Default low temperature bound when no custom range is specified in the alarm code (default 130). Temperature below this triggers alarm.
    fault_time : int
        Number of consecutive minutes that temperature must be outside the acceptable range before triggering an alarm (default 10).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    def __init__(self, bounds_df : pd.DataFrame, default_high_temp : float = 130, default_low_temp : float = 115, fault_time : int = 10):
        alarm_tag = 'TMPRANG'
        type_default_dict = {'Temp': [default_low_temp, default_high_temp]}
        self.fault_time = fault_time
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = False, range_bounds=True, alarm_db_type='TEMP_RANGE')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        # Process each unique alarm_code_id
        for dhw_var in self.bounds_df['variable_name'].unique():
            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                rows = self.bounds_df[self.bounds_df['variable_name'] == dhw_var]
                low_bound = rows.iloc[0]['bound']
                high_bound = rows.iloc[0]['bound2']
                pretty_name = rows.iloc[0]['pretty_name']

                if dhw_var in filtered_df.columns:
                    # Check if temp is above high bound or below low bound
                    out_of_range_mask = (filtered_df[dhw_var] > high_bound) | (filtered_df[dhw_var] < low_bound)

                    # Check for fault_time consecutive minutes
                    consecutive_condition = out_of_range_mask.rolling(window=self.fault_time).min() == 1
                    if consecutive_condition.any():
                        # Find all streaks of consecutive True values
                        group = (consecutive_condition != consecutive_condition.shift()).cumsum()

                        # Iterate through each streak and add an alarm for each
                        for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                            streak_indices = consecutive_condition[group == group_id].index
                            streak_length = len(streak_indices)

                            # Adjust start time because first (fault_time-1) minutes don't count in window
                            start_time = streak_indices[0] - pd.Timedelta(minutes=self.fault_time-1)
                            end_time = streak_indices[-1]
                            adjusted_streak_length = streak_length + self.fault_time - 1

                            self._add_an_alarm(start_time, end_time, dhw_var,
                                f"Temperature out of range: {pretty_name} was outside {low_bound}-{high_bound} F for {adjusted_streak_length} consecutive minutes starting at {start_time}.")