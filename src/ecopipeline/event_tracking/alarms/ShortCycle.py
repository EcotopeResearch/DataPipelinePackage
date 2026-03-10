import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class ShortCycle(Alarm):
    """
    Detects short cycling by identifying when a heat pump runs for fewer than short_cycle_time
    consecutive minutes before turning off. Short cycling can indicate equipment issues or
    improper system sizing.

    Variable_Names.csv configuration:
      alarm_codes column: SHRTCYC:### where ### is the power threshold above which the HP is considered 'on'.
      variable_name column: Must start with PowerIn_ (e.g., PowerIn_HPWH1).
        PowerIn_[name] - Heat pump power variable. Bound (###) from alarm_codes is the power threshold
            (default 1.0). Alarm triggers if the HP runs for fewer than short_cycle_time consecutive minutes.

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold when no bound is specified in the alarm code (default 1.0).
    short_cycle_time : int
        Minimum expected run time in minutes (default 15). Alarm triggers if the HP runs for fewer than
        this many consecutive minutes before turning off.
    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_threshold : float = 1.0, short_cycle_time : int = 15):
        alarm_tag = 'SHRTCYC'
        type_default_dict = {'PowerIn' : default_power_threshold}
        self.short_cycle_time = short_cycle_time
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = False)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for var_name in self.bounds_df['variable_name'].unique():
            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                rows = self.bounds_df[self.bounds_df['variable_name'] == var_name]
                pwr_thresh = rows.iloc[0]['bound']
                var_pretty = rows.iloc[0]['pretty_name']
                if len(rows) != 1:
                    raise Exception(f"Multiple short cycle alarm codes set for {var_name}")
                if var_name in filtered_df.columns:
                    power_on_mask = filtered_df[var_name] > pwr_thresh

                    # Find runs of consecutive True values by detecting changes in the mask
                    mask_changes = power_on_mask != power_on_mask.shift(1)
                    run_groups = mask_changes.cumsum()

                    # For each run where power is on, check if it's shorter than short_cycle_time
                    for group_id in run_groups[power_on_mask].unique():
                        run_indices = filtered_df.index[(run_groups == group_id) & power_on_mask]
                        run_length = len(run_indices)
                        if run_length > 0 and run_length < self.short_cycle_time:
                            start_time = run_indices[0]
                            end_time = run_indices[-1]
                            self._add_an_alarm(start_time, end_time, var_name, f"Short cycle: {var_pretty} was on for only {run_length} minutes starting at {start_time}.")