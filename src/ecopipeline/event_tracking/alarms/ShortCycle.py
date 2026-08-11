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
            rows = self.bounds_df[self.bounds_df['variable_name'] == var_name]
            if len(rows) != 1:
                raise Exception(f"Multiple short cycle alarm codes set for {var_name}")
            self.record_set_alarm([var_name])
            pwr_thresh = rows.iloc[0]['bound']
            var_pretty = rows.iloc[0]['pretty_name']
            if var_name in df.columns:
                power_on_mask = df[var_name] > pwr_thresh
                max_gap = self._gap_tolerance(self.short_cycle_time)
                short_cycle_duration = pd.Timedelta(minutes=self.short_cycle_time)

                # Run length is measured in minutes rather than counted in rows, so the
                # sample interval of the data does not change the result
                for start_time, end_time, duration, bounded in self._iter_runs(power_on_mask, max_gap):
                    if duration >= short_cycle_duration:
                        continue
                    # Unlike the alarms that fire on a condition lasting too long, missing
                    # data here makes a run look shorter than it was. A run that touches the
                    # edge of the frame or a data gap has an unknown true length, so it
                    # cannot be reported as a short cycle.
                    if not bounded:
                        continue
                    run_minutes = duration.total_seconds() / 60
                    self._add_an_alarm(start_time, end_time, var_name,
                        f"Short cycle: {var_pretty} was on for only {run_minutes:.0f} minutes starting at {start_time}.",
                        add_one_interval_to_end=False)