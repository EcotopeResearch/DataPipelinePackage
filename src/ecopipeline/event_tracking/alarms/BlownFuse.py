import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class BlownFuse(Alarm):
    """
    Detects blown fuse conditions for heating elements by identifying when an element is drawing power
    but significantly less than its expected draw, suggesting a blown fuse.

    Variable_Names.csv configuration:
      alarm_codes column: BLWNFSE:### where ### is the expected kW draw when the element is fully on.
      variable_name column: Must start with PowerIn_ (e.g., PowerIn_ERElement1).
        PowerIn_[name] - Element power variable. Bound (###) from alarm_codes is the expected kW draw (default 30 kW).
            Alarm triggers when element is on (power > default_power_threshold) but drawing less than
            (expected_draw - default_power_range) for fault_time consecutive minutes.

    Parameters
    ----------
    default_power_threshold : float
        Minimum power level (kW) to consider the element 'on' (default 1.0).
    default_power_range : float
        Allowable variance below expected power draw (default 2.0). Alarm triggers when actual draw < (expected - range).
    default_power_draw : float
        Default expected power draw in kW when no bound is specified in the alarm code (default 30).
    fault_time : int
        Number of consecutive minutes the fault condition must persist before triggering an alarm (default 3).
    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_threshold : float = 1.0, default_power_range : float = 2.0, default_power_draw : float = 30, fault_time : int = 3):
        alarm_tag = 'BLWNFSE'
        type_default_dict = {'PowerIn' : default_power_draw}
        self.default_power_threshold = default_power_threshold
        self.default_power_range = default_power_range
        self.fault_time = fault_time
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = False)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for var_name in self.bounds_df['variable_name'].unique():
            self.record_set_alarm([var_name])
            rows = self.bounds_df[self.bounds_df['variable_name'] == var_name]
            if len(rows) != 1:
                raise Exception(f"Multiple blown fuse alarm codes for {var_name}")
            expected_power_draw = rows.iloc[0]['bound']
            if var_name in df.columns:
                # Element is drawing power, but less than the expected draw for its fuse
                power_on_mask = df[var_name] > self.default_power_threshold
                unexpected_power_mask = df[var_name] < expected_power_draw - self.default_power_range
                combined_mask = power_on_mask & unexpected_power_mask

                # fault_time is a duration in minutes, so the sample interval of the data does
                # not change the result, and a data gap is never counted as fault time
                for start_time, end_time, _ in self._iter_sustained_streaks(combined_mask, self.fault_time):
                    self._add_an_alarm(start_time, end_time, var_name,
                        f"Blown Fuse: {var_name} had a power draw less than {expected_power_draw - self.default_power_range:.1f} while element was ON starting at {start_time}.",
                        add_one_interval_to_end=False, certainty="high")
