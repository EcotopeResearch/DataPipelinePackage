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
            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                rows = self.bounds_df[self.bounds_df['variable_name'] == var_name]
                expected_power_draw = rows.iloc[0]['bound']
                if len(rows) != 1:
                    raise Exception(f"Multiple blown fuse alarm codes for {var_name}")
                if var_name in filtered_df.columns:
                    # Check for consecutive minutes where both power and temp exceed thresholds
                    power_on_mask = filtered_df[var_name] > self.default_power_threshold
                    unexpected_power_mask = filtered_df[var_name] < expected_power_draw - self.default_power_range
                    combined_mask = power_on_mask & unexpected_power_mask

                    # Check for fault_time consecutive minutes
                    consecutive_condition = combined_mask.rolling(window=self.fault_time).min() == 1
                    if consecutive_condition.any():

                         # Find all streaks of consecutive True values
                        group = (consecutive_condition != consecutive_condition.shift()).cumsum()

                        # Iterate through each streak and add an alarm for each
                        for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                            streak_indices = consecutive_condition[group == group_id].index
                            start_time = streak_indices[0] - pd.Timedelta(minutes=self.fault_time-1)
                            end_time = streak_indices[-1]

                            self._add_an_alarm(start_time, end_time, var_name,
                                f"Blown Fuse: {var_name} had a power draw less than {expected_power_draw - self.default_power_range:.1f} while element was ON starting at {start_time}.",
                                certainty="high")
                            
                        # first_true_index = consecutive_condition.idxmax()
                        # adjusted_time = first_true_index - pd.Timedelta(minutes=self.fault_time-1)
                        # _add_an_alarm(alarms, day, var_name, f"Blown Fuse: {var_name} had a power draw less than {expected_power_draw - self.default_power_range:.1f} while element was ON starting at {adjusted_time}.")
