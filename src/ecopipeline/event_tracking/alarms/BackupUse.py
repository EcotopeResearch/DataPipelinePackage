import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class BackupUse(Alarm):
    """
    Detects improper backup equipment use by monitoring whether backup power exceeds an expected
    fraction of total system power, and whether setpoint variables have been altered from their
    expected values.

    Variable_Names.csv configuration:
      alarm_codes column: IMBCKUP or IMBCKUP:### where ### provides the bound for the variable (see types below).
      variable_name column: determines the role of the variable by its first underscore-separated part:
        PowerIn_[name] - Backup equipment power variable. Multiple allowed; daily values are summed.
            No bound needed in alarm_codes (use just IMBCKUP).
        PowerIn_Total[...] - Total system power variable. Bound (###) from alarm_codes is the ratio threshold
            (default 0.1 for 10%). Alarm triggers when sum of backup power >= total power * threshold.
        Setpoint_[name] - Setpoint variable that should remain constant. Bound (###) from alarm_codes is
            the expected setpoint value (default 130.0). Alarm triggers if value differs for 10+ consecutive minutes.

    Parameters
    ----------
    default_setpoint : float
        Default expected setpoint value for Setpoint variables when no bound is specified (default 130.0).
    default_power_ratio : float
        Default ratio threshold for PowerIn_Total variables when no bound is specified (default 0.1).
        Alarm triggers when sum of backup power >= total power * threshold.
    setpoint_fault_time : int
        Number of minutes a Setpoint variable must differ from its expected value before
        triggering an alarm (default 10).
    """
    def __init__(self, bounds_df : pd.DataFrame, default_setpoint : float = 130.0, default_power_ratio : float = 0.1,
                 setpoint_fault_time : int = 10):
        alarm_tag = 'IMBCKUP'
        type_default_dict = {
                'PowerIn': None,
                'PowerIn_Total': default_power_ratio,
                'Setpoint': default_setpoint
            }
        self.setpoint_fault_time = setpoint_fault_time
        super().__init__(bounds_df, alarm_tag, type_default_dict)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]

            # Get T and SP alarm codes for this ID
            pow_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
            tp_codes = id_group[id_group['alarm_code_type'] == 'PowerIn_Total']
            st_codes = id_group[id_group['alarm_code_type'] == 'Setpoint']

            # Check for multiple T or SP codes with same ID
            if len(tp_codes) > 1:
                raise Exception(f"Improper alarm codes for swing tank setpoint with id {alarm_id}")

            if len(st_codes) >= 1:
                # Check each ST code against its individual bound. This is a minute level
                # check, so it scans the whole frame at once and its threshold is a
                # duration in minutes rather than a row count.
                for idx, st_row in st_codes.iterrows():
                    st_var_name = st_row['variable_name']
                    st_pretty_name = st_row['pretty_name']
                    st_setpoint = st_row['bound']
                    # Check if st_var_name exists in df
                    if st_var_name in df.columns:
                        # Check if setpoint was altered for setpoint_fault_time minutes
                        altered_mask = df[st_var_name] != st_setpoint
                        for start_time, end_time, duration in self._iter_sustained_streaks(altered_mask, self.setpoint_fault_time):
                            actual_value = df.loc[start_time, st_var_name]
                            self._add_an_alarm(start_time, end_time, st_var_name,
                                f"Swing tank setpoint was altered: {st_pretty_name} was {actual_value} for {duration:.0f} minutes starting at {start_time} (expected {st_setpoint}).",
                                add_one_interval_to_end=False)

            if len(tp_codes) == 1 and len(pow_codes) >= 1:
                tp_var_name = tp_codes.iloc[0]['variable_name']
                tp_bound = tp_codes.iloc[0]['bound']
                # Get list of ER variable names
                bu_pow_names = pow_codes['variable_name'].tolist()
                self.record_set_alarm([tp_var_name] + bu_pow_names)
                if tp_var_name in daily_df.columns:

                    # Check if all ER variables exist in daily_df
                    if all(var in daily_df.columns for var in bu_pow_names):
                        # This check is genuinely per day, so it keeps its day loop
                        for day in daily_df.index:
                            # Sum all ER variables for this day
                            bu_pow_sum = daily_df.loc[day, bu_pow_names].sum()
                            tp_value = daily_df.loc[day, tp_var_name]

                            # Check if sum of ER >= OUT value
                            if bu_pow_sum >= tp_value*tp_bound:
                                # day + 1 is already the exclusive end of the day
                                self._add_an_alarm(day, day + timedelta(1), tp_var_name,
                                    f"Improper Back Up Use: Sum of back up equipment ({bu_pow_sum:.2f}) exceeds {(tp_bound * 100):.2f}% of total power.",
                                    add_one_interval_to_end=False, certainty="med")
    