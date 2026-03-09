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
    """
    def __init__(self, bounds_df : pd.DataFrame, default_setpoint : float = 130.0, default_power_ratio : float = 0.1):
        alarm_tag = 'IMBCKUP'
        type_default_dict = {
                'PowerIn': None,
                'PowerIn_Total': default_power_ratio,
                'Setpoint': default_setpoint
            }
        super().__init__(bounds_df, alarm_tag, type_default_dict, alarm_db_type='BACKUP_USE')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
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
                    # Check each ST code against its individual bound
                    for idx, st_row in st_codes.iterrows():
                        st_var_name = st_row['variable_name']
                        st_pretty_name = st_row['pretty_name']
                        st_setpoint = st_row['bound']
                        # Check if st_var_name exists in filtered_df
                        if st_var_name in filtered_df.columns:
                            # Check if setpoint was altered for over 10 minutes
                            altered_mask = filtered_df[st_var_name] != st_setpoint
                            consecutive_condition = altered_mask.rolling(window=10).min() == 1
                            if consecutive_condition.any():
                                # Find all consecutive groups where condition is true
                                group = (consecutive_condition != consecutive_condition.shift()).cumsum()
                                for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                                    streak_indices = consecutive_condition[group == group_id].index
                                    start_time = streak_indices[0] - pd.Timedelta(minutes=9)
                                    end_time = streak_indices[-1]
                                    streak_length = len(streak_indices) + 9
                                    actual_value = filtered_df.loc[streak_indices[0], st_var_name]
                                    self._add_an_alarm(start_time, end_time, st_var_name,
                                        f"Swing tank setpoint was altered: {st_pretty_name} was {actual_value} for {streak_length} minutes starting at {start_time} (expected {st_setpoint}).")

                if len(tp_codes) == 1 and len(pow_codes) >= 1:
                    tp_var_name = tp_codes.iloc[0]['variable_name']
                    tp_bound = tp_codes.iloc[0]['bound']
                    if tp_var_name in daily_df.columns:
                        # Get list of ER variable names
                        bu_pow_names = pow_codes['variable_name'].tolist()

                        # Check if all ER variables exist in daily_df
                        if all(var in daily_df.columns for var in bu_pow_names):
                            # Sum all ER variables for this day
                            bu_pow_sum = daily_df.loc[day, bu_pow_names].sum()
                            tp_value = daily_df.loc[day, tp_var_name]

                            # Check if sum of ER >= OUT value
                            if bu_pow_sum >= tp_value*tp_bound:
                                self._add_an_alarm(day, day + timedelta(1), tp_var_name,
                                    f"Improper Back Up Use: Sum of back up equipment ({bu_pow_sum:.2f}) exceeds {(tp_bound * 100):.2f}% of total power.", certainty="med")
    