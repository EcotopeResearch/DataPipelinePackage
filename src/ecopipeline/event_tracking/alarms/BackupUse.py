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
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    IMBCKUP_P_ID - Back Up Tank Power Varriable. Must be in same power units as total system power
    IMBCKUP_TP_ID:### - Total System Power for ratio alarming for alarming if back up power is more than ### (40% default) of usage
    IMBCKUP_ST_ID:### - Back Up Setpoint that should not change at all from ### (default 130)

    Parameters
    ----------
    default_setpoint : float
        Default temperature setpoint in degrees for T and ST alarm codes when no custom bound is specified (default 130.0)
    default_power_indication : float
        Default power threshold in kW for SP alarm codes when no custom bound is specified (default 1.0)
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4)
    """
    def __init__(self, bounds_df : pd.DataFrame, default_setpoint : float = 130.0, default_power_ratio : float = 0.1):
        alarm_tag = 'IMBCKUP'
        type_default_dict = {
                'POW': None,
                'TP': default_power_ratio,
                'ST': default_setpoint
            }
        super().__init__(bounds_df, alarm_tag, type_default_dict, alarm_db_type='BACKUP_USE')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            for alarm_id in self.bounds_df['alarm_code_id'].unique():
                id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]

                # Get T and SP alarm codes for this ID
                pow_codes = id_group[id_group['alarm_code_type'] == 'POW']
                tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
                st_codes = id_group[id_group['alarm_code_type'] == 'ST']

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
    