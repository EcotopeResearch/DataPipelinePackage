import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class TMSetpoint(Alarm):
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    TMNSTPT_T_ID:### - Swing Tank Outlet Temperature. Alarm triggered if over number ### (or 130) for 3 minutes with power on
    TMNSTPT_SP_ID:### - Swing Tank Power. ### is lowest recorded power for Swing Tank to be considered 'on'. Defaults to 1.0
    TMNSTPT_TP_ID:### - Total System Power for ratio alarming for alarming if swing tank power is more than ### (40% default) of usage
    TMNSTPT_ST_ID:### - Swing Tank Setpoint that should not change at all from ### (default 130)

    Parameters
    ----------
    default_fault_time : int
        Number of consecutive minutes for T+SP alarms (default 3). T+SP alarms trigger when tank is powered and temperature exceeds
        setpoint for this many consecutive minutes.
    default_setpoint : float
        Default temperature setpoint in degrees for T and ST alarm codes when no custom bound is specified (default 130.0)
    default_power_indication : float
        Default power threshold in kW for SP alarm codes when no custom bound is specified (default 1.0)
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4)
    """
    def __init__(self, bounds_df : pd.DataFrame, default_fault_time : int = 3, default_setpoint : float = 130.0, default_power_indication : float = 1.0,
                             default_power_ratio : float = 0.4):
        alarm_tag = 'TMNSTPT'
        self.default_fault_time = default_fault_time
        type_default_dict = {'T' : default_setpoint,
                 'SP': default_power_indication,
                 'TP': default_power_ratio,
                 'ST': default_setpoint}
        super().__init__(bounds_df, alarm_tag,type_default_dict, alarm_db_type='TM_SETPOINT')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            for alarm_id in self.bounds_df['alarm_code_id'].unique():
                id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]

                # Get T and SP alarm codes for this ID
                t_codes = id_group[id_group['alarm_code_type'] == 'T']
                sp_codes = id_group[id_group['alarm_code_type'] == 'SP']
                tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
                st_codes = id_group[id_group['alarm_code_type'] == 'ST']

                # Check for multiple T or SP codes with same ID
                if len(t_codes) > 1 or len(sp_codes) > 1 or len(tp_codes) > 1 or len(st_codes) > 1:
                    raise Exception(f"Improper alarm codes for swing tank setpoint with id {alarm_id}")
                trigger_columns_condition_met = False 
                if len(st_codes) == 1:
                    st_var_name = st_codes.iloc[0]['variable_name']
                    st_setpoint = st_codes.iloc[0]['bound']
                    st_pretty_name = st_codes.iloc[0]['pretty_name']
                    # Check if st_var_name exists in filtered_df
                    if st_var_name in filtered_df.columns:
                        trigger_columns_condition_met = True
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
                                    f"Setpoint altered: {st_pretty_name} was {actual_value} for {streak_length} minutes starting at {start_time} (expected {st_setpoint}).")
                # Check if we have both T and SP
                if len(t_codes) == 1 and len(sp_codes) == 1:
                    t_var_name = t_codes.iloc[0]['variable_name']
                    t_pretty_name = t_codes.iloc[0]['pretty_name']
                    sp_var_name = sp_codes.iloc[0]['variable_name']
                    sp_pretty_name = sp_codes.iloc[0]['pretty_name']
                    sp_power_indication = sp_codes.iloc[0]['bound']
                    t_setpoint = t_codes.iloc[0]['bound']
                    # Check if both variables exist in df
                    if t_var_name in filtered_df.columns and sp_var_name in filtered_df.columns:
                        trigger_columns_condition_met = True
                        # Check for consecutive minutes where SP > default_power_indication
                        # AND T >= default_setpoint
                        power_mask = filtered_df[sp_var_name] >= sp_power_indication
                        temp_mask = filtered_df[t_var_name] >= t_setpoint
                        combined_mask = power_mask & temp_mask

                        # Check for fault_time consecutive minutes
                        consecutive_condition = combined_mask.rolling(window=self.default_fault_time).min() == 1
                        if consecutive_condition.any():
                            # Find all consecutive groups where condition is true
                            group = (consecutive_condition != consecutive_condition.shift()).cumsum()
                            for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                                streak_indices = consecutive_condition[group == group_id].index
                                start_time = streak_indices[0] - pd.Timedelta(minutes=self.default_fault_time - 1)
                                end_time = streak_indices[-1]
                                streak_length = len(streak_indices) + self.default_fault_time - 1
                                actual_temp = filtered_df.loc[streak_indices[0], t_var_name]
                                self._add_an_alarm(start_time, end_time, sp_var_name,
                                    f"High TM Setpoint: {sp_pretty_name} showed draw for {streak_length} minutes starting at {start_time} while {t_pretty_name} was {actual_temp:.1f} F (above {t_setpoint} F).",
                                    certainty="med")

                if len(tp_codes) == 1 and len(sp_codes) == 1:
                    tp_var_name = tp_codes.iloc[0]['variable_name']
                    sp_var_name = sp_codes.iloc[0]['variable_name']
                    sp_pretty_name = sp_codes.iloc[0]['pretty_name']
                    tp_ratio = tp_codes.iloc[0]['bound']
                    # Check if both variables exist in df
                    if tp_var_name in daily_df.columns and sp_var_name in daily_df.columns:
                        trigger_columns_condition_met = True
                        # Check if swing tank power ratio exceeds threshold
                        if day in daily_df.index and daily_df.loc[day, tp_var_name] != 0:
                            power_ratio = daily_df.loc[day, sp_var_name] / daily_df.loc[day, tp_var_name]
                            if power_ratio > tp_ratio:
                                self._add_an_alarm(day, day + timedelta(1), sp_var_name,
                                    f"High temperature maintenance power ratio: {sp_pretty_name} accounted for {power_ratio * 100:.1f}% of daily power (threshold {tp_ratio * 100}%).",
                                    certainty="low")