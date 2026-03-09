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
    Detects temperature maintenance (TM) equipment issues including setpoint alterations, overheating
    while powered on, and excessive power consumption relative to total system power.

    Variable_Names.csv configuration:
      alarm_codes column: TMNSTPT or TMNSTPT:### where ### provides the bound for the variable (see types below).
      variable_name column: determines the role of the variable by its first underscore-separated part.
        Variables with the same element ID (derived from the rest of the variable name, Inlet/Outlet stripped)
        are grouped together:
        Temp_[ID][Outlet] - TM equipment temperature variable. Bound (###) from alarm_codes is the maximum acceptable
            temperature (default 130.0). Alarm triggers when equipment is on and temperature stays at or above
            this for default_fault_time consecutive minutes.
        PowerIn_[ID] - TM equipment power variable. Bound (###) from alarm_codes is the minimum power
            (default 1.0) to consider the equipment 'on'. Used with Temp for overheating detection and with
            PowerIn_Total for ratio comparison.
        PowerIn_Total - Total system power variable. Bound (###) from alarm_codes is the ratio threshold
            (default 0.4). Alarm triggers if sum of TM power / total power exceeds this on a given day.
        Setpoint_[ID] - Setpoint variable that should remain constant. Bound (###) from alarm_codes is the
            expected setpoint value (default 130.0). Alarm triggers if value differs for 10+ consecutive minutes.

    Parameters
    ----------
    default_fault_time : int
        Number of consecutive minutes for Temp+PowerIn overheating alarms (default 3).
    default_setpoint : float
        Default expected value for Temp and Setpoint variables when no bound is specified (default 130.0).
    default_power_indication : float
        Default power threshold for PowerIn variables when no bound is specified (default 1.0).
    default_power_ratio : float
        Default ratio threshold for PowerIn_Total variables when no bound is specified (default 0.4).
        Alarm triggers when TM power / total power exceeds this threshold.
    """
    def __init__(self, bounds_df : pd.DataFrame, default_fault_time : int = 3, default_setpoint : float = 130.0, default_power_indication : float = 1.0,
                             default_power_ratio : float = 0.4):
        alarm_tag = 'TMNSTPT'
        self.default_fault_time = default_fault_time
        type_default_dict = {'Temp' : default_setpoint,
                 'PowerIn': default_power_indication,
                 'PowerIn_Total': default_power_ratio,
                 'Setpoint': default_setpoint}
        super().__init__(bounds_df, alarm_tag,type_default_dict, alarm_db_type='TM_SETPOINT', element_id_matching = True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            tp_codes = self.bounds_df[self.bounds_df['alarm_code_type'] == 'PowerIn_Total']
            all_sp_codes = self.bounds_df[self.bounds_df['alarm_code_type'] == 'PowerIn']
            for alarm_id in self.bounds_df['alarm_code_id'].unique():
                id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]

                # Get T and SP alarm codes for this ID
                t_codes = id_group[id_group['alarm_code_type'] == 'Temp']
                sp_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
                st_codes = id_group[id_group['alarm_code_type'] == 'Setpoint']

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

            if len(tp_codes) == 1 and len(all_sp_codes) >= 1:
                tp_var_name = tp_codes.iloc[0]['variable_name']
                sp_var_names = all_sp_codes['variable_name']
                daily_df['PowerIn_all_TM'] = daily_df[sp_var_names].sum(axis=1)
                tp_ratio = tp_codes.iloc[0]['bound']
                # Check if both variables exist in df
                if tp_var_name in daily_df.columns:
                    trigger_columns_condition_met = True
                    # Check if swing tank power ratio exceeds threshold
                    if day in daily_df.index and daily_df.loc[day, tp_var_name] != 0:
                        power_ratio = daily_df.loc[day, 'PowerIn_all_TM'] / daily_df.loc[day, tp_var_name]
                        if power_ratio > tp_ratio:
                            self._add_an_alarm(day, day + timedelta(1), tp_var_name,
                                f"High temperature maintenance power ratio: TM heating accounted for {power_ratio * 100:.1f}% of daily power (threshold {tp_ratio * 100}%).",
                                certainty="low", add_one_minute_to_end = False)