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
    setpoint_fault_time : int
        Number of minutes a Setpoint variable must differ from its expected value before
        triggering an alarm (default 10).
    """
    def __init__(self, bounds_df : pd.DataFrame, default_fault_time : int = 3, default_setpoint : float = 130.0, default_power_indication : float = 1.0,
                             default_power_ratio : float = 0.4, setpoint_fault_time : int = 10):
        alarm_tag = 'TMNSTPT'
        self.default_fault_time = default_fault_time
        self.setpoint_fault_time = setpoint_fault_time
        type_default_dict = {'Temp' : default_setpoint,
                 'PowerIn': default_power_indication,
                 'PowerIn_Total': default_power_ratio,
                 'Setpoint': default_setpoint}
        super().__init__(bounds_df, alarm_tag,type_default_dict, element_id_matching = True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        tp_codes = self.bounds_df[self.bounds_df['alarm_code_type'] == 'PowerIn_Total']
        all_sp_codes = self.bounds_df[self.bounds_df['alarm_code_type'] == 'PowerIn']

        # Minute level checks. These scan the whole frame at once, so a fault spanning
        # midnight stays a single event, and their thresholds are durations in minutes
        # rather than row counts.
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]

            # Get T and SP alarm codes for this ID
            t_codes = id_group[id_group['alarm_code_type'] == 'Temp']
            sp_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
            st_codes = id_group[id_group['alarm_code_type'] == 'Setpoint']

            # Check for multiple T or SP codes with same ID
            if len(t_codes) > 1 or len(sp_codes) > 1 or len(tp_codes) > 1 or len(st_codes) > 1:
                raise Exception(f"Improper alarm codes for swing tank setpoint with id {alarm_id}")
            if len(st_codes) == 1:
                st_var_name = st_codes.iloc[0]['variable_name']
                self.record_set_alarm([st_var_name])
                st_setpoint = st_codes.iloc[0]['bound']
                st_pretty_name = st_codes.iloc[0]['pretty_name']
                # Check if st_var_name exists in df
                if st_var_name in df.columns:
                    # Check if setpoint was altered for setpoint_fault_time minutes
                    altered_mask = df[st_var_name] != st_setpoint
                    for start_time, end_time, duration in self._iter_sustained_streaks(altered_mask, self.setpoint_fault_time):
                        actual_value = df.loc[start_time, st_var_name]
                        self._add_an_alarm(start_time, end_time, st_var_name,
                            f"Setpoint altered: {st_pretty_name} was {actual_value} for {duration:.0f} minutes starting at {start_time} (expected {st_setpoint}).",
                            add_one_interval_to_end=False)
            # Check if we have both T and SP
            if len(t_codes) == 1 and len(sp_codes) == 1:
                t_var_name = t_codes.iloc[0]['variable_name']
                t_pretty_name = t_codes.iloc[0]['pretty_name']
                sp_var_name = sp_codes.iloc[0]['variable_name']
                sp_pretty_name = sp_codes.iloc[0]['pretty_name']
                sp_power_indication = sp_codes.iloc[0]['bound']
                t_setpoint = t_codes.iloc[0]['bound']
                self.record_set_alarm([t_var_name, sp_var_name])
                # Check if both variables exist in df
                if t_var_name in df.columns and sp_var_name in df.columns:
                    # Equipment drawing power while its temperature sits at or above setpoint
                    power_mask = df[sp_var_name] >= sp_power_indication
                    temp_mask = df[t_var_name] >= t_setpoint
                    combined_mask = power_mask & temp_mask

                    for start_time, end_time, duration in self._iter_sustained_streaks(combined_mask, self.default_fault_time):
                        actual_temp = df.loc[start_time, t_var_name]
                        self._add_an_alarm(start_time, end_time, sp_var_name,
                            f"High TM Setpoint: {sp_pretty_name} showed draw for {duration:.0f} minutes starting at {start_time} while {t_pretty_name} was {actual_temp:.1f} F (above {t_setpoint} F).",
                            add_one_interval_to_end=False, certainty="med")

        # Daily power ratio check. This one is genuinely per day, so it keeps its day loop.
        if len(tp_codes) == 1 and len(all_sp_codes) >= 1:
            tp_var_name = tp_codes.iloc[0]['variable_name']
            sp_var_names = all_sp_codes['variable_name']
            self.record_set_alarm([tp_var_name]+sp_var_names.to_list())
            tm_power = daily_df[sp_var_names].sum(axis=1)
            tp_ratio = tp_codes.iloc[0]['bound']
            # Check if both variables exist in df
            if tp_var_name in daily_df.columns:
                for day in daily_df.index:
                    # Check if swing tank power ratio exceeds threshold
                    if daily_df.loc[day, tp_var_name] != 0:
                        power_ratio = tm_power.loc[day] / daily_df.loc[day, tp_var_name]
                        if power_ratio > tp_ratio:
                            self._add_an_alarm(day, day + timedelta(1), tp_var_name,
                                f"High temperature maintenance power ratio: TM heating accounted for {power_ratio * 100:.1f}% of daily power (threshold {tp_ratio * 100}%).",
                                certainty="low", add_one_interval_to_end = False)