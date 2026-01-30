import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class BalancingValve(Alarm):
    """
    Detects recirculation balance issues by comparing sum of ER (equipment recirculation) heater
    power to either total power or heating output.

    VarNames syntax:
    BV_ER_[OPTIONAL ID] - Indicates a power variable for an ER heater (equipment recirculation).
        Multiple ER variables with the same ID will be summed together.
    BV_TP_[OPTIONAL ID]:### - Indicates the Total Power of the system. Optional ### for the percentage
        threshold that should not be crossed by the ER elements (default 0.4 for 40%).
        Alarm triggers when sum of ER >= total_power * threshold.
    BV_OUT_[OPTIONAL ID] - Indicates the heating output variable the ER heating contributes to.
        Alarm triggers when sum of ER > sum of OUT * 0.95 (i.e., ER exceeds 95% of heating output).
        Multiple OUT variables with the same ID will be summed together.

    Note: Each alarm ID requires at least one ER code AND either one TP code OR at least one OUT code.
    If a TP code exists for an ID, it takes precedence over OUT codes.

    Parameters
    ----------
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4).

    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_ratio : float = 0.4):
        alarm_tag = 'BV'
        type_default_dict = {'TP' : default_power_ratio}
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = True, alarm_db_type='BALANCING_VALVE', daily_only=True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]
            out_codes = id_group[id_group['alarm_code_type'] == 'OUT']
            tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
            er_codes = id_group[id_group['alarm_code_type'] == 'ER']
            if len(er_codes) < 1 or (len(out_codes) < 1 and len(tp_codes) != 1):
                raise Exception(f"Improper alarm codes for balancing valve with id {alarm_id}")
            er_var_names = er_codes['variable_name'].tolist()
            if len(tp_codes) == 1 and tp_codes.iloc[0]['variable_name']in daily_df.columns:
                tp_var_name = tp_codes.iloc[0]['variable_name']
                tp_bound = tp_codes.iloc[0]['bound']
                for day in daily_df.index:

                    # Check if all ER variables exist in daily_df
                    if all(var in daily_df.columns for var in er_var_names):
                        # Sum all ER variables for this day
                        er_sum = daily_df.loc[day, er_var_names].sum()
                        tp_value = daily_df.loc[day, tp_var_name]

                        # Check if sum of ER >= OUT value
                        if er_sum >= tp_value*tp_bound:
                            self._add_an_alarm(day, day + timedelta(1), tp_var_name, f"Recirculation imbalance: Sum of recirculation equipment ({er_sum:.2f}) exceeds or equals {(tp_bound * 100):.2f}% of total power.", add_one_minute_to_end=False)
            elif len(out_codes) >= 1:
                out_var_names = out_codes['variable_name'].tolist()
                for day in daily_df.index:

                    # Check if all ER variables exist in daily_df
                    if all(var in daily_df.columns for var in er_var_names) and all(var in daily_df.columns for var in out_var_names):
                        # Sum all ER variables for this day
                        er_sum = daily_df.loc[day, er_var_names].sum()
                        out_sum = daily_df.loc[day, out_var_names].sum()

                        # Check if sum of ER >= OUT value
                        if er_sum > out_sum:
                            self._add_an_alarm(day, day + timedelta(1), out_codes.iloc[0]['variable_name'], f"Recirculation imbalance: Sum of recirculation equipment power ({er_sum:.2f} kW) exceeds TM heating output ({out_sum:.2f} kW).",add_one_minute_to_end=False)
