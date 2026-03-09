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
    Detects recirculation balance issues by comparing the sum of electric recirculation (ER) heater
    power to either total system power or heating output.

    Variable_Names.csv configuration:
      alarm_codes column: BALVALV or BALVALV:### where ### provides the bound for the variable (see types below).
      variable_name column: determines the role of the variable by its first underscore-separated part:
        PowerIn_[name] - Electric recirculation (ER) heater power variable. Multiple allowed; daily values are summed.
            No bound needed in alarm_codes (use just BALVALV).
        PowerIn_Total - Total system power variable. Bound (###) from alarm_codes is the fraction threshold
            (default 0.4 for 40%). Alarm triggers when sum of ER >= total power * threshold. If present,
            takes precedence over HeatOut variables.
        HeatOut_[name] - Heating output variable. Multiple allowed; values are summed. Alarm triggers when
            sum of ER power exceeds sum of heating output. Only used if no PowerIn_Total variable is configured.
            No bound needed in alarm_codes (use just BALVALV).

    Note: Requires at least one PowerIn variable AND either one PowerIn_Total or at least one HeatOut variable.

    Parameters
    ----------
    default_power_ratio : float
        Default ratio threshold for PowerIn_Total variables when no bound is specified (default 0.4).
        Alarm triggers when sum of ER power >= total power * threshold.
    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_ratio : float = 0.4):
        alarm_tag = 'BALVALV'
        type_default_dict = {'PowerIn_Total' : default_power_ratio}
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = True, alarm_db_type='BALANCING_VALVE', daily_only=True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            alarm_triggered = False
            id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]
            out_codes = id_group[id_group['alarm_code_type'] == 'HeatOut']
            tp_codes = id_group[id_group['alarm_code_type'] == 'PowerIn_Total']
            er_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
            if len(er_codes) < 1 or (len(out_codes) < 1 and len(tp_codes) != 1):
                raise Exception(f"Improper alarm codes for balancing valve. Requires at least one Power variable for electric resistance element and one heat output or total power variable.")
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
                            self._add_an_alarm(day, day + timedelta(1), tp_var_name, 
                                               f"Recirculation imbalance: Sum of recirculation equipment ({er_sum:.2f}) exceeds or equals {(tp_bound * 100):.2f}% of total power.", 
                                               add_one_minute_to_end=False, certainty="low")
                            alarm_triggered = True
            if len(out_codes) >= 1 and not alarm_triggered:
                out_var_names = out_codes['variable_name'].tolist()
                for day in daily_df.index:

                    # Check if all ER variables exist in daily_df
                    if all(var in daily_df.columns for var in er_var_names) and all(var in daily_df.columns for var in out_var_names):
                        # Sum all ER variables for this day
                        er_sum = daily_df.loc[day, er_var_names].sum()
                        out_sum = daily_df.loc[day, out_var_names].sum()

                        # Check if sum of ER >= OUT value
                        if er_sum > out_sum:
                            self._add_an_alarm(day, day + timedelta(1), out_codes.iloc[0]['variable_name'], 
                                               f"Recirculation imbalance: Sum of recirculation equipment power ({er_sum:.2f} kW) exceeds TM heating output ({out_sum:.2f} kW).",
                                               add_one_minute_to_end=False, certainty="low")
