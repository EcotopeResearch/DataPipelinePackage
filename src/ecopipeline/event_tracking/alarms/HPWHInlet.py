import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class HPWHInlet(Alarm):
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    HPINLET_POW_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'
    HPINLET_T_[OPTIONAL ID]:### - Indicates heat pump inlet temperature variable. ### is the temperature threshold (default 120.0)
        that should not be exceeded while the heat pump is on

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 0.4). Heat pump is considered 'on'
        when power exceeds this value.
    default_temp_threshold : float
        Default temperature threshold for T alarm codes when no custom bound is specified (default 120.0). Alarm triggers when
        temperature exceeds this value while heat pump is on.
    fault_time : int
        Number of consecutive minutes that both power and temperature must exceed their thresholds before triggering an alarm (default 10).

    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_threshold : float = 1.0, default_temp_threshold : float = 115.0, fault_time : int = 5):
        alarm_tag = 'HPINLET'
        type_default_dict = {
                'PowerIn' : default_power_threshold,
                'Temp' : default_temp_threshold
            }
        self.fault_time = fault_time
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = True, alarm_db_type='HPWH_INLET', element_id_matching = True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]
                pow_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
                pow_var_name = pow_codes.iloc[0]['variable_name']
                pow_thresh = pow_codes.iloc[0]['bound']
                t_codes = id_group[id_group['alarm_code_type'] == 'Temp']
                t_var_name = t_codes.iloc[0]['variable_name']
                t_pretty_name = t_codes.iloc[0]['pretty_name']
                t_thresh = t_codes.iloc[0]['bound']
                if len(t_codes) != 1 or len(pow_codes) != 1:
                    raise Exception(f"Improper alarm codes for HPWH Inlet alarm for element with id {alarm_id}")
                if pow_var_name in filtered_df.columns and t_var_name in filtered_df.columns:
                    # Check for consecutive minutes where both power and temp exceed thresholds
                    power_mask = filtered_df[pow_var_name] > pow_thresh
                    temp_mask = filtered_df[t_var_name] > t_thresh
                    combined_mask = power_mask & temp_mask

                    # Check for fault_time consecutive minutes
                    consecutive_condition = combined_mask.rolling(window=self.fault_time).min() == 1
                    if consecutive_condition.any():
                        group = (consecutive_condition != consecutive_condition.shift()).cumsum()
                        for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                            streak_indices = consecutive_condition[group == group_id].index
                            start_time = streak_indices[0] - pd.Timedelta(minutes=self.fault_time - 1)
                            end_time = streak_indices[-1]
                            streak_length = len(streak_indices) + self.fault_time - 1
                            self._add_an_alarm(start_time, end_time, t_var_name,
                                f"High heat pump inlet temperature: {t_pretty_name} was above {t_thresh:.1f} F for {streak_length} minutes while HP was ON starting at {start_time}.")
