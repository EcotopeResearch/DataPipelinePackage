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
    Detects high heat pump inlet temperature by checking if the inlet temperature exceeds a threshold
    while the heat pump is running. An alarm triggers if the temperature stays above the threshold for
    fault_time consecutive minutes while the HP is on.

    Variable_Names.csv configuration:
      alarm_codes column: HPINLET:### where ### provides the bound for the variable (see types below).
      variable_name column: determines the role and element ID of the variable. The element ID is derived
        by removing the leading unit type and any trailing 'Inlet'/'Outlet' suffix
        (e.g., 'PowerIn_HPWH1' and 'Temp_HPWH1_Inlet' both yield element ID 'HPWH1' and are paired together).
        PowerIn_[ID] - HP power variable. Bound (###) from alarm_codes is the power threshold (default 1.0)
            above which the HP is considered 'on'.
        Temp_[ID][Inlet] - HP inlet temperature variable. Bound (###) from alarm_codes is the maximum acceptable
            temperature (default 115.0). Alarm triggers when temperature exceeds this while HP is on.

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold for PowerIn variables when no bound is specified (default 1.0).
    default_temp_threshold : float
        Default temperature threshold for Temp variables when no bound is specified (default 115.0).
        Alarm triggers when inlet temperature exceeds this value while the HP is on.
    fault_time : int
        Number of consecutive minutes that both conditions must hold before triggering an alarm (default 5).
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
