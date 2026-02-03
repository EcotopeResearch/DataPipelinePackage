import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class HPWHOutlet(Alarm):
    """
    Detects low heat pump outlet temperature by checking if the outlet temperature falls below a threshold
    while the heat pump is running. The first 10 minutes after each HP turn-on are excluded as a warmup
    period. An alarm triggers if the temperature stays below the threshold for `fault_time` consecutive
    minutes after the warmup period.

    VarNames syntax:
    HPOUTLT_POW_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'.
    HPOUTLT_T_[OPTIONAL ID]:### - Indicates heat pump outlet temperature variable. ### is the temperature threshold (default 140.0)
        that should always be exceeded while the heat pump is on after the 10-minute warmup period.

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    default_temp_threshold : float
        Default temperature threshold for T alarm codes when no custom bound is specified (default 140.0). Alarm triggers when
        temperature falls BELOW this value while heat pump is on (after warmup period).
    fault_time : int
        Number of consecutive minutes that temperature must be below threshold (after warmup) before triggering an alarm (default 5).

    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_threshold : float = 1.0, default_temp_threshold : float = 140.0, fault_time : int = 5):
        alarm_tag = 'HPOUTLT'
        type_default_dict = {
                'POW' : default_power_threshold,
                'T' : default_temp_threshold
            }
        self.fault_time = fault_time
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = True, alarm_db_type='HPWH_OUTLET')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]
                pow_codes = id_group[id_group['alarm_code_type'] == 'POW']
                pow_var_name = pow_codes.iloc[0]['variable_name']
                pow_thresh = pow_codes.iloc[0]['bound']
                t_codes = id_group[id_group['alarm_code_type'] == 'T']
                t_var_name = t_codes.iloc[0]['variable_name']
                t_pretty_name = t_codes.iloc[0]['pretty_name']
                t_thresh = t_codes.iloc[0]['bound']
                if len(t_codes) != 1 or len(pow_codes) != 1:
                    raise Exception(f"Improper alarm codes for balancing valve with id {alarm_id}")
                if pow_var_name in filtered_df.columns and t_var_name in filtered_df.columns:
                    # Check for consecutive minutes where both power and temp exceed thresholds
                    power_mask = filtered_df[pow_var_name] > pow_thresh
                    temp_mask = filtered_df[t_var_name] < t_thresh

                    # Exclude first 10 minutes after each HP turn-on (warmup period)
                    warmup_minutes = 10
                    mask_changes = power_mask != power_mask.shift(1)
                    run_groups = mask_changes.cumsum()
                    cumcount_in_run = power_mask.groupby(run_groups).cumcount() + 1
                    past_warmup_mask = power_mask & (cumcount_in_run > warmup_minutes)

                    combined_mask = past_warmup_mask & temp_mask

                    # Check for fault_time consecutive minutes
                    consecutive_condition = combined_mask.rolling(window=self.fault_time).min() == 1
                    if consecutive_condition.any():
                        # Find all consecutive groups where condition is true
                        group = (consecutive_condition != consecutive_condition.shift()).cumsum()
                        for group_id in consecutive_condition.groupby(group).first()[lambda x: x].index:
                            streak_indices = consecutive_condition[group == group_id].index
                            start_time = streak_indices[0] - pd.Timedelta(minutes=self.fault_time - 1)
                            end_time = streak_indices[-1]
                            streak_length = len(streak_indices) + self.fault_time - 1
                            actual_temp = filtered_df.loc[streak_indices[0], t_var_name]
                            self._add_an_alarm(start_time, end_time, t_var_name,
                                f"Low heat pump outlet temperature: {t_pretty_name} was {actual_temp:.1f} F (below {t_thresh:.1f} F) for {streak_length} minutes while HP was ON starting at {start_time}.")
