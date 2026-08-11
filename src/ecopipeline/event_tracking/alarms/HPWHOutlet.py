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

    Variable_Names.csv configuration:
      alarm_codes column: HPOUTLT:### where ### provides the bound for the variable (see types below).
      variable_name column: determines the role and element ID of the variable. The element ID is derived
        by removing the leading unit type and any trailing 'Inlet'/'Outlet' suffix
        (e.g., 'PowerIn_HPWH1' and 'Temp_HPWH1_Outlet' both yield element ID 'HPWH1' and are paired together).
        PowerIn_[ID] - HP power variable. Bound (###) from alarm_codes is the power threshold (default 1.0)
            above which the HP is considered 'on'.
        Temp_[ID][Outlet] - HP outlet temperature variable. Bound (###) from alarm_codes is the minimum
            acceptable temperature (default 140.0). Alarm triggers when temp falls below this after warmup.

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold for PowerIn variables when no bound is specified (default 1.0).
    default_temp_threshold : float
        Default temperature threshold for Temp variables when no bound is specified (default 140.0).
        Alarm triggers when outlet temperature falls BELOW this value after the warmup period.
    fault_time : int
        Number of consecutive minutes that temperature must be below threshold (after warmup) before triggering an alarm (default 5).
    warmup_minutes : int
        Number of minutes after each HP turn-on to exclude as a warmup period (default 10).
    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_threshold : float = 1.0, default_temp_threshold : float = 140.0, fault_time : int = 5,
                 warmup_minutes : int = 10):
        alarm_tag = 'HPOUTLT'
        type_default_dict = {
                'PowerIn' : default_power_threshold,
                'Temp' : default_temp_threshold
            }
        self.fault_time = fault_time
        self.warmup_minutes = warmup_minutes
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = True, element_id_matching=True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            id_group = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]
            pow_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
            t_codes = id_group[id_group['alarm_code_type'] == 'Temp']
            if len(t_codes) != 1 or len(pow_codes) != 1:
                raise Exception(f"Improper alarm codes for balancing valve with id {alarm_id}")
            pow_var_name = pow_codes.iloc[0]['variable_name']
            pow_thresh = pow_codes.iloc[0]['bound']
            t_var_name = t_codes.iloc[0]['variable_name']
            t_pretty_name = t_codes.iloc[0]['pretty_name']
            t_thresh = t_codes.iloc[0]['bound']
            self.record_set_alarm([t_var_name, pow_var_name])
            if pow_var_name in df.columns and t_var_name in df.columns:
                power_mask = df[pow_var_name] > pow_thresh
                temp_mask = df[t_var_name] < t_thresh

                # Exclude the warmup period after each HP turn-on. Elapsed time is measured
                # from the timestamps rather than counted in rows, and a data gap starts a
                # new run, so an interrupted run does not carry accumulated runtime forward.
                run_ids = self._streak_ids(power_mask, self._gap_tolerance(self.warmup_minutes))
                sample_times = power_mask.index.to_series()
                run_start = sample_times.groupby(run_ids).transform('first')
                past_warmup_mask = power_mask & ((sample_times - run_start) >= pd.Timedelta(minutes=self.warmup_minutes))

                combined_mask = past_warmup_mask & temp_mask

                # fault_time is a duration in minutes, so the sample interval of the data does
                # not change the result, and a data gap is never counted as fault time
                for start_time, end_time, duration in self._iter_sustained_streaks(combined_mask, self.fault_time):
                    actual_temp = df.loc[start_time, t_var_name]
                    self._add_an_alarm(start_time, end_time, t_var_name,
                        f"Low heat pump outlet temperature: {t_pretty_name} was {actual_temp:.1f} F (below {t_thresh:.1f} F) for {duration:.0f} minutes while HP was ON starting at {start_time}.",
                        add_one_interval_to_end=False)
