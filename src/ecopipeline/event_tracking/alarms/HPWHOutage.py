import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class HPWHOutage(Alarm):
    """
    Detects possible heat pump failures or outages by checking if heat pump power consumption falls below
    an expected ratio of total system power over a rolling period, or by checking for non-zero values in
    a direct alarm variable from the heat pump controller.

    Variable_Names.csv configuration:
      alarm_codes column: HPOUTGE or HPOUTGE:### where ### provides the bound for the variable (see types below).
      variable_name column: determines the role of the variable by its first underscore-separated part:
        PowerIn_[name] - HP power variable. Bound (###) from alarm_codes is the minimum expected ratio of HP
            power to total system power over the rolling period (default 0.3 for 30%). Alarm triggers when
            HP power falls below this ratio. Must be in the same units as the PowerIn_Total variable.
        PowerIn_Total - Total system power variable. No bound needed in alarm_codes (use just HPOUTGE).
            Required when using PowerIn variables.
        Alarm_[name] - Direct alarm variable from HP controller. No bound needed in alarm_codes (use just HPOUTGE).
            Alarm triggers on any non-zero value. If triggered, the ratio check is skipped.

    Parameters
    ----------
    day_table_name : str
        Name of the site's daily aggregate value table in the database for fetching historical data.
    default_power_ratio : float
        Default minimum ratio threshold for PowerIn variables when no bound is specified (default 0.3).
        Alarm triggers when HP power falls below this ratio of total power over the rolling period.
    ratio_period_days : int
        Number of days to use for the rolling power ratio calculation (default 7). Must be greater than 1.
    """
    def __init__(self, bounds_df : pd.DataFrame, day_table_name : str, default_power_ratio : float = 0.3,
                   ratio_period_days : int = 7):
        alarm_tag = 'HPOUTGE'
        type_default_dict = {
                'PowerIn': default_power_ratio,
                'PowerIn_Total': None,
                'Alarm': None
            }
        self.day_table_name = day_table_name # TODO this could be a security issue. Swap it for config manager
        self.default_power_ratio = default_power_ratio
        self.ratio_period_days = ratio_period_days
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = True, alarm_db_type='HPWH_INLET')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            alarm_triggered = False
            id_group = self.bounds_df[(self.bounds_df['alarm_code_id'] == alarm_id) | (self.bounds_df['alarm_code_type'] == 'PowerIn_Total')]
            # Get T and SP alarm codes for this ID
            pow_codes = id_group[id_group['alarm_code_type'] == 'PowerIn']
            tp_codes = id_group[id_group['alarm_code_type'] == 'PowerIn_Total']
            alrm_codes = id_group[id_group['alarm_code_type'] == 'Alarm']
            if len(alrm_codes) > 0:
                for i in range(len(alrm_codes)):
                    alrm_var_name = alrm_codes.iloc[i]['variable_name']
                    alrm_pretty_name = alrm_codes.iloc[i]['pretty_name']
                    if alrm_var_name in df.columns:
                        for day in daily_df.index:
                            next_day = day + pd.Timedelta(days=1)
                            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                            if not filtered_df.empty:
                                # Find all consecutive blocks where alarm variable is non-zero
                                alarm_mask = filtered_df[alrm_var_name] != 0
                                if alarm_mask.any():
                                    # Find consecutive groups
                                    group = (alarm_mask != alarm_mask.shift()).cumsum()

                                    # Iterate through each consecutive block of non-zero values
                                    for group_id in alarm_mask.groupby(group).first()[lambda x: x].index:
                                        streak_indices = alarm_mask[group == group_id].index
                                        start_time = streak_indices[0]
                                        end_time = streak_indices[-1]
                                        streak_length = len(streak_indices)
                                        alarm_value = filtered_df.loc[start_time, alrm_var_name]

                                        self._add_an_alarm(start_time, end_time, alrm_var_name,
                                            f"Heat pump alarm triggered: {alrm_pretty_name} was {alarm_value} for {streak_length} minutes starting at {start_time}.")
                                        alarm_triggered = True
            elif len(pow_codes) > 0 and len(tp_codes) != 1:
                raise Exception(f"Improper alarm codes for heat pump outage with id {alarm_id}. Requires 1 total power (TP) variable.")
            if len(pow_codes) > 0 and len(tp_codes) == 1 and not alarm_triggered:
                if self.ratio_period_days <= 1:
                    print("HP Outage alarm period, ratio_period_days, must be more than 1")
                else: 
                    tp_var_name = tp_codes.iloc[0]['variable_name'] 
                    daily_df_copy = daily_df.copy()
                    daily_df_copy = self._append_previous_days_to_df(daily_df_copy, config, self.ratio_period_days, self.day_table_name)
                    for i in range(self.ratio_period_days - 1, len(daily_df_copy)):
                        start_idx = i - self.ratio_period_days + 1
                        end_idx = i + 1
                        day = daily_df_copy.index[i]
                        block_data = daily_df_copy.iloc[start_idx:end_idx].sum()
                        for j in range(len(pow_codes)):
                            pow_var_name = pow_codes.iloc[j]['variable_name']
                            pow_var_bound = pow_codes.iloc[j]['bound']
                            if block_data[pow_var_name] < block_data[tp_var_name] * pow_var_bound:
                                self._add_an_alarm(day, day + timedelta(1), pow_var_name, f"Possible Heat Pump failure or outage.", False,
                                                   certainty='med')