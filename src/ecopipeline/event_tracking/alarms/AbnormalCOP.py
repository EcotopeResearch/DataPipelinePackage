import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class AbnormalCOP(Alarm):
    """
    Detects unexpected state of operation (SOO) changes by checking if the heat pump turns on or off
    when the temperature is not near the expected aquastat setpoint thresholds. An alarm is triggered
    if the HP turns on/off and the corresponding temperature is more than 5.0 degrees away from the
    expected threshold.

    VarNames syntax:
    SOOCHNG_POW:### - Indicates a power variable for the heat pump system (should be total power across all primary heat pumps). ### is the power threshold (default 1.0) above which
        the heat pump system is considered 'on'.
    SOOCHNG_ON_[Mode ID]:### - Indicates the temperature variable at the ON aquastat fraction. ### is the temperature (default 115.0)
        that should trigger the heat pump to turn ON. Mode ID should be the load up mode from ['loadUp','shed','criticalPeak','gridEmergency','advLoadUp','normal'] or left blank for normal mode
    SOOCHNG_OFF_[Mode ID]:### - Indicates the temperature variable at the OFF aquastat fraction (can be same as ON aquastat). ### is the temperature (default 140.0)
        that should trigger the heat pump to turn OFF. Mode ID should be the load up mode from ['loadUp','shed','criticalPeak','gridEmergency','advLoadUp','normal'] or left blank for normal mode

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    default_on_temp : float
        Default ON temperature threshold (default 115.0). When the HP turns on, an alarm triggers if the temperature
        is more than 5.0 degrees away from this value.
    default_off_temp : float
        Default OFF temperature threshold (default 140.0). When the HP turns off, an alarm triggers if the temperature
        is more than 5.0 degrees away from this value.
    """
    def __init__(self, bounds_df : pd.DataFrame, default_high_bound : float = 4.5, default_low_bound : float = 0):
        self.default_high_bound = default_high_bound
        self.default_low_bound = default_low_bound
        
        super().__init__(bounds_df, None, {}, alarm_db_type='ABNORMAL_COP', daily_only=True)

    def _process_bounds_df_alarm_codes(self, og_bounds_df : pd.DataFrame) -> pd.DataFrame:
        bounds_df = og_bounds_df.copy()
        if not "variable_name" in bounds_df.columns:
            raise Exception(f"variable_name is not present in Variable_Names.csv")
        if not 'pretty_name' in bounds_df.columns:
            bounds_df['pretty_name'] = bounds_df['variable_name']
        else:
            bounds_df['pretty_name'] = bounds_df['pretty_name'].fillna(bounds_df['variable_name'])
        if not 'high_alarm' in bounds_df.columns:
            bounds_df['high_alarm'] = self.default_high_bound
        else:
            bounds_df['high_alarm'] = bounds_df['high_alarm'].fillna(self.default_high_bound)
        if not 'low_alarm' in bounds_df.columns:
            bounds_df['low_alarm'] = self.default_low_bound
        else:
            bounds_df['low_alarm'] = bounds_df['low_alarm'].fillna(self.default_low_bound)

        bounds_df = bounds_df.loc[:, ["variable_name", "high_alarm", "low_alarm", "pretty_name"]]
        bounds_df.dropna(axis=0, thresh=2, inplace=True)
        bounds_df.set_index(['variable_name'], inplace=True)

        return bounds_df

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        cop_pattern = re.compile(r'^(COP\w*|SystemCOP\w*)$')
        cop_columns = [col for col in daily_df.columns if re.match(cop_pattern, col)]

        if not daily_df.empty and len(cop_columns) > 0:
            for bound_var, bounds in self.bounds_df.iterrows():
                if bound_var in cop_columns:
                    for day, day_values in daily_df.iterrows():
                        if not day_values[bound_var] is None and (day_values[bound_var] > bounds['high_alarm'] or day_values[bound_var] < bounds['low_alarm']):
                            alarm_str = f"Unexpected COP Value detected: {bounds['pretty_name']} = {round(day_values[bound_var],2)}"
                            self._add_an_alarm(day, day + timedelta(1), bound_var, alarm_str, add_one_minute_to_end=False)