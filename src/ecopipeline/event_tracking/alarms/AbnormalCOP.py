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
    Detects abnormal COP (Coefficient of Performance) values by checking if any COP variable
    falls outside its expected high/low bounds on a given day.

    Note: This alarm does not use the alarm_codes column. Variables are matched by their column name
    pattern in daily_df (must start with 'COP' or 'SystemCOP'), and bounds are read directly from the
    high_alarm and low_alarm columns in Variable_Names.csv.

    Variable_Names.csv columns:
    variable_name - Name of the COP variable to monitor (must match a column starting with 'COP' or 'SystemCOP' in daily_df).
    high_alarm - Upper bound for acceptable COP. Alarm triggers if daily COP exceeds this value. Default: 4.5.
    low_alarm - Lower bound for acceptable COP. Alarm triggers if daily COP falls below this value. Default: 0.
    pretty_name - (Optional) Display name for the variable in alarm messages. Defaults to variable_name.

    Parameters
    ----------
    default_high_bound : float
        Default upper COP bound when no high_alarm value is specified (default 5.5).
    default_low_bound : float
        Default lower COP bound when no low_alarm value is specified (default 1.5).
    """
    def __init__(self, bounds_df : pd.DataFrame, default_high_bound : float = 5.5, default_low_bound : float = 1.5):
        self.default_high_bound = default_high_bound
        self.default_low_bound = default_low_bound
        type_default_dict = {'COP': [default_low_bound, default_high_bound],
            'SystemCOP': [default_low_bound, default_high_bound]}
        super().__init__(bounds_df, "ABNRMCP",type_default_dict, two_part_tag = False, range_bounds=True, daily_only=True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        for bound_var in self.bounds_df['variable_name'].unique():
            rows = self.bounds_df[self.bounds_df['variable_name'] == bound_var]
            bounds = rows.iloc[0]
            low_bound = bounds['bound']
            high_bound = bounds['bound2']
            for day, day_values in daily_df.iterrows():
                if bound_var in daily_df.columns and not day_values[bound_var] is None and (day_values[bound_var] > high_bound or day_values[bound_var] < low_bound):
                    alarm_str = f"Unexpected COP Value detected: {bounds['pretty_name']} = {round(day_values[bound_var],2)}"
                    self._add_an_alarm(day, day + timedelta(1), bound_var, alarm_str, add_one_minute_to_end=False)