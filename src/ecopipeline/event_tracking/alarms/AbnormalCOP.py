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
        Default upper COP bound when no high_alarm value is specified (default 4.5).
    default_low_bound : float
        Default lower COP bound when no low_alarm value is specified (default 0).
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