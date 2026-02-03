import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class LSInconsist(Alarm):
    """
    Detects when reported loadshift mode does not match its expected value during a load shifting event.
    An alarm is triggered if the variable value does not equal the expected value during the
    time periods defined in the load shifting schedule for that mode.

    VarNames syntax:
    SOOSCHD_[mode]:### - Indicates a variable that should equal ### during [mode] load shifting events.
        [mode] can be: normal, loadUp, shed, criticalPeak, gridEmergency, advLoadUp
        ### is the expected value (e.g., SOOSCHD_loadUp:1 means the variable should be 1 during loadUp events)
    """
    def __init__(self, bounds_df : pd.DataFrame):
        alarm_tag = 'SOOSCHD'
        type_default_dict = {}
        super().__init__(bounds_df, alarm_tag, type_default_dict, two_part_tag=True, alarm_db_type='LS_INCONSIST')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        ls_df = config.get_ls_df()
        if ls_df.empty:
            return # no load shifting events to check

        valid_modes = ['loadUp', 'shed', 'criticalPeak', 'gridEmergency', 'advLoadUp']

        for _, row in self.bounds_df.iterrows():
            mode = row['alarm_code_type']
            if mode not in valid_modes and mode != 'normal':
                continue

            var_name = row['variable_name']
            pretty_name = row['pretty_name']
            expected_value = row['bound']

            if var_name not in df.columns:
                continue

            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]

                if filtered_df.empty:
                    continue

                if mode == 'normal':
                    # For 'normal' mode, check periods NOT covered by any load shifting events
                    normal_df = filtered_df.copy()
                    if not ls_df.empty:
                        mask = pd.Series(True, index=normal_df.index)
                        for _, event_row in ls_df.iterrows():
                            event_start = event_row['startDateTime']
                            event_end = event_row['endDateTime']
                            mask &= ~((normal_df.index >= event_start) & (normal_df.index < event_end))
                        normal_df = normal_df[mask]

                    if normal_df.empty:
                        continue

                    # Check if any values don't match the expected value during normal periods
                    mismatch_mask = normal_df[var_name] != expected_value

                    if mismatch_mask.any():
                        # Find all consecutive streaks of mismatches
                        group = (mismatch_mask != mismatch_mask.shift()).cumsum()

                        for group_id in mismatch_mask.groupby(group).first()[lambda x: x].index:
                            streak_indices = mismatch_mask[group == group_id].index
                            start_time = streak_indices[0]
                            end_time = streak_indices[-1]
                            streak_length = len(streak_indices)
                            actual_value = normal_df.loc[start_time, var_name]

                            self._add_an_alarm(start_time, end_time, var_name,
                                f"Load shift mode inconsistency: {pretty_name} was {actual_value} for {streak_length} minutes starting at {start_time} during normal operation (expected {expected_value}).")
                else:
                    # For load shifting modes, check periods covered by those specific events
                    mode_events = ls_df[ls_df['event'] == mode]
                    if mode_events.empty:
                        continue

                    # Check each load shifting event for this mode on this day
                    for _, event_row in mode_events.iterrows():
                        event_start = event_row['startDateTime']
                        event_end = event_row['endDateTime']

                        # Filter for data during this event
                        event_df = filtered_df.loc[(filtered_df.index >= event_start) & (filtered_df.index < event_end)]

                        if event_df.empty:
                            continue

                        # Check if any values don't match the expected value
                        mismatch_mask = event_df[var_name] != expected_value

                        if mismatch_mask.any():
                            # Find all consecutive streaks of mismatches
                            group = (mismatch_mask != mismatch_mask.shift()).cumsum()

                            for group_id in mismatch_mask.groupby(group).first()[lambda x: x].index:
                                streak_indices = mismatch_mask[group == group_id].index
                                start_time = streak_indices[0]
                                end_time = streak_indices[-1]
                                streak_length = len(streak_indices)
                                actual_value = event_df.loc[start_time, var_name]

                                self._add_an_alarm(start_time, end_time, var_name,
                                    f"Load shift mode inconsistency: {pretty_name} was {actual_value} for {streak_length} minutes starting at {start_time} during {mode} event (expected {expected_value}).")