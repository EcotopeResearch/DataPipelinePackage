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
    Detects when a variable's value does not match its expected value during a load shifting event
    (or during normal operation). An alarm is triggered whenever the variable differs from the
    expected value for the relevant time period.

    Variable_Names.csv configuration:
      alarm_codes column: SOOSCHD_[mode]:###
        [mode] must be one of: normal, loadUp, shed, criticalPeak, gridEmergency, advLoadUp.
        Bound (###) from alarm_codes is the expected value of the variable during that mode.

    Known limitations
    -----------------
    There is no fault time here: any mismatch alarms, however brief. Streak durations are
    measured in minutes and split on data gaps, so the sample interval of the data does not
    change the result. Two things are outstanding:

    1. The day loop is still in place, so a mismatch spanning midnight is recorded as two
       events rather than one. :meth:`Alarm._compress_alarm_df` merges them again when they
       land within one sample interval of each other, which is the usual case, so this mostly
       self-corrects. Removing the loop and scanning the whole frame would be cleaner.
    2. In 'normal' mode the load shift event windows are dropped from the frame before the
       streaks are found. Those removals leave holes in the index that are indistinguishable
       from missing data, so a mismatch either side of a load shift event is reported as two
       separate normal-mode events. That is arguably correct, since normal operation genuinely
       was interrupted, but it is a side effect of how the filtering works rather than a
       deliberate decision.
    """
    def __init__(self, bounds_df : pd.DataFrame):
        alarm_tag = 'SOOSCHD'
        type_default_dict = {}
        super().__init__(bounds_df, alarm_tag, type_default_dict, two_part_tag=True)

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
            self.record_set_alarm([var_name])
            pretty_name = row['pretty_name']
            expected_value = row['bound']

            if var_name not in df.columns:
                continue

            # KNOWN LIMITATION: this day slicing records a mismatch spanning midnight as two
            # events. _compress_alarm_df usually merges them back together. See the Known
            # limitations section of the class docstring.
            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]

                if filtered_df.empty:
                    continue

                if mode == 'normal':
                    # For 'normal' mode, check periods NOT covered by any load shifting events.
                    # KNOWN LIMITATION: dropping those rows leaves holes in the index that read
                    # as data gaps, so a mismatch either side of a load shift event becomes two
                    # separate normal-mode events.
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

                    # Any mismatch alarms, so there is no duration threshold here. Streaks
                    # are still split on data gaps, and their length is measured in minutes
                    # rather than counted in rows.
                    for start_time, end_time, duration, _ in self._iter_runs(mismatch_mask, self._gap_tolerance()):
                        actual_value = normal_df.loc[start_time, var_name]
                        self._add_an_alarm(start_time, end_time, var_name,
                            f"Load shift mode inconsistency: {pretty_name} was {actual_value} for {duration.total_seconds() / 60:.0f} minutes starting at {start_time} during normal operation (expected {expected_value}).",
                            add_one_interval_to_end=False)
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

                        # Any mismatch alarms, so there is no duration threshold here. Streaks
                        # are still split on data gaps, and their length is measured in minutes
                        # rather than counted in rows.
                        for start_time, end_time, duration, _ in self._iter_runs(mismatch_mask, self._gap_tolerance()):
                            actual_value = event_df.loc[start_time, var_name]
                            self._add_an_alarm(start_time, end_time, var_name,
                                f"Load shift mode inconsistency: {pretty_name} was {actual_value} for {duration.total_seconds() / 60:.0f} minutes starting at {start_time} during {mode} event (expected {expected_value}).",
                                add_one_interval_to_end=False)
                                
    def _organize_alarm_codes(self, bounds_df : pd.DataFrame) -> list:
        alarm_code_parts = []
        for idx, row in bounds_df.iterrows():
            parts = row['alarm_codes'].split('_')
            if self.two_part_tag:
                if len(parts) == 2:
                    alarm_code_parts.append([parts[1], "No ID"])
                elif len(parts) == 3:
                    alarm_code_parts.append([parts[1], parts[2]])
                else:
                    raise Exception(f"improper {self.alarm_tag} alarm code format for {row['variable_name']}")
            else:
                if len(parts) == 1:
                    alarm_code_parts.append(["default", "No ID"])
                elif len(parts) == 2:
                    alarm_code_parts.append(["default", parts[1]])
                else:
                    raise Exception(f"improper {self.alarm_tag} alarm code format for {row['variable_name']}")
        return alarm_code_parts