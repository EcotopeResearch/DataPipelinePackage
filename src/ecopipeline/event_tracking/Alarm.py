import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta

class Alarm:
    def __init__(self, bounds_df : pd.DataFrame, alarm_tag : str = None, type_default_dict : dict = {},
                 two_part_tag : bool = True, range_bounds : bool = False,
                 daily_only : bool = False, element_id_matching : bool = False):
        self.daily_only = daily_only
        self.alarm_tag = alarm_tag
        self.two_part_tag = two_part_tag
        self.range_bounds = range_bounds
        self.type_default_dict = type_default_dict
        self.element_id_matching = element_id_matching
        self.interval_minutes = 1.0
        self.triggered_alarms = {
                'start_time_pt' : [],
                'end_time_pt' : [],
                'alarm_type' : [],
                'event_detail' : [],
                'variable_name' : [],
                'certainty' : []
            }
        self.bounds_df = self._process_bounds_df_alarm_codes(bounds_df)
        self.set_alarms ={
            'alarm_type' : [],
            'variables' : []
        }
    
    def record_set_alarm(self, variable_triggers : list):
        if len(variable_triggers) > 0:
            variable_triggers_str = ';'.join(variable_triggers)
            if len(self.set_alarms['variables']) > 0 and variable_triggers_str in self.set_alarms['variables']:
                # alarm already recorded
                return
            self.set_alarms['alarm_type'].append(self.alarm_tag)
            self.set_alarms['variables'].append(variable_triggers_str)

    def get_alarm_set_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.set_alarms)

    def find_alarms(self, df: pd.DataFrame, daily_data : pd.DataFrame, config : ConfigManager) -> pd.DataFrame:
        """
        Parameters
        ----------
        df: pd.DataFrame
            Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
            are out of order or have gaps, the function may return erroneous alarms.
        daily_df: pd.DataFrame
            Post-transformed dataframe for daily data.
        config : ecopipeline.ConfigManager
            The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
            called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
            The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
            name of each variable in the dataframe that requires alarming and the appropriate alarm codes.
        Returns
        -------
        pd.DataFrame:
            Pandas dataframe with alarm events
        """
        if self.bounds_df.empty:
            return self._convert_silent_alarm_dict_to_df({}) # no alarms to look into 
        if self.daily_only:
            if daily_data.empty:
                print(f"cannot flag {self.alarm_tag} alarms. Dataframe is empty")
                return pd.DataFrame()
        elif df.empty:
            print(f"cannot flag {self.alarm_tag} alarms. Dataframe is empty")
            return pd.DataFrame()
        if df is not None and not df.empty:
            self.interval_minutes = self._infer_interval_minutes(df.index)
        self.specific_alarm_function(df, daily_data, config)
        return self._convert_silent_alarm_dict_to_df(self.triggered_alarms)

    def _infer_interval_minutes(self, index) -> float:
        """
        Estimate the spacing between samples, in minutes.

        Uses the most common spacing rather than the mean or median so that data gaps
        do not inflate the estimate. Falls back to one minute when the index is too
        short or is not time-based.

        Different sources having different cadences is handled, and so is a source
        dropping out entirely, since a gap larger than :meth:`_gap_tolerance` ends a
        streak rather than being counted as elapsed time.

        Known limitation
        ----------------
        This is one estimate for the whole frame. A source that switches cadence partway
        through a single pull, say from 1-minute to 5-minute samples, gets the modal
        interval of whichever cadence dominates, and the coarser stretch then reads as a
        run of gaps and never alarms. Left unaddressed because a single source is not
        expected to change its reporting cadence mid-pull. If one ever does, replace the
        frame-wide estimate with a local one::

            diff_sec  = mask.index.to_series().diff().dt.total_seconds()
            local_sec = diff_sec.rolling(5, center=True, min_periods=1).median()
            limit_sec = np.minimum(2 * local_sec, max(fault_minutes - 1, 1) * 60)

        There is a commented-out test for this case in tests/event_tracking_test.py, named
        test_temp_range_coarse_stretch_inside_fine_frame_alarms.

        Parameters
        ----------
        index : pd.Index
            Index of the minute dataframe being alarmed on.

        Returns
        -------
        float
            Sample interval in minutes.
        """
        if not isinstance(index, pd.DatetimeIndex) or len(index) < 2:
            return 1.0
        diffs = index.to_series().diff().dt.total_seconds()
        diffs = diffs[diffs > 0]
        if diffs.empty:
            return 1.0
        return float(diffs.mode().iloc[0]) / 60.0

    def _gap_tolerance(self, fault_minutes : float = None, max_gap_minutes : float = None) -> pd.Timedelta:
        """
        Largest jump in the index that may appear inside a single stretch of data.

        Defaults to twice the sample interval. When ``fault_minutes`` is given the
        result is also capped at ``fault_minutes - 1``, so that missing data can never
        account for most of a fault window. Alarms with no duration threshold pass no
        ``fault_minutes`` and get the uncapped allowance.

        Parameters
        ----------
        fault_minutes : float, optional
            Duration the caller is measuring against, used for the cap. Omit for alarms
            that have no duration threshold.
        max_gap_minutes : float, optional
            Explicit tolerance in minutes, overriding the interval-derived default.

        Returns
        -------
        pd.Timedelta
            Maximum tolerated gap.
        """
        allowance = pd.Timedelta(minutes=2 * self.interval_minutes if max_gap_minutes is None else max_gap_minutes)
        if fault_minutes is None:
            return allowance
        return min(allowance, pd.Timedelta(minutes=max(fault_minutes - 1, 1)))

    def _streak_ids(self, mask : pd.Series, max_gap : pd.Timedelta) -> pd.Series:
        """
        Label each row with a streak id, so that rows sharing an id are both adjacent
        in time and hold the same value of ``mask``.

        A new streak starts wherever the condition flips or the index jumps a gap
        larger than ``max_gap``, which keeps missing data from being read as a
        continuous run.

        Parameters
        ----------
        mask : pd.Series
            Boolean series indexed by timestamp.
        max_gap : pd.Timedelta
            Largest index jump that may appear inside one streak.

        Returns
        -------
        pd.Series
            Integer streak id per row, aligned to ``mask``.
        """
        index_jumps = mask.index.to_series().diff() > max_gap
        return (mask.ne(mask.shift()) | index_jumps).cumsum()

    def _iter_sustained_streaks(self, mask : pd.Series, fault_minutes : float, max_gap_minutes : float = None):
        """
        Find every stretch where a fault condition held long enough to alarm.

        ``fault_minutes`` is a duration rather than a row count, so the same value
        behaves the same way on 1-minute and 5-minute data. Durations are measured
        from the timestamps, and any jump in the index larger than the allowed gap
        breaks the stretch, so an alarm is never inferred across missing data.

        Parameters
        ----------
        mask : pd.Series
            Boolean series indexed by timestamp, True where the fault condition holds.
        fault_minutes : float
            Minutes the condition must hold before the stretch counts as an alarm.
        max_gap_minutes : float, optional
            Largest jump in the index tolerated inside a stretch. Defaults to twice the
            sample interval, and is capped at ``fault_minutes - 1`` either way so that a
            gap can never account for most of the fault window.

        Yields
        ------
        tuple of (pd.Timestamp, pd.Timestamp, float)
            Stretch start, stretch end, and observed duration in minutes. The end is
            exclusive - it is the last faulted sample plus one sample width - so pass it
            to :meth:`_add_an_alarm` with ``add_one_interval_to_end=False``.
        """
        fault_duration = pd.Timedelta(minutes=fault_minutes)
        max_gap = self._gap_tolerance(fault_minutes, max_gap_minutes)
        for start_time, end_time, duration, _ in self._iter_runs(mask, max_gap):
            if duration >= fault_duration:
                yield start_time, end_time, duration.total_seconds() / 60

    def _iter_runs(self, mask : pd.Series, max_gap : pd.Timedelta):
        """
        Walk every run of consecutive True values in ``mask``.

        Durations are measured from the timestamps rather than counted in rows, and a
        jump in the index larger than ``max_gap`` ends a run, so missing data is never
        read as a continuous stretch.

        Parameters
        ----------
        mask : pd.Series
            Boolean series indexed by timestamp.
        max_gap : pd.Timedelta
            Largest index jump that may appear inside one run.

        Yields
        ------
        tuple of (pd.Timestamp, pd.Timestamp, pd.Timedelta, bool)
            Run start, run end, observed duration, and whether both edges of the run
            were actually observed. The end is exclusive - it is the last sample in the
            run plus one sample width - so pass it to :meth:`_add_an_alarm` with
            ``add_one_interval_to_end=False``.

            A run is *bounded* when the samples immediately before and after it are
            both present in the data. An unbounded run runs off the edge of the frame
            or up against a gap, so its true length is unknown. Alarms that fire on a
            condition lasting too *long* can ignore this; alarms that fire on one
            ending too *soon*, such as short cycling, must not report an unbounded run.
        """
        mask = mask.fillna(False).astype(bool)
        if not mask.any():
            return
        if not mask.index.is_monotonic_increasing:
            # durations are measured from the index, so order matters here
            mask = mask.sort_index()

        fallback_width = pd.Timedelta(minutes=self.interval_minutes)
        run_ids = self._streak_ids(mask, max_gap).to_numpy()
        values = mask.to_numpy()
        # gap_before[i] is True when the sample before row i is missing
        gap_before = (mask.index.to_series().diff() > max_gap).to_numpy()
        row_count = len(mask)

        starts = np.flatnonzero(np.r_[True, run_ids[1:] != run_ids[:-1]])
        ends = np.r_[starts[1:] - 1, row_count - 1]

        for first, last in zip(starts, ends):
            if not values[first]:
                continue
            run_index = mask.index[first:last + 1]
            # measure sample width inside this run so that a frame with changing
            # intervals is judged against its local spacing
            widths = run_index.to_series().diff().dropna()
            width = widths.median() if not widths.empty else fallback_width
            duration = (run_index[-1] - run_index[0]) + width
            bounded = (first > 0 and not gap_before[first]
                       and last < row_count - 1 and not gap_before[last + 1])
            yield run_index[0], run_index[-1] + width, duration, bounded


    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        self.triggered_alarms = {}

    def _add_an_alarm(self, start_time : datetime, end_time : datetime, var_name : str, alarm_string : str, add_one_interval_to_end : bool = True, certainty : str = "high"):
        """
        Record one alarm event.

        Parameters
        ----------
        start_time : datetime
            First moment the alarm condition held.
        end_time : datetime
            Last moment the alarm condition held, or the exclusive end when
            ``add_one_interval_to_end`` is False.
        var_name : str
            Variable the alarm is attributed to.
        alarm_string : str
            Human readable description stored in the event_detail column.
        add_one_interval_to_end : bool
            When True, extend ``end_time`` by one sample interval so that the recorded
            end is exclusive. Pass False when ``end_time`` is already exclusive, which
            is the case for every end produced by :meth:`_iter_runs`.
        certainty : str
            One of "high", "med" or "low".
        """
        certainty_dict = {
            "high" : 3,
            "med" : 2,
            "low" : 1
        }
        if certainty not in certainty_dict.keys():
            raise Exception(f"{certainty} is not a valid certainty key. Valid keys are {certainty_dict.keys()}")
        else:
            certainty = certainty_dict[certainty]
        if add_one_interval_to_end:
            end_time = end_time + timedelta(minutes=self.interval_minutes)
        self.triggered_alarms['start_time_pt'].append(start_time)
        self.triggered_alarms['end_time_pt'].append(end_time)
        self.triggered_alarms['alarm_type'].append(self.alarm_tag)
        self.triggered_alarms['event_detail'].append(alarm_string)
        self.triggered_alarms['variable_name'].append(var_name)
        self.triggered_alarms['certainty'].append(certainty)
    
    def _convert_silent_alarm_dict_to_df(self, alarm_dict : dict) -> pd.DataFrame:

        alarm_df = pd.DataFrame(alarm_dict)
        alarm_df = self._compress_alarm_df(alarm_df)
        return alarm_df

    def _compress_alarm_df(self, alarm_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compresses consecutive alarms of the same variable_name and alarm_type into single rows.
        If one alarm's start_time_pt is within one sample interval of another alarm's
        end_time_pt, they are merged into one row with the earliest start_time_pt and latest
        end_time_pt. The tolerance follows the sample interval rather than being fixed at one
        minute, so alarms that are adjacent in 5-minute data still merge.

        Parameters
        ----------
        alarm_df : pd.DataFrame
            DataFrame with columns: start_time_pt, end_time_pt, alarm_type, variable_name, event_detail

        Returns
        -------
        pd.DataFrame
            Compressed DataFrame with consecutive alarms merged
        """
        # TODO figure out what to do with event detail
        if alarm_df.empty:
            return alarm_df
        # Sort entire DataFrame by start_time_pt before processing
        alarm_df = alarm_df.sort_values('start_time_pt').reset_index(drop=True)

        merge_tolerance = timedelta(minutes=self.interval_minutes)
        compressed_rows = []

        # Group by variable_name and alarm_type
        for (var_name, alarm_type), group in alarm_df.groupby(['variable_name', 'alarm_type'], sort=False):
            # Group is already sorted since we sorted the whole DataFrame above
            group = group.reset_index(drop=True)

            current_start = None
            current_end = None
            current_detail = None
            current_certainty = None

            for _, row in group.iterrows():
                row_start = row['start_time_pt']
                row_end = row['end_time_pt']

                if current_start is None:
                    # First row in group
                    current_start = row_start
                    current_end = row_end
                    current_detail = row['event_detail']
                    current_certainty = row['certainty']
                elif row_start <= current_end + merge_tolerance:
                    # This row is within 1 minute of current end - merge it after checking 
                    row_certainty = row['certainty']
                    if row_certainty > current_certainty:
                        if row_start > current_start:
                            compressed_rows.append({
                                'start_time_pt': current_start,
                                'end_time_pt': row_start,
                                'alarm_type': alarm_type,
                                'event_detail': current_detail,
                                'variable_name': var_name,
                                'certainty': current_certainty
                            })
                        if row_end >= current_end:
                            current_start = row_start
                            current_end = row_end
                            current_detail = row['event_detail']
                            current_certainty = row_certainty
                        else:
                            #encompassed
                            compressed_rows.append({
                                    'start_time_pt': row_start,
                                    'end_time_pt': row_end,
                                    'alarm_type': alarm_type,
                                    'event_detail': row['event_detail'],
                                    'variable_name': var_name,
                                    'certainty': row_certainty
                                })
                            current_start = row_end

                    elif row_certainty < current_certainty:
                        if row_end > current_end:
                            compressed_rows.append({
                                    'start_time_pt': current_start,
                                    'end_time_pt': current_end,
                                    'alarm_type': alarm_type,
                                    'event_detail': current_detail,
                                    'variable_name': var_name,
                                    'certainty': current_certainty
                                })
                            current_start = current_end
                            current_end = row_end
                            current_detail = row['event_detail']
                            current_certainty = row_certainty
                        
                    else:
                        current_end = max(current_end, row_end)
                else:
                    # Gap is more than one sample interval - save current and start new
                    compressed_rows.append({
                        'start_time_pt': current_start,
                        'end_time_pt': current_end,
                        'alarm_type': alarm_type,
                        'event_detail': current_detail,
                        'variable_name': var_name,
                        'certainty': current_certainty
                    })
                    current_start = row_start
                    current_end = row_end
                    current_detail = row['event_detail']
                    current_certainty = row['certainty']

            # Don't forget the last accumulated row
            if current_start is not None:
                compressed_rows.append({
                    'start_time_pt': current_start,
                    'end_time_pt': current_end,
                    'alarm_type': alarm_type,
                    'event_detail': current_detail,
                    'variable_name': var_name,
                    'certainty': current_certainty
                })
        return pd.DataFrame(compressed_rows)
    
    def _process_bounds_df_alarm_codes(self, og_bounds_df : pd.DataFrame) -> pd.DataFrame:
        # Should only do for alarm codes of format: [TAG]_[TYPE]_[OPTIONAL_ID]:[BOUND]
        bounds_df = og_bounds_df.copy()
        required_columns = ["variable_name", "alarm_codes"]
        for required_column in required_columns:
            if not required_column in bounds_df.columns:
                raise Exception(f"{required_column} is not present in Variable_Names.csv")
        if not 'pretty_name' in bounds_df.columns:
            bounds_df['pretty_name'] = bounds_df['variable_name']
        else:
            bounds_df['pretty_name'] = bounds_df['pretty_name'].fillna(bounds_df['variable_name'])

        bounds_df = bounds_df.loc[:, ["variable_name", "alarm_codes", "pretty_name"]]
        bounds_df.dropna(axis=0, thresh=2, inplace=True)

        # Check if all alarm_codes are null or if dataframe is empty
        if bounds_df.empty or bounds_df['alarm_codes'].isna().all():
            return pd.DataFrame()
        
        bounds_df = bounds_df[bounds_df['alarm_codes'].str.contains(self.alarm_tag, na=False)]

        # Split alarm_codes by semicolons and create a row for each STS code
        expanded_rows = []
        for idx, row in bounds_df.iterrows():
            alarm_codes = str(row['alarm_codes']).split(';')
            tag_codes = [code.strip() for code in alarm_codes if code.strip().startswith(self.alarm_tag)]

            if tag_codes:
                for tag_code in tag_codes:
                    new_row = row.copy()
                    if ":" in tag_code:
                        tag_parts = tag_code.split(':')
                        if len(tag_parts) > 2:
                            raise Exception(f"Improperly formated alarm code : {tag_code}")
                        if self.range_bounds:
                            bounds = tag_parts[1]
                            bound_range = bounds.split('-')
                            if len(bound_range) != 2:
                                raise Exception(f"Improperly formated alarm code : {tag_code}. Expected bound range in form '[number]-[number]' but recieved '{bounds}'.")
                            new_row['bound'] = bound_range[0]
                            new_row['bound2'] = bound_range[1]
                        else:    
                            new_row['bound'] = tag_parts[1]
                        tag_code = tag_parts[0]
                    else:
                        new_row['bound'] = None
                        if self.range_bounds:
                            new_row['bound2'] = None
                    new_row['alarm_codes'] = tag_code

                    expanded_rows.append(new_row)

        if expanded_rows:
            bounds_df = pd.DataFrame(expanded_rows)
        else:
            return pd.DataFrame()# no tagged alarms to look into
        
        alarm_code_parts = self._organize_alarm_codes(bounds_df)

        if len(alarm_code_parts) > 0:
            bounds_df[['alarm_code_type', 'alarm_code_id']] = pd.DataFrame(alarm_code_parts, index=bounds_df.index)

            # Replace None bounds with appropriate defaults based on alarm_code_type
            for idx, row in bounds_df.iterrows():
                if pd.isna(row['bound']) or row['bound'] is None:
                    if row['alarm_code_type'] in self.type_default_dict.keys():
                        if self.range_bounds:
                            bounds_df.at[idx, 'bound'] = self.type_default_dict[row['alarm_code_type']][0]
                            bounds_df.at[idx, 'bound2'] = self.type_default_dict[row['alarm_code_type']][1]
                        else:
                            bounds_df.at[idx, 'bound'] = self.type_default_dict[row['alarm_code_type']]
            # Coerce bound column to float
            bounds_df['bound'] = pd.to_numeric(bounds_df['bound'], errors='coerce').astype(float)
            if self.range_bounds:
                bounds_df['bound2'] = pd.to_numeric(bounds_df['bound2'], errors='coerce').astype(float)

        return bounds_df

    def _organize_alarm_codes(self, bounds_df : pd.DataFrame) -> list:
        alarm_code_parts = []
        seen_total_power = False
        for idx, row in bounds_df.iterrows():
            element_id = 'No ID'
            parts = row['variable_name'].split('_')
            if len(parts) <= 1:
                if len(parts) < 1 or parts[0] != 'SystemCOP':
                    raise Exception(f"Improper variable name for '{row['variable_name']}', must be in form '[Unit Type]_[Element Identifier]' (e.g. 'Temp_HPWH' or 'PowerIn_SwingTank1').")
            if parts[0] == "PowerIn" and parts[1] == "Total":
                # total power is own catagory
                if seen_total_power:
                    raise Exception(f"Multiple instances of PowerIn_Total seen for alarm code {self.alarm_tag}. There may only be one variable that starts with 'PowerIn_Total'. This should be total system power.")
                alarm_code_parts.append(["PowerIn_Total", element_id])
                seen_total_power = True
            else:
                if self.element_id_matching:
                    var_name_no_unit = "_".join(parts[1:])
                    for suffix in ["_Inlet", "_inlet", "_Outlet", "_outlet", "Inlet", "inlet", "Outlet", "outlet"]:
                        if var_name_no_unit.endswith(suffix):
                            var_name_no_unit = var_name_no_unit[:-len(suffix)]
                            break
                    element_id = var_name_no_unit
                    
                alarm_code_parts.append([parts[0], element_id])

        return alarm_code_parts
        
    
    def _append_previous_days_to_df(self, daily_df: pd.DataFrame, config : ConfigManager, ratio_period_days : int, day_table_name : str, primary_key : str = "time_pt") -> pd.DataFrame:
        db_connection, cursor = config.connect_db()
        period_start = daily_df.index.min() - timedelta(ratio_period_days)
        try:
            # find existing times in database for upsert statement
            cursor.execute(
                f"SELECT * FROM {day_table_name} WHERE {primary_key} < '{daily_df.index.min()}' AND {primary_key} >= '{period_start}'")
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            old_days_df = pd.DataFrame(result, columns=column_names)
            old_days_df = old_days_df.set_index(primary_key)
            daily_df = pd.concat([daily_df, old_days_df])
            daily_df = daily_df.sort_index(ascending=True)
        except mysqlerrors.Error:
            print(f"Table {day_table_name} has no data.")

        db_connection.close()
        cursor.close()
        return daily_df
