import pandas as pd
from ecopipeline import ConfigManager
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class WaterLeak(Alarm):
    """
    Detects possible water leaks by flagging days with unusually high total flow where flow never
    stops during the day.

    A day is flagged when its daily flow is more than std_multiplier standard deviations above the
    site's mean daily flow and none of that day's minute flow values are at or below the bound.
    Mean and standard deviation are read from the SiteConfig stat_cache table (stat_ids
    mean_daily_flow and std_daily_flow) when both entries are fresh. Otherwise they are calculated
    from the non-null values of the variable in the site's daily table, excluding days covered by
    this site's site_events with summary_filtered set (and all days before an MV_COMMISSIONED event).

    Variable_Names.csv configuration:
      alarm_codes column: WTRLEAK:### where ### is the flow at or below which flow is considered
        stopped (default 0.0).
      variable_name column: Must start with Flow_ (e.g., Flow_CityWater). Only one variable may
        carry the WTRLEAK alarm code.

    Parameters
    ----------
    stat_max_age_hours : float
        Maximum age in hours of cached stat_cache values before they are recalculated from the
        daily table (default 72).
    min_days : int
        Minimum number of non-null daily values needed in the daily table to calculate mean and
        standard deviation (default 30).
    std_multiplier : float
        Number of standard deviations above the mean daily flow a day must exceed to be checked
        for a leak (default 3).
    """
    def __init__(self, bounds_df : pd.DataFrame, stat_max_age_hours : float = 72, min_days : int = 30, std_multiplier : float = 2.5):
        alarm_tag = 'WTRLEAK'
        type_default_dict = {"Flow" : 0.0}
        self.stat_max_age_hours = stat_max_age_hours
        self.min_days = min_days
        self.std_multiplier = std_multiplier
        super().__init__(bounds_df, alarm_tag, type_default_dict, two_part_tag = False)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        if daily_df is None or daily_df.empty:
            print("daily_df is empty, cannot process WTRLEAK alarm")
            return
        if len(self.bounds_df) != 1:
            print(f"WTRLEAK alarm requires exactly one variable with a WTRLEAK alarm code, but found {len(self.bounds_df)}. Cannot process WTRLEAK alarm")
            return
        row = self.bounds_df.iloc[0]
        var_name = row['variable_name']
        if row['alarm_code_type'] != "Flow":
            print(f"WTRLEAK alarm requires a Flow_ variable, but {var_name} is not one. Cannot process WTRLEAK alarm")
            return
        self.record_set_alarm([var_name])
        if var_name not in daily_df.columns:
            print(f"{var_name} is not in daily_df, cannot process WTRLEAK alarm")
            return

        flow_stats = self._get_flow_stats(var_name, config)
        if flow_stats is None:
            return
        mean_daily_flow, std_daily_flow = flow_stats
        threshold = mean_daily_flow + (self.std_multiplier * std_daily_flow)
        bound = row['bound']

        daily_flow = daily_df[var_name].copy().dropna()
        minute_flow = df[var_name].copy().dropna() if var_name in df.columns else pd.Series(dtype=float)
        for day, day_value in daily_flow.items():
            if day_value > threshold:
                day_start = pd.Timestamp(day).normalize()
                day_end = day_start + timedelta(1)
                day_minutes = minute_flow[(minute_flow.index >= day_start) & (minute_flow.index < day_end)]
                if day_minutes.empty:
                    self._add_an_alarm(day_start, day_end, var_name,
                        f"Possible Water Leak: {var_name} daily flow of {day_value:.2f} was more than {self.std_multiplier} standard deviations above the mean daily flow of {mean_daily_flow:.2f}. No minute data was available to confirm flow never stopped.",
                        add_one_interval_to_end=False, certainty="low")
                elif not (day_minutes <= bound).any():
                    self._add_an_alarm(day_start, day_end, var_name,
                        f"Possible Water Leak: {var_name} daily flow of {day_value:.2f} was more than {self.std_multiplier} standard deviations above the mean daily flow of {mean_daily_flow:.2f} and flow never dropped to {bound} or below.",
                        add_one_interval_to_end=False, certainty="med")

    def _get_flow_stats(self, var_name : str, config : ConfigManager):
        """
        Return (mean_daily_flow, std_daily_flow), or None if they cannot be found or calculated.
        Uses fresh stat_cache entries when available, otherwise calculates them from the daily table.
        """
        cached_stats = self._get_cached_flow_stats(config)
        if cached_stats is not None:
            return cached_stats

        db_connection, cursor = config.connect_db(quiet_connection=True)
        if db_connection is None:
            print("Unable to connect to database, cannot process WTRLEAK alarm")
            return None
        try:
            cursor.execute(f"SELECT time_pt, `{var_name}` FROM `{config.get_table_name('day')}`")
            results = cursor.fetchall()
            history = pd.Series([result_row[1] for result_row in results],
                                index=pd.to_datetime([result_row[0] for result_row in results]), dtype=float)
        except mysqlerrors.ProgrammingError:
            # daily table or var_name column does not exist
            history = pd.Series(dtype=float, index=pd.DatetimeIndex([]))
        finally:
            cursor.close()
            db_connection.close()

        # ---------------- BEGIN site_events filtering (comment out this block to disable) ----------------
        events = config.get_summary_filtered_events()
        if events is None:
            print("Warning: unable to retrieve site_events. Calculating WTRLEAK stats from unfiltered daily history.")
        else:
            history = self._filter_event_days(history, events)
        # ---------------- END site_events filtering ----------------

        history = history.dropna()
        if len(history) < self.min_days:
            print("daily table does not have sufficient data, cannot process WTRLEAK alarm")
            return None
        return history.mean(), history.std()

    def _filter_event_days(self, history : pd.Series, events : pd.DataFrame) -> pd.Series:
        """
        Return a copy of history without the days covered by the given site events.

        A day is removed if any part of it overlaps an event. Events with no end_time_pt are treated
        as still running. For MV_COMMISSIONED events, every day before the event's end_time_pt
        (or start_time_pt if it has no end) is removed.
        """
        day_starts = history.index.normalize()
        day_ends = day_starts + timedelta(1)
        keep = pd.Series(True, index=history.index)
        for _, event in events.iterrows():
            if event['event_type'] == "MV_COMMISSIONED":
                cutoff = event['end_time_pt'] if pd.notna(event['end_time_pt']) else event['start_time_pt']
                keep &= ~(day_starts < cutoff)
            else:
                overlaps = day_ends > event['start_time_pt']
                if pd.notna(event['end_time_pt']):
                    overlaps &= day_starts <= event['end_time_pt']
                keep &= ~overlaps
        return history[keep].copy()

    def _get_cached_flow_stats(self, config : ConfigManager):
        """
        Return (mean_daily_flow, std_daily_flow) from the SiteConfig stat_cache table if both entries
        exist and are newer than stat_max_age_hours, otherwise None.
        """
        mean_daily_flow = config.get_cached_stat("mean_daily_flow", self.stat_max_age_hours)
        if mean_daily_flow is None:
            return None
        std_daily_flow = config.get_cached_stat("std_daily_flow", self.stat_max_age_hours)
        if std_daily_flow is None:
            return None
        return mean_daily_flow, std_daily_flow
