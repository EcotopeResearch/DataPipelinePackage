import pandas as pd
import numpy as np
import datetime as dt
from ecopipeline import ConfigManager

def flag_boundary_alarms(df: pd.DataFrame, config : ConfigManager, default_fault_time : int = 15, system: str = "", full_days : list = None) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least three columns which must be titled "variable_name", "low_alarm", and "high_alarm" which should contain the
        name of each variable in the dataframe that requires the alarming, the lower bound for acceptable data, and the upper bound for
        acceptable data respectively
    default_fault_time : int
        Number of consecutive minutes that a sensor must be out of bounds for to trigger an alarm. Can be customized for each variable with 
        the fault_time column in Varriable_Names.csv
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    full_days : list
        list of pd.Datetimes that should be considered full days here. If set to none, will take any day at all present in df

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    if (system != ""):
        if not 'system' in bounds_df.columns:
            raise Exception("system parameter is non null, however, system is not present in Variable_Names.csv")
        bounds_df = bounds_df.loc[bounds_df['system'] == system]

    required_columns = ["variable_name", "high_alarm", "low_alarm"]
    for required_column in required_columns:
        if not required_column in bounds_df.columns:
            raise Exception(f"{required_column} is not present in Variable_Names.csv")
    if not 'pretty_name' in bounds_df.columns:
        bounds_df['pretty_name'] = bounds_df['variable_name']
    if not 'fault_time' in bounds_df.columns:
        bounds_df['fault_time'] = default_fault_time

    idx = df.index
    if full_days is None:
        full_days = pd.to_datetime(pd.Series(idx).dt.normalize().unique())
    
    bounds_df = bounds_df.loc[:, ["variable_name", "high_alarm", "low_alarm", "fault_time", "pretty_name"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)
    # ensure that lower and upper bounds are numbers
    bounds_df['high_alarm'] = pd.to_numeric(bounds_df['high_alarm'], errors='coerce').astype(float)
    bounds_df['low_alarm'] = pd.to_numeric(bounds_df['low_alarm'], errors='coerce').astype(float)
    bounds_df['fault_time'] = pd.to_numeric(bounds_df['fault_time'], errors='coerce').astype('Int64')
    bounds_df = bounds_df[bounds_df.index.notnull()]
    alarms = {}
    for bound_var, bounds in bounds_df.iterrows():
        if bound_var in df.columns:
            lower_mask = df[bound_var] < bounds["low_alarm"]
            upper_mask = df[bound_var] > bounds["high_alarm"]
            if pd.isna(bounds['fault_time']):
                bounds['fault_time'] = default_fault_time
            for day in full_days:
                if bounds['fault_time'] < 1 :
                    print(f"Could not process alarm for {bound_var}. Fault time must be greater than or equal to 1 minute.")
                _check_and_add_alarm(df, lower_mask, alarms, day, bounds["fault_time"], bound_var, bounds['pretty_name'], 'Lower')
                _check_and_add_alarm(df, upper_mask, alarms, day, bounds["fault_time"], bound_var, bounds['pretty_name'], 'Upper')
    events = {
        'start_time_pt' : [],
        'end_time_pt' : [],
        'event_type' : [],
        'event_detail' : [],
        'variable_name' : []
    }
    for key, value_list in alarms.items():
        for value in value_list:
            events['start_time_pt'].append(key)
            events['end_time_pt'].append(key)
            events['event_type'].append('SILENT_ALARM')
            events['event_detail'].append(value[1])
            events['variable_name'].append(value[0])

    event_df = pd.DataFrame(events)
    event_df.set_index('start_time_pt', inplace=True)
    return event_df

def _check_and_add_alarm(df : pd.DataFrame, mask : pd.Series, alarms_dict, day, fault_time : int, var_name : str, pretty_name : str, alarm_type : str = 'Lower'):
    # KNOWN BUG : Avg value during fault time excludes the first (fault_time-1) minutes of each fault window
    next_day = day + pd.Timedelta(days=1)
    filtered_df = mask.loc[(mask.index >= day) & (mask.index < next_day)]
    consecutive_condition = filtered_df.rolling(window=fault_time).min() == 1
    if consecutive_condition.any():
        group = (consecutive_condition != consecutive_condition.shift()).cumsum()
        streaks = consecutive_condition.groupby(group).agg(['sum', 'size', 'idxmin'])
        true_streaks = streaks[consecutive_condition.groupby(group).first()]
        longest_streak_length = true_streaks['size'].max()
        avg_streak_length = true_streaks['size'].mean() + fault_time-1
        longest_group = true_streaks['size'].idxmax()
        streak_indices = consecutive_condition[group == longest_group].index
        starting_index = streak_indices[0]
        
        day_df = df.loc[(df.index >= day) & (df.index < next_day)]
        average_value = day_df.loc[consecutive_condition, var_name].mean()

        # first_true_index = consecutive_condition.idxmax()
        # because first (fault_time-1) minutes don't count in window
        adjusted_time = starting_index - pd.Timedelta(minutes=fault_time-1) 
        adjusted_longest_streak_length = longest_streak_length + fault_time-1
        alarm_string = f"{alarm_type} bound alarm for {pretty_name} (longest at {adjusted_time.strftime('%H:%M')} for {adjusted_longest_streak_length} minutes). Avg fault time : {round(avg_streak_length,1)} minutes, Avg value during fault: {round(average_value,2)}"
        if day in alarms_dict:
            alarms_dict[day].append([var_name, alarm_string])
        else:
            alarms_dict[day] = [[var_name, alarm_string]]

# def flag_dhw_outage(df: pd.DataFrame, daily_df : pd.DataFrame, dhw_outlet_column : str, supply_temp : int = 110, consecutive_minutes : int = 15) -> pd.DataFrame:
#     """
#      Parameters
#     ----------
#     df : pd.DataFrame
#         Single pandas dataframe of sensor data on minute intervals.
#     daily_df : pd.DataFrame
#         Single pandas dataframe of sensor data on daily intervals.
#     dhw_outlet_column : str
#         Name of the column in df and daily_df that contains temperature of DHW supplied to building occupants
#     supply_temp : int
#         the minimum DHW temperature acceptable to supply to building occupants
#     consecutive_minutes : int
#         the number of minutes in a row that DHW is not delivered to tenants to qualify as a DHW Outage

#     Returns
#     -------
#     event_df : pd.DataFrame
#         Dataframe with 'ALARM' events on the days in which there was a DHW Outage.
#     """
#     # TODO edge case for outage that spans over a day
#     events = {
#         'start_time_pt' : [],
#         'end_time_pt' : [],
#         'event_type' : [],
#         'event_detail' : [],
#     }
#     mask = df[dhw_outlet_column] < supply_temp
#     for day in daily_df.index:
#         next_day = day + pd.Timedelta(days=1)
#         filtered_df = mask.loc[(mask.index >= day) & (mask.index < next_day)]

#         consecutive_condition = filtered_df.rolling(window=consecutive_minutes).min() == 1
#         if consecutive_condition.any():
#             # first_true_index = consecutive_condition['supply_temp'].idxmax()
#             first_true_index = consecutive_condition.idxmax()
#             adjusted_time = first_true_index - pd.Timedelta(minutes=consecutive_minutes-1)
#             events['start_time_pt'].append(day)
#             events['end_time_pt'].append(next_day - pd.Timedelta(minutes=1))
#             events['event_type'].append("ALARM")
#             events['event_detail'].append(f"Hot Water Outage Occured (first one starting at {adjusted_time.strftime('%H:%M')})")
#     event_df = pd.DataFrame(events)
#     event_df.set_index('start_time_pt', inplace=True)
#     return event_df

# def generate_event_log_df(config : ConfigManager):
#     """
#     Creates an event log df based on user submitted events in an event log csv
#     Parameters
#     ----------
#     config : ecopipeline.ConfigManager
#         The ConfigManager object that holds configuration data for the pipeline.

#     Returns
#     -------
#     event_df : pd.DataFrame
#         Dataframe formatted from events in Event_log.csv for pipeline.
#     """
#     event_filename = config.get_event_log_path()
#     try:
#         event_df = pd.read_csv(event_filename)
#         event_df['start_time_pt'] = pd.to_datetime(event_df['start_time_pt'])
#         event_df['end_time_pt'] = pd.to_datetime(event_df['end_time_pt'])
#         event_df.set_index('start_time_pt', inplace=True)
#         return event_df
#     except Exception as e:
#         print(f"Error processing file {event_filename}: {e}")
#         return pd.DataFrame({
#             'start_time_pt' : [],
#             'end_time_pt' : [],
#             'event_type' : [],
#             'event_detail' : [],
#         })



# def create_data_statistics_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Function must be called on the raw minute data df after the rename_varriables() and before the ffill_missing() function has been called.
#     The function returns a dataframe indexed by day. Each column will expanded to 3 columns, appended with '_missing_mins', '_avg_gap', and
#     '_max_gap' respectively. the columns will carry the following statisctics:
#     _missing_mins -> the number of minutes in the day that have no reported data value for the column
#     _avg_gap -> the average gap (in minutes) between collected data values that day
#     _max_gap -> the maximum gap (in minutes) between collected data values that day

#     Parameters
#     ---------- 
#     df : pd.DataFrame
#         minute data df after the rename_varriables() and before the ffill_missing() function has been called

#     Returns
#     -------
#     daily_data_stats : pd.DataFrame
#         new dataframe with the columns descriped in the function's description
#     """
#     min_time = df.index.min()
#     start_day = min_time.floor('D')

#     # If min_time is not exactly at the start of the day, move to the next day
#     if min_time != start_day:
#         start_day = start_day + pd.tseries.offsets.Day(1)

#     # Build a complete minutely timestamp index over the full date range
#     full_index = pd.date_range(start=start_day,
#                                end=df.index.max().floor('D') - pd.Timedelta(minutes=1),
#                                freq='T')
    
#     # Reindex to include any completely missing minutes
#     df_full = df.reindex(full_index)

#     # Resample daily to count missing values per column
#     total_missing = df_full.isna().resample('D').sum().astype(int)

#     # Function to calculate max consecutive missing values
#     def max_consecutive_nans(x):
#         is_na = x.isna()
#         groups = (is_na != is_na.shift()).cumsum()
#         return is_na.groupby(groups).sum().max() or 0

#     # Function to calculate average consecutive missing values
#     def avg_consecutive_nans(x):
#         is_na = x.isna()
#         groups = (is_na != is_na.shift()).cumsum()
#         gap_lengths = is_na.groupby(groups).sum()
#         gap_lengths = gap_lengths[gap_lengths > 0]
#         if len(gap_lengths) == 0:
#             return 0
#         return gap_lengths.mean()

#     # Apply daily, per column
#     max_consec_missing = df_full.resample('D').apply(lambda day: day.apply(max_consecutive_nans))
#     avg_consec_missing = df_full.resample('D').apply(lambda day: day.apply(avg_consecutive_nans))

#     # Rename columns to include a suffix
#     total_missing = total_missing.add_suffix('_missing_mins')
#     max_consec_missing = max_consec_missing.add_suffix('_max_gap')
#     avg_consec_missing = avg_consec_missing.add_suffix('_avg_gap')

#     # Concatenate along columns (axis=1)
#     combined_df = pd.concat([total_missing, max_consec_missing, avg_consec_missing], axis=1)

#     return combined_df
