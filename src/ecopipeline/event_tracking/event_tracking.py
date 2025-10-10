import pandas as pd
import numpy as np
import datetime as dt
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta

def central_alarm_df_creator(df: pd.DataFrame, daily_data : pd.DataFrame, config : ConfigManager, system: str = "", 
                             default_cop_high_bound : float = 4.5, default_cop_low_bound : float = 0,
                             default_boundary_fault_time : int = 15, site_name : str = None, day_table_name_header : str = "day",
                             power_ratio_period_days : int = 7) -> pd.DataFrame:
    day_list = daily_data.index.to_list()
    print('Checking for alarms...')
    alarm_df = _convert_silent_alarm_dict_to_df({})
    boundary_alarm_df = flag_boundary_alarms(df, config, full_days=day_list, system=system, default_fault_time= default_boundary_fault_time)
    pwr_alarm_df = power_ratio_alarm(daily_data, config, day_table_name = config.get_table_name(day_table_name_header), system=system, ratio_period_days=power_ratio_period_days)
    abnormal_COP_df = flag_abnormal_COP(daily_data, config, system = system, default_high_bound=default_cop_high_bound, default_low_bound=default_cop_low_bound)

    if len(boundary_alarm_df) > 0:
        print("Boundary alarms detected. Adding them to event df...")
        alarm_df = boundary_alarm_df
    else:
        print("No boundary alarms detected.")

    if len(pwr_alarm_df) > 0:
        print("Power alarms detected. Adding them to event df...")
        alarm_df = pd.concat([alarm_df, pwr_alarm_df])
    else:
        print("No power alarms detected.")
    
    if _check_if_during_ongoing_cop_alarm(daily_data, config, site_name):
        print("Ongoing DATA_LOSS_COP detected. No further DATA_LOSS_COP events will be uploaded")
    elif len(abnormal_COP_df) > 0:
        print("Abnormal COPs detected. Adding them to event df...")
        alarm_df = pd.concat([alarm_df, abnormal_COP_df])
    else:
        print("No abnormal COPs.")

    return alarm_df

def flag_abnormal_COP(daily_data: pd.DataFrame, config : ConfigManager, system: str = "", default_high_bound : float = 4.5, default_low_bound : float = 0) -> pd.DataFrame:
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
    if not "variable_name" in bounds_df.columns:
        raise Exception(f"variable_name is not present in Variable_Names.csv")
    if not 'pretty_name' in bounds_df.columns:
        bounds_df['pretty_name'] = bounds_df['variable_name']
    else:
        bounds_df['pretty_name'] = bounds_df['pretty_name'].fillna(bounds_df['variable_name'])
    if not 'high_alarm' in bounds_df.columns:
        bounds_df['high_alarm'] = default_high_bound
    else:
        bounds_df['high_alarm'] = bounds_df['high_alarm'].fillna(default_high_bound)
    if not 'low_alarm' in bounds_df.columns:
        bounds_df['low_alarm'] = default_low_bound
    else:
        bounds_df['low_alarm'] = bounds_df['low_alarm'].fillna(default_low_bound)

    bounds_df = bounds_df.loc[:, ["variable_name", "high_alarm", "low_alarm", "pretty_name"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)

    cop_pattern = re.compile(r'^(COP\w*|SystemCOP\w*)$')
    cop_columns = [col for col in daily_data.columns if re.match(cop_pattern, col)]

    alarms_dict = {}
    if not daily_data.empty and len(cop_columns) > 0:
        for bound_var, bounds in bounds_df.iterrows():
            if bound_var in cop_columns:
                for day, day_values in daily_data.iterrows():
                    if not day_values[bound_var] is None and (day_values[bound_var] > bounds['high_alarm'] or day_values[bound_var] < bounds['low_alarm']):
                        alarm_str = f"Unexpected COP Value detected: {bounds['pretty_name']} = {round(day_values[bound_var],2)}"
                        if day in alarms_dict:
                            alarms_dict[day].append([bound_var, alarm_str])
                        else:
                            alarms_dict[day] = [[bound_var, alarm_str]]
    return _convert_event_type_dict_to_df(alarms_dict, event_type="SILENT_ALARM")

def _check_if_during_ongoing_cop_alarm(daily_df : pd.DataFrame, config : ConfigManager, site_name : str = None) -> bool:
    if site_name is None:
        site_name = config.get_site_name()
    connection, cursor = config.connect_db()
    on_going_cop = False
    try:
        # find existing times in database for upsert statement
        cursor.execute(
            f"SELECT id FROM site_events WHERE start_time_pt <= '{daily_df.index.min()}' AND (end_time_pt IS NULL OR end_time_pt >= '{daily_df.index.max()}') AND site_name = '{site_name}' AND event_type = 'DATA_LOSS_COP'")
        # Fetch the results into a DataFrame
        existing_rows = pd.DataFrame(cursor.fetchall(), columns=['id'])
        if not existing_rows.empty:
            on_going_cop = True

    except mysqlerrors.Error as e:
        print(f"Retrieving data from site_events caused exception: {e}")
    connection.close()
    cursor.close()
    return on_going_cop

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
    if df.empty:
        print("cannot flag boundary alarms. Dataframe is empty")
        return pd.DataFrame()
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
    else:
        bounds_df['pretty_name'] = bounds_df['pretty_name'].fillna(bounds_df['variable_name'])
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

    return _convert_silent_alarm_dict_to_df(alarms)

def _convert_silent_alarm_dict_to_df(alarm_dict : dict) -> pd.DataFrame:
    events = {
        'start_time_pt' : [],
        'end_time_pt' : [],
        'event_type' : [],
        'event_detail' : [],
        'variable_name' : []
    }
    for key, value_list in alarm_dict.items():
        for value in value_list:
            events['start_time_pt'].append(key)
            events['end_time_pt'].append(key)
            events['event_type'].append('SILENT_ALARM')
            events['event_detail'].append(value[1])
            events['variable_name'].append(value[0])

    event_df = pd.DataFrame(events)
    event_df.set_index('start_time_pt', inplace=True)
    return event_df

def _convert_event_type_dict_to_df(alarm_dict : dict, event_type = 'DATA_LOSS_COP') -> pd.DataFrame:
    events = {
        'start_time_pt' : [],
        'end_time_pt' : [],
        'event_type' : [],
        'event_detail' : [],
        'variable_name' : []
    }
    for key, value in alarm_dict.items():
        for i in range(len(value)):
            events['start_time_pt'].append(key)
            events['end_time_pt'].append(key)
            events['event_type'].append(event_type)
            events['event_detail'].append(value[i][1])
            events['variable_name'].append(value[i][0])

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

def power_ratio_alarm(daily_df: pd.DataFrame, config : ConfigManager, day_table_name : str, system: str = "", verbose : bool = False, ratio_period_days : int = 7) -> pd.DataFrame:
    """
    Function will take a pandas dataframe of daily data and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    Parameters
    ----------
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data. It should be noted that this function expects consecutive, in order days. If days
        are out of order or have gaps, the function may return erroneous alarms.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name", "alarm_codes" which should contain the
        name of each variable in the dataframe that requires the alarming and the ratio alarm code in the form "PR_{Power Ratio Name}:{low percentage}-{high percentage}
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    verbose : bool
        add print statements in power ratio

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events, empty if no alarms triggered
    """
    daily_df_copy = daily_df.copy()
    variable_names_path = config.get_var_names_path()
    try:
        ratios_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    if (system != ""):
        if not 'system' in ratios_df.columns:
            raise Exception("system parameter is non null, however, system is not present in Variable_Names.csv")
        ratios_df = ratios_df.loc[ratios_df['system'] == system]
    required_columns = ["variable_name", "alarm_codes"]
    for required_column in required_columns:
        if not required_column in ratios_df.columns:
            raise Exception(f"{required_column} is not present in Variable_Names.csv")
    if ratios_df['alarm_codes'].isna().all() or ratios_df['alarm_codes'].isnull().all():
        print("No alarm codes in ", variable_names_path)
        return pd.DataFrame()
    if not 'pretty_name' in ratios_df.columns:
        ratios_df['pretty_name'] = ratios_df['variable_name']
    else:
        ratios_df['pretty_name'] = ratios_df['pretty_name'].fillna(ratios_df['variable_name'])
    ratios_df = ratios_df.loc[:, ["variable_name", "alarm_codes", "pretty_name"]]
    ratios_df = ratios_df[ratios_df['alarm_codes'].str.contains('PR', na=False)]
    ratios_df.dropna(axis=0, thresh=2, inplace=True)
    if ratio_period_days > 1:
        if verbose:
            print(f"adding last {ratio_period_days} to daily_df")
        daily_df_copy = _append_previous_days_to_df(daily_df_copy, config, ratio_period_days, day_table_name)
    elif ratio_period_days < 1:
        print("power ratio alarm period, ratio_period_days, must be more than 1")
        return pd.DataFrame()

    ratios_df.set_index(['variable_name'], inplace=True)
    ratio_dict = {}
    for ratios_var, ratios in ratios_df.iterrows():
        if not ratios_var in daily_df_copy.columns:
                daily_df_copy[ratios_var] = 0
        alarm_codes = str(ratios['alarm_codes']).split(";")
        for alarm_code in alarm_codes:
            if alarm_code[:2] == "PR":
                split_out_alarm = alarm_code.split(":")
                low_high = split_out_alarm[1].split("-")
                pr_id = split_out_alarm[0].split("_")[1]
                if len(low_high) != 2:
                    raise Exception(f"Error processing alarm code {alarm_code}")
                if pr_id in ratio_dict:
                    ratio_dict[pr_id][0].append(ratios_var)
                    ratio_dict[pr_id][1].append(float(low_high[0]))
                    ratio_dict[pr_id][2].append(float(low_high[1]))
                    ratio_dict[pr_id][3].append(ratios['pretty_name'])
                else:
                    ratio_dict[pr_id] = [[ratios_var],[float(low_high[0])],[float(low_high[1])],[ratios['pretty_name']]]
    if verbose:
        print("ratio_dict keys:", ratio_dict.keys())
    # Create blocks of ratio_period_days
    blocks_df = _create_period_blocks(daily_df_copy, ratio_period_days, verbose)

    if blocks_df.empty:
        print("No complete blocks available for analysis")
        return pd.DataFrame()
    
    alarms = {}
    for key, value_list in ratio_dict.items():
        # Calculate total for each block
        blocks_df[key] = blocks_df[value_list[0]].sum(axis=1)
        for i in range(len(value_list[0])):
            column_name = value_list[0][i]
            # Calculate ratio for each block
            blocks_df[f'{column_name}_{key}'] = (blocks_df[column_name]/blocks_df[key]) * 100
            if verbose:
                print(f"Block ratios for {column_name}_{key}:", blocks_df[f'{column_name}_{key}'])
            _check_and_add_ratio_alarm_blocks(blocks_df, key, column_name, value_list[3][i], alarms, value_list[2][i], value_list[1][i], ratio_period_days)
    return _convert_silent_alarm_dict_to_df(alarms) 
    # alarms = {}
    # for key, value_list in ratio_dict.items():
    #     daily_df_copy[key] = daily_df_copy[value_list[0]].sum(axis=1)
    #     for i in range(len(value_list[0])):
    #         column_name = value_list[0][i]
    #         daily_df_copy[f'{column_name}_{key}'] = (daily_df_copy[column_name]/daily_df_copy[key]) * 100
    #         if verbose:
    #             print(f"Ratios for {column_name}_{key}",daily_df_copy[f'{column_name}_{key}'])
    #         _check_and_add_ratio_alarm(daily_df_copy, key, column_name, value_list[3][i], alarms, value_list[2][i], value_list[1][i])
    # return _convert_silent_alarm_dict_to_df(alarms)      

# def _check_and_add_ratio_alarm(daily_df: pd.DataFrame, alarm_key : str, column_name : str, pretty_name : str, alarms_dict : dict, high_bound : float, low_bound : float):
#     alarm_daily_df = daily_df.loc[(daily_df[f"{column_name}_{alarm_key}"] < low_bound) | (daily_df[f"{column_name}_{alarm_key}"] > high_bound)]
#     if not alarm_daily_df.empty:
#         for day, values in alarm_daily_df.iterrows():
#             alarm_str = f"Power ratio alarm: {pretty_name} accounted for {round(values[f'{column_name}_{alarm_key}'], 2)}% of {alarm_key} energy use. {round(low_bound, 2)}-{round(high_bound, 2)}% of {alarm_key} energy use expected."
#             if day in alarms_dict:
#                 alarms_dict[day].append([column_name, alarm_str])
#             else:
#                 alarms_dict[day] = [[column_name, alarm_str]]
def _check_and_add_ratio_alarm_blocks(blocks_df: pd.DataFrame, alarm_key: str, column_name: str, pretty_name: str, alarms_dict: dict, high_bound: float, low_bound: float, ratio_period_days: int):
    """
    Check for alarms in block-based ratios and add to alarms dictionary.
    """
    alarm_blocks_df = blocks_df.loc[(blocks_df[f"{column_name}_{alarm_key}"] < low_bound) | (blocks_df[f"{column_name}_{alarm_key}"] > high_bound)]
    if not alarm_blocks_df.empty:
        for block_end_date, values in alarm_blocks_df.iterrows():
            alarm_str = f"Power ratio alarm ({ratio_period_days}-day block ending {block_end_date.strftime('%Y-%m-%d')}): {pretty_name} accounted for {round(values[f'{column_name}_{alarm_key}'], 2)}% of {alarm_key} energy use. {round(low_bound, 2)}-{round(high_bound, 2)}% of {alarm_key} energy use expected."
            if block_end_date in alarms_dict:
                alarms_dict[block_end_date].append([column_name, alarm_str])
            else:
                alarms_dict[block_end_date] = [[column_name, alarm_str]]

def _create_period_blocks(daily_df: pd.DataFrame, ratio_period_days: int, verbose: bool = False) -> pd.DataFrame:
    """
    Create blocks of ratio_period_days by summing values within each block.
    Each block will be represented by its end date.
    """
    if len(daily_df) < ratio_period_days:
        if verbose:
            print(f"Not enough data for {ratio_period_days}-day blocks. Need at least {ratio_period_days} days, have {len(daily_df)}")
        return pd.DataFrame()
    
    blocks = []
    block_dates = []
    
    # Create blocks by summing consecutive groups of ratio_period_days
    for i in range(ratio_period_days - 1, len(daily_df)):
        start_idx = i - ratio_period_days + 1
        end_idx = i + 1
        
        block_data = daily_df.iloc[start_idx:end_idx].sum()
        blocks.append(block_data)
        # Use the end date of the block as the identifier
        block_dates.append(daily_df.index[i])
    
    if not blocks:
        return pd.DataFrame()
    
    blocks_df = pd.DataFrame(blocks, index=block_dates)
    
    if verbose:
        print(f"Created {len(blocks_df)} blocks of {ratio_period_days} days each")
        print(f"Block date range: {blocks_df.index.min()} to {blocks_df.index.max()}")
    
    return blocks_df

def _append_previous_days_to_df(daily_df: pd.DataFrame, config : ConfigManager, ratio_period_days : int, day_table_name : str, primary_key : str = "time_pt") -> pd.DataFrame:
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
