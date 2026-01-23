import pandas as pd
import numpy as np
import datetime as datetime
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
    dict_of_alarms = {}
    dict_of_alarms['boundary'] = flag_boundary_alarms(df, config, full_days=day_list, system=system, default_fault_time= default_boundary_fault_time)
    dict_of_alarms['power ratio'] = power_ratio_alarm(daily_data, config, day_table_name = config.get_table_name(day_table_name_header), system=system, ratio_period_days=power_ratio_period_days)
    dict_of_alarms['abnormal COP'] = flag_abnormal_COP(daily_data, config, system = system, default_high_bound=default_cop_high_bound, default_low_bound=default_cop_low_bound)
    dict_of_alarms['temperature maintenance setpoint'] = flag_high_tm_setpoint(df, daily_data, config, system=system)
    dict_of_alarms['recirculation loop balancing valve'] = flag_recirc_balance_valve(daily_data, config, system=system)
    dict_of_alarms['HPWH inlet temperature'] = flag_hp_inlet_temp(df, daily_data, config, system)
    dict_of_alarms['HPWH outlet temperature'] = flag_hp_outlet_temp(df, daily_data, config, system)
    dict_of_alarms['improper backup heating use'] = flag_backup_use(df, daily_data, config, system)
    dict_of_alarms['blown equipment fuse'] = flag_blown_fuse(df, daily_data, config, system)
    dict_of_alarms['unexpected SOO change'] = flag_unexpected_soo_change(df, daily_data, config, system)
    dict_of_alarms['short cycle'] = flag_shortcycle(df, daily_data, config, system)
    dict_of_alarms['HPWH outage'] = flag_HP_outage(df, daily_data, config, day_table_name = config.get_table_name(day_table_name_header), system=system)
    dict_of_alarms['unexpected temperature'] = flag_unexpected_temp(df, daily_data, config, system)
    dict_of_alarms['demand response inconsistency'] = flag_ls_mode_inconsistancy(df, daily_data, config, system)


    ongoing_COP_exception = ['abnormal COP']

    for key, value in dict_of_alarms.items():
        if key in ongoing_COP_exception and _check_if_during_ongoing_cop_alarm(daily_data, config, site_name):
            print("Ongoing DATA_LOSS_COP detected. No further DATA_LOSS_COP events will be uploaded")
        elif len(value) > 0:
            print(f"Detected {key} alarm(s). Adding to event df...")
            alarm_df = pd.concat([alarm_df, value])
        else:
            print(f"No {key} alarm(s) detected.")

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

def flag_high_tm_setpoint(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, default_fault_time : int = 3, 
                             system: str = "", default_setpoint : float = 130.0, default_power_indication : float = 1.0,
                             default_power_ratio : float = 0.4) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    TMSTPT_T_ID:### - Swing Tank Outlet Temperature. Alarm triggered if over number ### (or 130) for 3 minutes with power on
    TMSTPT_SP_ID:### - Swing Tank Power. ### is lowest recorded power for Swing Tank to be considered 'on'. Defaults to 1.0
    TMSTPT_TP_ID:### - Total System Power for ratio alarming for alarming if swing tank power is more than ### (40% default) of usage
    TMSTPT_ST_ID:### - Swing Tank Setpoint that should not change at all from ### (default 130)

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data. Used for checking power ratios and determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the TMSTPT alarm codes (e.g., TMSTPT_T_1:140, TMSTPT_SP_1:2.0)
    default_fault_time : int
        Number of consecutive minutes for T+SP alarms (default 3). T+SP alarms trigger when tank is powered and temperature exceeds
        setpoint for this many consecutive minutes.
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    default_setpoint : float
        Default temperature setpoint in degrees for T and ST alarm codes when no custom bound is specified (default 130.0)
    default_power_indication : float
        Default power threshold in kW for SP alarm codes when no custom bound is specified (default 1.0)
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4)

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag swing tank setpoint alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'TMSTPT', 
                {'T' : default_setpoint,
                 'SP': default_power_indication,
                 'TP': default_power_ratio,
                 'ST': default_setpoint},
                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for day in daily_df.index:
        next_day = day + pd.Timedelta(days=1)
        filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
        alarmed_for_day = False
        for alarm_id in bounds_df['alarm_code_id'].unique():
            id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]

            # Get T and SP alarm codes for this ID
            t_codes = id_group[id_group['alarm_code_type'] == 'T']
            sp_codes = id_group[id_group['alarm_code_type'] == 'SP']
            tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
            st_codes = id_group[id_group['alarm_code_type'] == 'ST']

            # Check for multiple T or SP codes with same ID
            if len(t_codes) > 1 or len(sp_codes) > 1 or len(tp_codes) > 1 or len(st_codes) > 1:
                raise Exception(f"Improper alarm codes for swing tank setpoint with id {alarm_id}")

            # Check if we have both T and SP
            if len(t_codes) == 1 and len(sp_codes) == 1:
                t_var_name = t_codes.iloc[0]['variable_name']
                t_pretty_name = t_codes.iloc[0]['pretty_name']
                sp_var_name = sp_codes.iloc[0]['variable_name']
                sp_pretty_name = sp_codes.iloc[0]['pretty_name']
                sp_power_indication = sp_codes.iloc[0]['bound']
                t_setpoint = t_codes.iloc[0]['bound']
                # Check if both variables exist in df
                if t_var_name in filtered_df.columns and sp_var_name in filtered_df.columns:
                    # Check for consecutive minutes where SP > default_power_indication
                    # AND T >= default_setpoint
                    power_mask = filtered_df[sp_var_name] >= sp_power_indication
                    temp_mask = filtered_df[t_var_name] >= t_setpoint
                    combined_mask = power_mask & temp_mask

                    # Check for 3 consecutive minutes
                    consecutive_condition = combined_mask.rolling(window=default_fault_time).min() == 1
                    if consecutive_condition.any():
                        # Get the first index where condition was met
                        first_true_index = consecutive_condition.idxmax()
                        # Adjust for the rolling window (first fault_time-1 minutes don't count)
                        adjusted_time = first_true_index - pd.Timedelta(minutes=default_fault_time-1)
                        _add_an_alarm(alarms, adjusted_time, sp_var_name, f"High TM Setpoint: {sp_pretty_name} showed draw at {adjusted_time} although {t_pretty_name} was above {t_setpoint} F.")
                        alarmed_for_day = True
            if not alarmed_for_day and len(st_codes) == 1:
                st_var_name = st_codes.iloc[0]['variable_name']
                st_setpoint = st_codes.iloc[0]['bound']
                st_pretty_name = st_codes.iloc[0]['pretty_name']
                # Check if st_var_name exists in filtered_df
                if st_var_name in filtered_df.columns:
                    # Check if setpoint was altered for over 10 minutes
                    altered_mask = filtered_df[st_var_name] != st_setpoint
                    consecutive_condition = altered_mask.rolling(window=10).min() == 1
                    if consecutive_condition.any():
                        # Get the first index where condition was met
                        first_true_index = consecutive_condition.idxmax()
                        # Adjust for the rolling window
                        adjusted_time = first_true_index - pd.Timedelta(minutes=9)
                        _add_an_alarm(alarms, day, st_var_name, f"{st_pretty_name} was altered at {adjusted_time}")
                        alarmed_for_day = True
            if not alarmed_for_day and len(tp_codes) == 1 and len(sp_codes) == 1:
                tp_var_name = tp_codes.iloc[0]['variable_name']
                sp_var_name = sp_codes.iloc[0]['variable_name']
                sp_pretty_name = sp_codes.iloc[0]['pretty_name']
                tp_ratio = tp_codes.iloc[0]['bound']
                # Check if both variables exist in df
                if tp_var_name in daily_df.columns and sp_var_name in daily_df.columns:
                    # Check if swing tank power ratio exceeds threshold
                    if day in daily_df.index and daily_df.loc[day, tp_var_name] != 0:
                        power_ratio = daily_df.loc[day, sp_var_name] / daily_df.loc[day, tp_var_name]
                        if power_ratio > tp_ratio:
                            _add_an_alarm(alarms, day, sp_var_name, f"High temperature maintenace power ratio: {sp_pretty_name} accounted for more than {tp_ratio * 100}% of daily power.")
    return _convert_silent_alarm_dict_to_df(alarms) 

def flag_backup_use(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, 
                             system: str = "", default_setpoint : float = 130.0, default_power_ratio : float = 0.1) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    BU_P_ID - Back Up Tank Power Varriable. Must be in same power units as total system power
    BU_TP_ID:### - Total System Power for ratio alarming for alarming if back up power is more than ### (40% default) of usage
    BU_ST_ID:### - Back Up Setpoint that should not change at all from ### (default 130)

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data. Used for checking power ratios and determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the STS alarm codes (e.g., STS_T_1:140, STS_SP_1:2.0)
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    default_setpoint : float
        Default temperature setpoint in degrees for T and ST alarm codes when no custom bound is specified (default 130.0)
    default_power_indication : float
        Default power threshold in kW for SP alarm codes when no custom bound is specified (default 1.0)
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4)

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag swing tank setpoint alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'BU', 
                {'POW': None,
                 'TP': default_power_ratio,
                 'ST': default_setpoint},
                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for day in daily_df.index:
        next_day = day + pd.Timedelta(days=1)
        filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
        alarmed_for_day = False
        for alarm_id in bounds_df['alarm_code_id'].unique():
            id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]

            # Get T and SP alarm codes for this ID
            pow_codes = id_group[id_group['alarm_code_type'] == 'POW']
            tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
            st_codes = id_group[id_group['alarm_code_type'] == 'ST']

            # Check for multiple T or SP codes with same ID
            if len(tp_codes) > 1:
                raise Exception(f"Improper alarm codes for swing tank setpoint with id {alarm_id}")

            if not alarmed_for_day and len(st_codes) >= 1:
                # Check each ST code against its individual bound
                for idx, st_row in st_codes.iterrows():
                    st_var_name = st_row['variable_name']
                    st_setpoint = st_row['bound']
                    # Check if st_var_name exists in filtered_df
                    if st_var_name in filtered_df.columns:
                        # Check if setpoint was altered for over 10 minutes
                        altered_mask = filtered_df[st_var_name] != st_setpoint
                        consecutive_condition = altered_mask.rolling(window=10).min() == 1
                        if consecutive_condition.any():
                            # Get the first index where condition was met
                            first_true_index = consecutive_condition.idxmax()
                            # Adjust for the rolling window
                            adjusted_time = first_true_index - pd.Timedelta(minutes=9)
                            _add_an_alarm(alarms, day, st_var_name, f"Swing tank setpoint was altered at {adjusted_time}")
                            alarmed_for_day = True
                            break  # Exit loop once we've found an alarm for this day
            if not alarmed_for_day and len(tp_codes) == 1 and len(pow_codes) >= 1:
                tp_var_name = tp_codes.iloc[0]['variable_name']
                tp_bound = tp_codes.iloc[0]['bound']
                if tp_var_name in daily_df.columns:
                    # Get list of ER variable names
                    bu_pow_names = pow_codes['variable_name'].tolist()

                    # Check if all ER variables exist in daily_df
                    if all(var in daily_df.columns for var in bu_pow_names):
                        # Sum all ER variables for this day
                        bu_pow_sum = daily_df.loc[day, bu_pow_names].sum()
                        tp_value = daily_df.loc[day, tp_var_name]

                        # Check if sum of ER >= OUT value
                        if bu_pow_sum >= tp_value*tp_bound:
                            _add_an_alarm(alarms, day, tp_var_name, f"Improper Back Up Use: Sum of back up equipment ({bu_pow_sum:.2f}) exceeds {(tp_bound * 100):.2f}% of total power.")
    
    return _convert_silent_alarm_dict_to_df(alarms) 

def flag_HP_outage(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, day_table_name : str, system: str = "", default_power_ratio : float = 0.3,
                   ratio_period_days : int = 7) -> pd.DataFrame:
    """
    Detects possible heat pump failures or outages by checking if heat pump power consumption falls below
    an expected ratio of total system power over a rolling period, or by checking for non-zero values in
    a direct alarm variable from the heat pump controller.

    VarNames syntax:
    HPOUT_POW_[OPTIONAL ID]:### - Heat pump power variable. ### is the minimum expected ratio of HP power to total power
        (default 0.3 for 30%). Must be in same power units as total system power.
    HPOUT_TP_[OPTIONAL ID] - Total system power variable for ratio comparison. Required when using POW codes.
    HPOUT_ALRM_[OPTIONAL ID] - Direct alarm variable from HP controller. Alarm triggers if any non-zero value is detected.

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. Used for checking ALRM codes for non-zero values.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for checking power ratios over the rolling period.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the HPOUT alarm codes (e.g., HPOUT_POW_1:0.3, HPOUT_TP_1, HPOUT_ALRM_1).
    day_table_name : str
        Name of the daily database table to fetch previous days' data for the rolling period calculation.
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_ratio : float
        Default minimum power ratio threshold (as decimal, e.g., 0.3 for 30%) for POW alarm codes when no custom bound is specified (default 0.3).
        An alarm triggers if HP power falls below this ratio of total power over the rolling period.
    ratio_period_days : int
        Number of days to use for the rolling power ratio calculation (default 7). Must be greater than 1.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag swing tank setpoint alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'HPOUT', 
                {'POW': default_power_ratio,
                 'TP': None,
                 'ALRM': None},
                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for alarm_id in bounds_df['alarm_code_id'].unique():
            id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]

            # Get T and SP alarm codes for this ID
            pow_codes = id_group[id_group['alarm_code_type'] == 'POW']
            tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
            alrm_codes = id_group[id_group['alarm_code_type'] == 'ALRM']
            if len(pow_codes) > 0 and len(tp_codes) != 1:
                raise Exception(f"Improper alarm codes for heat pump outage with id {alarm_id}. Requires 1 total power (TP) variable.")
            elif len(pow_codes) > 0 and len(tp_codes) == 1:
                if ratio_period_days <= 1:
                    print("HP Outage alarm period, ratio_period_days, must be more than 1")
                else: 
                    tp_var_name = tp_codes.iloc[0]['variable_name'] 
                    daily_df_copy = daily_df.copy()
                    daily_df_copy = _append_previous_days_to_df(daily_df_copy, config, ratio_period_days, day_table_name)
                    for i in range(ratio_period_days - 1, len(daily_df_copy)):
                        start_idx = i - ratio_period_days + 1
                        end_idx = i + 1
                        day = daily_df_copy.index[i]
                        block_data = daily_df_copy.iloc[start_idx:end_idx].sum()
                        for j in range(len(pow_codes)):
                            pow_var_name = pow_codes.iloc[j]['variable_name']
                            pow_var_bound = pow_codes.iloc[j]['bound']
                            if block_data[pow_var_name] < block_data[tp_var_name] * pow_var_bound:
                                _add_an_alarm(alarms, day, pow_var_name, f"Possible Heat Pump failure or outage.")
            elif len(alrm_codes) > 0:
                for i in range(len(alrm_codes)):
                    alrm_var_name = alrm_codes.iloc[i]['variable_name']
                    if alrm_var_name in df.columns:
                        for day in daily_df.index:
                            next_day = day + pd.Timedelta(days=1)
                            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
                            if not filtered_df.empty and (filtered_df[alrm_var_name] != 0).any():
                                _add_an_alarm(alarms, day, alrm_var_name, f"Heat pump alarm triggered.")
                                break

    return _convert_silent_alarm_dict_to_df(alarms) 

def flag_recirc_balance_valve(daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_ratio : float = 0.4) -> pd.DataFrame:
    """
    Detects recirculation balance issues by comparing sum of ER (equipment recirculation) heater
    power to either total power or heating output.

    VarNames syntax:
    BV_ER_[OPTIONAL ID] - Indicates a power variable for an ER heater (equipment recirculation).
        Multiple ER variables with the same ID will be summed together.
    BV_TP_[OPTIONAL ID]:### - Indicates the Total Power of the system. Optional ### for the percentage
        threshold that should not be crossed by the ER elements (default 0.4 for 40%).
        Alarm triggers when sum of ER >= total_power * threshold.
    BV_OUT_[OPTIONAL ID] - Indicates the heating output variable the ER heating contributes to.
        Alarm triggers when sum of ER > sum of OUT * 0.95 (i.e., ER exceeds 95% of heating output).
        Multiple OUT variables with the same ID will be summed together.

    Note: Each alarm ID requires at least one ER code AND either one TP code OR at least one OUT code.
    If a TP code exists for an ID, it takes precedence over OUT codes.

    Parameters
    ----------
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for checking recirculation balance by comparing sum of ER equipment
        power to total power or heating output power.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the BV alarm codes (e.g., BV_ER_1, BV_TP_1:0.3)
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if daily_df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'BV', 
                {'TP' : default_power_ratio},
                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no BV alarms to look into 
    # Process each unique alarm_code_id
    alarms = {}
    for alarm_id in bounds_df['alarm_code_id'].unique():
        id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]
        out_codes = id_group[id_group['alarm_code_type'] == 'OUT']
        tp_codes = id_group[id_group['alarm_code_type'] == 'TP']
        er_codes = id_group[id_group['alarm_code_type'] == 'ER']
        if len(er_codes) < 1 or (len(out_codes) < 1 and len(tp_codes) != 1):
            raise Exception(f"Improper alarm codes for balancing valve with id {alarm_id}")
        er_var_names = er_codes['variable_name'].tolist()
        if len(tp_codes) == 1 and tp_codes.iloc[0]['variable_name']in daily_df.columns:
            tp_var_name = tp_codes.iloc[0]['variable_name']
            tp_bound = tp_codes.iloc[0]['bound']
            for day in daily_df.index:

                # Check if all ER variables exist in daily_df
                if all(var in daily_df.columns for var in er_var_names):
                    # Sum all ER variables for this day
                    er_sum = daily_df.loc[day, er_var_names].sum()
                    tp_value = daily_df.loc[day, tp_var_name]

                    # Check if sum of ER >= OUT value
                    if er_sum >= tp_value*tp_bound:
                        _add_an_alarm(alarms, day, tp_var_name, f"Recirculation imbalance: Sum of recirculation equipment ({er_sum:.2f}) exceeds or equals {(tp_bound * 100):.2f}% of total power.")
        elif len(out_codes) >= 1:
            out_var_names = out_codes['variable_name'].tolist()
            for day in daily_df.index:

                # Check if all ER variables exist in daily_df
                if all(var in daily_df.columns for var in er_var_names) and all(var in daily_df.columns for var in out_var_names):
                    # Sum all ER variables for this day
                    er_sum = daily_df.loc[day, er_var_names].sum()
                    out_sum = daily_df.loc[day, out_var_names].sum()

                    # Check if sum of ER >= OUT value
                    if er_sum > out_sum:
                        _add_an_alarm(alarms, day, out_codes.iloc[0]['variable_name'], f"Recirculation imbalance: Sum of recirculation equipment power ({er_sum:.2f} kW) exceeds TM heating output ({out_sum:.2f} kW).")
    return _convert_silent_alarm_dict_to_df(alarms)

def flag_hp_inlet_temp(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_temp_threshold : float = 115.0, fault_time : int = 5) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    HPI_POW_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'
    HPI_T_[OPTIONAL ID]:### - Indicates heat pump inlet temperature variable. ### is the temperature threshold (default 120.0)
        that should not be exceeded while the heat pump is on

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the HPI alarm codes (e.g., HPI_POW_1:0.5, HPI_T_1:125.0)
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 0.4). Heat pump is considered 'on'
        when power exceeds this value.
    default_temp_threshold : float
        Default temperature threshold for T alarm codes when no custom bound is specified (default 120.0). Alarm triggers when
        temperature exceeds this value while heat pump is on.
    fault_time : int
        Number of consecutive minutes that both power and temperature must exceed their thresholds before triggering an alarm (default 10).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'HPI',
                                               {'POW' : default_power_threshold,
                                                'T' : default_temp_threshold},
                                                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for alarm_id in bounds_df['alarm_code_id'].unique():
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]
            pow_codes = id_group[id_group['alarm_code_type'] == 'POW']
            pow_var_name = pow_codes.iloc[0]['variable_name']
            pow_thresh = pow_codes.iloc[0]['bound']
            t_codes = id_group[id_group['alarm_code_type'] == 'T']
            t_var_name = t_codes.iloc[0]['variable_name']
            t_pretty_name = t_codes.iloc[0]['pretty_name']
            t_thresh = t_codes.iloc[0]['bound']
            if len(t_codes) != 1 or len(pow_codes) != 1:
                raise Exception(f"Improper alarm codes for balancing valve with id {alarm_id}")
            if pow_var_name in filtered_df.columns and t_var_name in filtered_df.columns:
                # Check for consecutive minutes where both power and temp exceed thresholds
                power_mask = filtered_df[pow_var_name] > pow_thresh
                temp_mask = filtered_df[t_var_name] > t_thresh
                combined_mask = power_mask & temp_mask

                # Check for fault_time consecutive minutes
                consecutive_condition = combined_mask.rolling(window=fault_time).min() == 1
                if consecutive_condition.any():
                    first_true_index = consecutive_condition.idxmax()
                    adjusted_time = first_true_index - pd.Timedelta(minutes=fault_time-1)
                    _add_an_alarm(alarms, day, t_var_name, f"High heat pump inlet temperature: {t_pretty_name} was above {t_thresh:.1f} while HP was ON starting at {adjusted_time}.")

    return _convert_silent_alarm_dict_to_df(alarms)

def flag_hp_outlet_temp(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_temp_threshold : float = 140.0, fault_time : int = 5) -> pd.DataFrame:
    """
    Detects low heat pump outlet temperature by checking if the outlet temperature falls below a threshold
    while the heat pump is running. The first 10 minutes after each HP turn-on are excluded as a warmup
    period. An alarm triggers if the temperature stays below the threshold for `fault_time` consecutive
    minutes after the warmup period.

    VarNames syntax:
    HPO_POW_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'.
    HPO_T_[OPTIONAL ID]:### - Indicates heat pump outlet temperature variable. ### is the temperature threshold (default 140.0)
        that should always be exceeded while the heat pump is on after the 10-minute warmup period.

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
        name of each variable in the dataframe that requires alarming and the HPO alarm codes (e.g., HPO_POW_1:1.0, HPO_T_1:140.0).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    default_temp_threshold : float
        Default temperature threshold for T alarm codes when no custom bound is specified (default 140.0). Alarm triggers when
        temperature falls BELOW this value while heat pump is on (after warmup period).
    fault_time : int
        Number of consecutive minutes that temperature must be below threshold (after warmup) before triggering an alarm (default 5).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'HPO',
                                               {'POW' : default_power_threshold,
                                                'T' : default_temp_threshold},
                                                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for alarm_id in bounds_df['alarm_code_id'].unique():
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]
            pow_codes = id_group[id_group['alarm_code_type'] == 'POW']
            pow_var_name = pow_codes.iloc[0]['variable_name']
            pow_thresh = pow_codes.iloc[0]['bound']
            t_codes = id_group[id_group['alarm_code_type'] == 'T']
            t_var_name = t_codes.iloc[0]['variable_name']
            t_pretty_name = t_codes.iloc[0]['pretty_name']
            t_thresh = t_codes.iloc[0]['bound']
            if len(t_codes) != 1 or len(pow_codes) != 1:
                raise Exception(f"Improper alarm codes for balancing valve with id {alarm_id}")
            if pow_var_name in filtered_df.columns and t_var_name in filtered_df.columns:
                # Check for consecutive minutes where both power and temp exceed thresholds
                power_mask = filtered_df[pow_var_name] > pow_thresh
                temp_mask = filtered_df[t_var_name] < t_thresh

                # Exclude first 10 minutes after each HP turn-on (warmup period)
                warmup_minutes = 10
                mask_changes = power_mask != power_mask.shift(1)
                run_groups = mask_changes.cumsum()
                cumcount_in_run = power_mask.groupby(run_groups).cumcount() + 1
                past_warmup_mask = power_mask & (cumcount_in_run > warmup_minutes)

                combined_mask = past_warmup_mask & temp_mask

                # Check for fault_time consecutive minutes
                consecutive_condition = combined_mask.rolling(window=fault_time).min() == 1
                if consecutive_condition.any():
                    first_true_index = consecutive_condition.idxmax()
                    adjusted_time = first_true_index - pd.Timedelta(minutes=fault_time-1)
                    _add_an_alarm(alarms, day, t_var_name, f"Low heat pump outlet temperature: {t_pretty_name} was below {t_thresh:.1f} while HP was ON starting at {adjusted_time}.")

    return _convert_silent_alarm_dict_to_df(alarms)

def flag_blown_fuse(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_power_range : float = 2.0, default_power_draw : float = 30, fault_time : int = 3) -> pd.DataFrame:
    """
    Detects blown fuse alarms for heating elements by identifying when an element is drawing power
    but significantly less than expected, which may indicate a blown fuse.

    VarNames syntax:
    BF_[OPTIONAL ID]:### - Indicates a blown fuse alarm for an element. ### is the expected kW input when the element is on.

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
        name of each variable in the dataframe that requires alarming and the BF alarm codes (e.g., BF:30, BF_1:25).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Power threshold to determine if the element is "on" (default 1.0). Element is considered on when power exceeds this value.
    default_power_range : float
        Allowable variance below the expected power draw (default 2.0). An alarm triggers when the actual power draw is less than
        (expected_power_draw - default_power_range) while the element is on.
    default_power_draw : float
        Default expected power draw in kW when no custom bound is specified in the alarm code (default 30).
    fault_time : int
        Number of consecutive minutes that the fault condition must persist before triggering an alarm (default 3).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'BF',
                                               {'default' : default_power_draw},
                                                system, two_part_tag=False)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for var_name in bounds_df['variable_name'].unique():
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            rows = bounds_df[bounds_df['variable_name'] == var_name]
            expected_power_draw = rows.iloc[0]['bound']
            if len(rows) != 1:
                raise Exception(f"Multiple blown fuse alarm codes for {var_name}")
            if var_name in filtered_df.columns:
                # Check for consecutive minutes where both power and temp exceed thresholds
                power_on_mask = filtered_df[var_name] > default_power_threshold
                unexpected_power_mask = filtered_df[var_name] < expected_power_draw - default_power_range
                combined_mask = power_on_mask & unexpected_power_mask

                # Check for fault_time consecutive minutes
                consecutive_condition = combined_mask.rolling(window=fault_time).min() == 1
                if consecutive_condition.any():
                    first_true_index = consecutive_condition.idxmax()
                    adjusted_time = first_true_index - pd.Timedelta(minutes=fault_time-1)
                    _add_an_alarm(alarms, day, var_name, f"Blown Fuse: {var_name} had a power draw less than {expected_power_draw - default_power_range:.1f} while element was ON starting at {adjusted_time}.")

    return _convert_silent_alarm_dict_to_df(alarms)

def flag_unexpected_soo_change(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_on_temp : float = 115.0, default_off_temp : float = 140.0) -> pd.DataFrame:
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
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the SOOCHNG alarm codes (e.g., SOOCHNG_POW_normal:1.0, SOOCHNG_ON_normal:115.0, SOOCHNG_OFF_normal:140.0).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    default_on_temp : float
        Default ON temperature threshold (default 115.0). When the HP turns on, an alarm triggers if the temperature
        is more than 5.0 degrees away from this value.
    default_off_temp : float
        Default OFF temperature threshold (default 140.0). When the HP turns off, an alarm triggers if the temperature
        is more than 5.0 degrees away from this value.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    soo_dict = {
        'loadUp' : 'LOAD UP',
        'shed' : 'SHED',
        'criticalPeak': 'CRITICAL PEAK',
        'gridEmergency' : 'GRID EMERGENCY',
        'advLoadUp' : 'ADVANCED LOAD UP'
    }
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'SOOCHNG',
                                               {'POW' : default_power_threshold,
                                                'ON' : default_on_temp,
                                                'OFF' : default_off_temp},
                                                system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 
    
    ls_df = config.get_ls_df()

    # Process each unique alarm_code_id
    alarms = {}
    pow_codes = bounds_df[bounds_df['alarm_code_type'] == 'POW']
    if len(pow_codes) != 1:
                raise Exception(f"Improper alarm codes for SOO changes; must have 1 POW variable to indicate power to HPWH(s).")
    pow_var_name = pow_codes.iloc[0]['variable_name']
    pow_thresh = pow_codes.iloc[0]['bound']
    bounds_df = bounds_df[bounds_df['alarm_code_type'] != 'POW']

    for alarm_id in bounds_df['alarm_code_id'].unique():
        ls_filtered_df = df.copy()
        soo_mode_name = 'NORMAL'
        if alarm_id in soo_dict.keys():
            if not ls_df.empty:
                # Filter ls_filtered_df for only date ranges in the right mode of ls_df
                mode_rows = ls_df[ls_df['event'] == alarm_id]
                mask = pd.Series(False, index=ls_filtered_df.index)
                for _, row in mode_rows.iterrows():
                    mask |= (ls_filtered_df.index >= row['startDateTime']) & (ls_filtered_df.index < row['endDateTime'])
                ls_filtered_df = ls_filtered_df[mask]
                soo_mode_name = soo_dict[alarm_id]
            else:
                print(f"Cannot check for {alarm_id} because there are no {alarm_id} periods in time frame.")
                continue
        elif not ls_df.empty:
            # Filter out all date range rows from ls_filtered_df's indexes
            mask = pd.Series(True, index=ls_filtered_df.index)
            for _, row in ls_df.iterrows():
                mask &= ~((ls_filtered_df.index >= row['startDateTime']) & (ls_filtered_df.index < row['endDateTime']))
            ls_filtered_df = ls_filtered_df[mask]

        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = ls_filtered_df.loc[(ls_filtered_df.index >= day) & (ls_filtered_df.index < next_day)]
            id_group = bounds_df[bounds_df['alarm_code_id'] == alarm_id]
            on_t_codes = id_group[id_group['alarm_code_type'] == 'ON']
            off_t_codes = id_group[id_group['alarm_code_type'] == 'ON']
            if len(on_t_codes) != 1 or len(off_t_codes) != 1:
                raise Exception(f"Improper alarm codes for SOO changes with id {alarm_id}. Must have 1 ON and 1 OFF variable")
            on_t_var_name = on_t_codes.iloc[0]['variable_name']
            on_t_pretty_name = on_t_codes.iloc[0]['pretty_name']
            on_t_thresh = on_t_codes.iloc[0]['bound']
            off_t_var_name = off_t_codes.iloc[0]['variable_name']
            off_t_pretty_name = off_t_codes.iloc[0]['pretty_name']
            off_t_thresh = off_t_codes.iloc[0]['bound']
            if pow_var_name in filtered_df.columns: 
                found_alarm = False
                power_below = filtered_df[pow_var_name] <= pow_thresh
                power_above = filtered_df[pow_var_name] > pow_thresh
                if on_t_var_name in filtered_df.columns:
                    power_turn_on = power_below.shift(1) & power_above
                    power_on_times = filtered_df.index[power_turn_on.fillna(False)]
                    # Check if temperature is within 5.0 of on_t_thresh at each turn-on moment
                    for power_time in power_on_times:
                        temp_at_turn_on = filtered_df.loc[power_time, on_t_var_name]
                        if abs(temp_at_turn_on - on_t_thresh) > 5.0:
                            _add_an_alarm(alarms, day, on_t_var_name,
                                f"Unexpected SOO change: during {soo_mode_name}, HP turned on at {power_time} but {on_t_pretty_name} was {temp_at_turn_on:.1f} F (setpoint at {on_t_thresh} F).")
                            found_alarm = True
                            break # TODO soon don't do this
                if not found_alarm and off_t_var_name in filtered_df.columns:
                    power_turn_off = power_above.shift(1) & power_below
                    power_off_times = filtered_df.index[power_turn_off.fillna(False)]
                    # Check if temperature is within 5.0 of off_t_thresh at each turn-on moment
                    for power_time in power_off_times:
                        temp_at_turn_off = filtered_df.loc[power_time, off_t_var_name]
                        if abs(temp_at_turn_off - off_t_thresh) > 5.0:
                            _add_an_alarm(alarms, day, off_t_var_name,
                                f"Unexpected SOO change: during {soo_mode_name}, HP turned off at {power_time} but {off_t_pretty_name} was {temp_at_turn_off:.1f} F (setpoint at {off_t_thresh} F)).")
                            found_alarm = True
                            break # TODO soon don't do this

    return _convert_silent_alarm_dict_to_df(alarms)

def flag_ls_mode_inconsistancy(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "") -> pd.DataFrame:
    """
    Detects when a variable does not match its expected value during a load shifting event.
    An alarm is triggered if the variable value does not equal the expected value during the
    time periods defined in the load shifting schedule for that mode.

    VarNames syntax:
    SOO_[mode]:### - Indicates a variable that should equal ### during [mode] load shifting events.
        [mode] can be: loadUp, shed, criticalPeak, gridEmergency, advLoadUp
        ### is the expected value (e.g., SOO_loadUp:1 means the variable should be 1 during loadUp events)

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive,
        in order minutes. If minutes are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Pandas dataframe with daily data. This dataframe should have a datetime index.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag load shift mode inconsistency alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'SOO', {}, system)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({})  # no alarms to look into

    ls_df = config.get_ls_df()
    if ls_df.empty:
        return _convert_silent_alarm_dict_to_df({})  # no load shifting events to check

    valid_modes = ['loadUp', 'shed', 'criticalPeak', 'gridEmergency', 'advLoadUp']

    alarms = {}
    for _, row in bounds_df.iterrows():
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
                mismatched = normal_df[normal_df[var_name] != expected_value]

                if not mismatched.empty:
                    first_mismatch_time = mismatched.index[0]
                    actual_value = mismatched.iloc[0][var_name]
                    _add_an_alarm(alarms, day, var_name,
                        f"Load shift mode inconsistency: {pretty_name} was {actual_value} at {first_mismatch_time} during normal operation (expected {expected_value}).")
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
                    mismatched = event_df[event_df[var_name] != expected_value]

                    if not mismatched.empty:
                        first_mismatch_time = mismatched.index[0]
                        actual_value = mismatched.iloc[0][var_name]
                        _add_an_alarm(alarms, day, var_name,
                            f"Load shift mode inconsistency: {pretty_name} was {actual_value} at {first_mismatch_time} during {mode} event (expected {expected_value}).")
                        break  # Only one alarm per variable per day

    return _convert_silent_alarm_dict_to_df(alarms)

def flag_unexpected_temp(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_high_temp : float = 130,
                       default_low_temp : float = 115, fault_time : int = 10) -> pd.DataFrame:
    """
    Detects when domestic hot water (DHW) supply temperature falls outside an acceptable range for
    too long. An alarm is triggered if the temperature is above the high bound or below the low bound
    for `fault_time` consecutive minutes.

    VarNames syntax:
    TMPRNG_[OPTIONAL ID]:###-### - Indicates a temperature variable. ###-### is the acceptable temperature range
        (e.g., TMPRNG:110-130 means temperature should stay between 110 and 130 degrees).

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the DHW alarm codes (e.g., DHW:110-130, DHW_1:115-125).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_high_temp : float
        Default high temperature bound when no custom range is specified in the alarm code (default 130). Temperature above this triggers alarm.
    default_low_temp : float
        Default low temperature bound when no custom range is specified in the alarm code (default 130). Temperature below this triggers alarm.
    fault_time : int
        Number of consecutive minutes that temperature must be outside the acceptable range before triggering an alarm (default 10).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'TMPRNG',
                                               {'default': [default_low_temp,default_high_temp]},
                                                system, two_part_tag=False,
                                                range_bounds=True)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for dhw_var in bounds_df['variable_name'].unique():
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            rows = bounds_df[bounds_df['variable_name'] == dhw_var]
            low_bound = rows.iloc[0]['bound']
            high_bound = rows.iloc[0]['bound2']
            pretty_name = rows.iloc[0]['pretty_name']

            if dhw_var in filtered_df.columns:
                # Check if temp is above high bound or below low bound
                out_of_range_mask = (filtered_df[dhw_var] > high_bound) | (filtered_df[dhw_var] < low_bound)

                # Check for fault_time consecutive minutes
                consecutive_condition = out_of_range_mask.rolling(window=fault_time).min() == 1
                if consecutive_condition.any():
                    first_true_index = consecutive_condition.idxmax()
                    adjusted_time = first_true_index - pd.Timedelta(minutes=fault_time-1)
                    _add_an_alarm(alarms, day, dhw_var,
                        f"Temperature out of range: {pretty_name} was outside {low_bound}-{high_bound} F for {fault_time}+ consecutive minutes starting at {adjusted_time}.")

    return _convert_silent_alarm_dict_to_df(alarms)

def flag_shortcycle(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       short_cycle_time : int = 15) -> pd.DataFrame:
    """
    Detects short cycling by identifying when the heat pump turns on for less than `short_cycle_time`
    consecutive minutes before turning off again. Short cycling can indicate equipment issues or
    improper system sizing.

    VarNames syntax:
    SHRTCYC_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'.

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
        name of each variable in the dataframe that requires alarming and the SHRTCYC alarm codes (e.g., SHRTCYC:1.0, SHRTCYC_1:0.5).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Default power threshold when no custom bound is specified in the alarm code (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    short_cycle_time : int
        Minimum expected run time in minutes (default 15). An alarm triggers if the heat pump runs for fewer than this many
        consecutive minutes before turning off.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    bounds_df = _process_bounds_df_alarm_codes(bounds_df, 'SHRTCYC',
                                               {'default' : default_power_threshold},
                                                system, two_part_tag=False)
    if bounds_df.empty:
        return _convert_silent_alarm_dict_to_df({}) # no alarms to look into 

    # Process each unique alarm_code_id
    alarms = {}
    for var_name in bounds_df['variable_name'].unique():
        for day in daily_df.index:
            next_day = day + pd.Timedelta(days=1)
            filtered_df = df.loc[(df.index >= day) & (df.index < next_day)]
            rows = bounds_df[bounds_df['variable_name'] == var_name]
            pwr_thresh = rows.iloc[0]['bound']
            var_pretty = rows.iloc[0]['pretty_name']
            if len(rows) != 1:
                raise Exception(f"Multiple blown fuse alarm codes for {var_name}")
            if var_name in filtered_df.columns:
                power_on_mask = filtered_df[var_name] > pwr_thresh

                # Find runs of consecutive True values by detecting changes in the mask
                mask_changes = power_on_mask != power_on_mask.shift(1)
                run_groups = mask_changes.cumsum()

                # For each run where power is on, check if it's shorter than short_cycle_time
                for group_id in run_groups[power_on_mask].unique():
                    run_indices = filtered_df.index[(run_groups == group_id) & power_on_mask]
                    run_length = len(run_indices)
                    if run_length > 0 and run_length < short_cycle_time:
                        start_time = run_indices[0]
                        _add_an_alarm(alarms, day, var_name,
                            f"Short cycle: {var_pretty} was on for only {run_length} minutes starting at {start_time}.")
                        break

    return _convert_silent_alarm_dict_to_df(alarms)

def _process_bounds_df_alarm_codes(bounds_df : pd.DataFrame, alarm_tag : str, type_default_dict : dict = {}, system : str = "",
                                   two_part_tag : bool = True, range_bounds : bool = False) -> pd.DataFrame:
    # Should only do for alarm codes of format: [TAG]_[TYPE]_[OPTIONAL_ID]:[BOUND]
    if (system != ""):
        if not 'system' in bounds_df.columns:
            raise Exception("system parameter is non null, however, system is not present in Variable_Names.csv")
        bounds_df = bounds_df.loc[bounds_df['system'] == system]

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
    
    bounds_df = bounds_df[bounds_df['alarm_codes'].str.contains(alarm_tag, na=False)]

    # Split alarm_codes by semicolons and create a row for each STS code
    expanded_rows = []
    for idx, row in bounds_df.iterrows():
        alarm_codes = str(row['alarm_codes']).split(';')
        tag_codes = [code.strip() for code in alarm_codes if code.strip().startswith(alarm_tag)]

        if tag_codes:  # Only process if there are STS codes
            for tag_code in tag_codes:
                new_row = row.copy()
                if ":" in tag_code:
                    tag_parts = tag_code.split(':')
                    if len(tag_parts) > 2:
                        raise Exception(f"Improperly formated alarm code : {tag_code}")
                    if range_bounds:
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
                    if range_bounds:
                        new_row['bound2'] = None
                new_row['alarm_codes'] = tag_code

                expanded_rows.append(new_row)

    if expanded_rows:
        bounds_df = pd.DataFrame(expanded_rows)
    else:
        return pd.DataFrame()# no tagged alarms to look into
    
    alarm_code_parts = []
    for idx, row in bounds_df.iterrows():
        parts = row['alarm_codes'].split('_')
        if two_part_tag:
            if len(parts) == 2:
                alarm_code_parts.append([parts[1], "No ID"])
            elif len(parts) == 3:
                alarm_code_parts.append([parts[1], parts[2]])
            else:
                raise Exception(f"improper {alarm_tag} alarm code format for {row['variable_name']}")
        else:
            if len(parts) == 1:
                alarm_code_parts.append(["default", "No ID"])
            elif len(parts) == 2:
                alarm_code_parts.append(["default", parts[1]])
            else:
                raise Exception(f"improper {alarm_tag} alarm code format for {row['variable_name']}")
    if alarm_code_parts:
        bounds_df[['alarm_code_type', 'alarm_code_id']] = pd.DataFrame(alarm_code_parts, index=bounds_df.index)

        # Replace None bounds with appropriate defaults based on alarm_code_type
        for idx, row in bounds_df.iterrows():
            if pd.isna(row['bound']) or row['bound'] is None:
                if row['alarm_code_type'] in type_default_dict.keys():
                    if range_bounds:
                        bounds_df.at[idx, 'bound'] = type_default_dict[row['alarm_code_type']][0]
                        bounds_df.at[idx, 'bound2'] = type_default_dict[row['alarm_code_type']][1]
                    else:
                        bounds_df.at[idx, 'bound'] = type_default_dict[row['alarm_code_type']]
        # Coerce bound column to float
        bounds_df['bound'] = pd.to_numeric(bounds_df['bound'], errors='coerce').astype(float)
        if range_bounds:
            bounds_df['bound2'] = pd.to_numeric(bounds_df['bound2'], errors='coerce').astype(float)

    return bounds_df

def _add_an_alarm(alarm_dict : dict, day : datetime, var_name : str, alarm_string : str):
    # Round down to beginning of day
    day = pd.Timestamp(day).normalize()

    if day in alarm_dict:
        alarm_dict[day].append([var_name, alarm_string])
    else:
        alarm_dict[day] = [[var_name, alarm_string]]

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
