import pandas as pd
import numpy as np
import datetime as dt
import csv
import os
from ecopipeline.utils.unit_convert import temp_c_to_f_non_noaa, volume_l_to_g, power_btuhr_to_kw
from ecopipeline import ConfigManager

pd.set_option('display.max_columns', None)


def concat_last_row(df: pd.DataFrame, last_row: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes in a dataframe with new data and a second data frame meant to be the
    last row from the database the new data is being processed for. The two dataframes are then concatenated 
    such that the new data can later be forward filled from the info the last row

    Parameters
    ----------
    df : pd.DataFrame
        dataframe with new data that needs to be forward filled from data in the last row of a database
    last_row : pd.DataFrame 
        last row of the database to forward fill from in a pandas dataframe
    
    Returns
    -------
    pd.DataFrame: 
        Pandas dataframe with last row concatenated
    """
    df = pd.concat([last_row, df], join="inner")
    df = df.sort_index()
    return df


def round_time(df: pd.DataFrame):
    """
    Function takes in a dataframe and rounds dataTime index down to the nearest minute. Works in place

    Parameters
    ----------
    df : pd.DataFrame
        a dataframe indexed by datetimes. These date times will all be rounded down to the nearest minute.

    Returns
    -------
    boolean
        Returns True if the indexes have been rounded down. Returns False if the fuinction failed (e.g. if df was empty)
    """
    if (df.empty):
        return False
    if not df.index.tz is None:
        tz = df.index.tz
        df.index = df.index.tz_localize(None)
        df.index = df.index.floor('T')
        df.index = df.index.tz_localize(tz, ambiguous='infer')
    else:
        df.index = df.index.floor('T')
    return True


def rename_sensors(original_df: pd.DataFrame, config : ConfigManager, site: str = "", system: str = ""):
    """
    Function will take in a dataframe and a string representation of a file path and renames
    sensors from their alias to their true name. Also filters the dataframe by site and system if specified.

    Parameters
    ---------- 
    original_df: pd.DataFrame
        A dataframe that contains data labeled by the raw varriable names to be renamed.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv")
        The csv this points to should have at least 2 columns called "variable_alias" (the raw name to be changed from) and "variable_name"
        (the name to be changed to). All columns without a cooresponding variable_name will be dropped from the dataframe.
    site: str
        If the pipeline is processing data for a particular site with a dataframe that contains data from multiple sites that 
        need to be prossessed seperatly, fill in this optional varriable to drop data from all other sites in the returned dataframe. 
        Appropriate varriables in your Variable_Names.csv must have a matching substring to this varriable in a column called "site".
    system: str
        If the pipeline is processing data for a particular system with a dataframe that contains data from multiple systems that 
        need to be prossessed seperatly, fill in this optional varriable to drop data from all other systems in the returned dataframe. 
        Appropriate varriables in your Variable_Names.csv must have a matching string to this varriable in a column called "system"
    
    Returns
    -------  
    df: pd.DataFrame 
        Pandas dataframe that has been filtered by site and system (if either are applicable) with column names that match those specified in
        Variable_Names.csv.
    """
    variable_names_path = config.get_var_names_path()
    try:
        variable_data = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        raise Exception("File Not Found: "+ variable_names_path)
    
    if (site != ""):
        variable_data = variable_data.loc[variable_data['site'] == site]
    if (system != ""):
        variable_data = variable_data.loc[variable_data['system'].str.contains(system, na=False)]

    variable_data = variable_data.loc[:, ['variable_alias', 'variable_name']]
    variable_data.dropna(axis=0, inplace=True)
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))
    # Create a copy of the original DataFrame
    df = original_df.copy()

    df.rename(columns=variable_alias_true_dict, inplace=True)

    # drop columns that do not have a corresponding true name
    df.drop(columns=[col for col in df if col in variable_alias and col not in variable_true], inplace=True)

    # drop columns that are not documented in variable names csv file at all
    df.drop(columns=[col for col in df if col not in variable_true], inplace=True)
    #drop null columns
    df = df.dropna(how='all')

    return df

def avg_duplicate_times(df: pd.DataFrame, timezone : str) -> pd.DataFrame:
    """
    Function will take in a dataframe and look for duplicate timestamps (ususally due to daylight savings or rounding). 
    The dataframe will be altered to just have one line for the timestamp, takes the average values between the duplicate timestamps
    for the columns of the line.

    Parameters
    ----------
    df: pd.DataFrame 
        Pandas dataframe to be altered
    timezone: str 
        The timezone for the indexes in the output dataframe as a string. Must be a string recognized as a 
        time stamp by the pandas tz_localize() function https://pandas.pydata.org/docs/reference/api/pandas.Series.tz_localize.html
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas dataframe with all duplicate timestamps compressed into one, averegaing data values 
    """
    df.index = pd.DatetimeIndex(df.index).tz_localize(None)

    # Get columns with non-numeric values
    non_numeric_cols = df.select_dtypes(exclude='number').columns

    # Group by index, taking only the first value in case of duplicates
    non_numeric_df = df.groupby(df.index)[non_numeric_cols].first()

    numeric_df = df.groupby(df.index).mean(numeric_only = True)
    df = pd.concat([non_numeric_df, numeric_df], axis=1)
    df.index = (df.index).tz_localize(timezone)
    return df

def _rm_cols(col, bounds_df):  # Helper function for remove_outliers
    """
    Function will take in a pandas series and bounds information
    stored in a dataframe, then check each element of that column and set it to nan
    if it is outside the given bounds. 

    Args: 
        col: pd.Series 
            Pandas dataframe column from data being processed
        bounds_df: pd.DataFrame
            Pandas dataframe indexed by the names of the columns from the dataframe that col came from. There should be at least
            two columns in this dataframe, lower_bound and upper_bound, for use in removing outliers
    Returns: 
        None 
    """
    if (col.name in bounds_df.index):
        c_lower = float(bounds_df.loc[col.name]["lower_bound"])
        c_upper = float(bounds_df.loc[col.name]["upper_bound"])
        col.mask((col > c_upper) | (col < c_lower), other=np.NaN, inplace=True)

# TODO: remove_outliers STRETCH GOAL: Functionality for alarms being raised based on bounds needs to happen here.
def remove_outliers(original_df: pd.DataFrame, config : ConfigManager, site: str = "") -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of bounds information in a csv,
    store the bounds data in a dataframe, then remove outliers above or below bounds as 
    designated by the csv. Function then returns the resulting dataframe. 

    Parameters
    ----------
    original_df: pd.DataFrame
        Pandas dataframe for which outliers need to be removed
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least three columns which must be titled "variable_name", "lower_bound", and "upper_bound" which should contain the
        name of each variable in the dataframe that requires the removal of outliers, the lower bound for acceptable data, and the upper bound for
        acceptable data respectively
    site: str
        string of site name if processing a particular site in a Variable_Names.csv file with multiple sites. Leave as an empty string if not aplicable.

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with outliers removed and replaced with nans
    """
    df = original_df.copy()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return df

    if (site != ""):
        bounds_df = bounds_df.loc[bounds_df['site'] == site]

    bounds_df = bounds_df.loc[:, [
        "variable_name", "lower_bound", "upper_bound"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)
    bounds_df = bounds_df[bounds_df.index.notnull()]

    df.apply(_rm_cols, args=(bounds_df,))
    return df


def _ffill(col, ffill_df, previous_fill: pd.DataFrame = None):  # Helper function for ffill_missing
    """
    Function will take in a pandas series and ffill information from a pandas dataframe,
    then for each entry in the series, either forward fill unconditionally or up to the 
    provided limit based on the information in provided dataframe. 

    Args: 
        col (pd.Series): Pandas series
        ffill_df (pd.DataFrame): Pandas dataframe
    Returns: 
        None (df is modified, not returned)
    """
    if (col.name in ffill_df.index):
        #set initial fill value where needed for first row
        if previous_fill is not None and len(col) > 0 and pd.isna(col.iloc[0]):
            col.iloc[0] = previous_fill[col.name].iloc[0]
        cp = ffill_df.loc[col.name]["changepoint"]
        length = ffill_df.loc[col.name]["ffill_length"]
        if (length != length):  # check for nan, set to 0
            length = 0
        length = int(length)  # casting to int to avoid float errors
        if (cp == 1):  # ffill unconditionally
            col.fillna(method='ffill', inplace=True)
        elif (cp == 0):  # ffill only up to length
            col.fillna(method='ffill', inplace=True, limit=length)


def ffill_missing(original_df: pd.DataFrame, config : ConfigManager, previous_fill: pd.DataFrame = None) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 
    
    Parameters
    ----------
    original_df: pd.DataFrame
        Pandas dataframe that needs to be forward filled
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        There should be at least three columns in this csv: "variable_name", "changepoint", "ffill_length".
        The variable_name column should contain the name of each variable in the dataframe that requires forward filling.
        The changepoint column should contain one of three values: 
            "0" if the variable should be forward filled to a certain length (see ffill_length).
            "1" if the varrible should be forward filled completely until the next change point.
            null if the variable should not be forward filled.
        The ffill_length contains the number of rows which should be forward filled if the value in the changepoint is "0"
    previous_fill: pd.DataFrame (default None)
        A pandas dataframe with the same index type and at least some of the same columns as original_df (usually taken as the last entry from the pipeline that has been put
        into the destination database). The values of this will be used to forward fill into the new set of data if applicable.
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas dataframe that has been forward filled to the specifications detailed in the vars_filename csv
    """
    df = original_df.copy()
    vars_filename = config.get_var_names_path()
    try:
        # ffill dataframe holds ffill length and changepoint bool
        ffill_df = pd.read_csv(vars_filename)
    except FileNotFoundError:
        print("File Not Found: ", vars_filename)
        return df

    ffill_df = ffill_df.loc[:, [
        "variable_name", "changepoint", "ffill_length"]]
    # drop data without changepoint AND ffill_length
    ffill_df.dropna(axis=0, thresh=2, inplace=True)
    ffill_df.set_index(['variable_name'], inplace=True)
    ffill_df = ffill_df[ffill_df.index.notnull()]  # drop data without names

    # add any columns in previous_fill that are missing from df and fill with nans
    if previous_fill is not None:
       # Get column names of df and previous_fill
        a_cols = set(df.columns)
        b_cols = set(previous_fill.columns)
        b_cols.discard('time_pt') # avoid duplicate column bug

        # Find missing columns in df and add them with NaN values
        missing_cols = list(b_cols - a_cols)
        if missing_cols:
            for col in missing_cols:
                df[col] = np.nan 

    df.apply(_ffill, args=(ffill_df,previous_fill))
    return df

# TODO test this
def nullify_erroneous(original_df: pd.DataFrame, config : ConfigManager) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and make erroneous values NaN. 

    Parameters
    ---------- 
    original_df: pd.DataFrame
        Pandas dataframe that needs to be filtered for error values
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        There should be at least two columns in this csv: "variable_name" and "error_value"
        The variable_name should contain the names of all columns in the dataframe that need to have there erroneous values removed
        The error_value column should contain the error value of each variable_name, or null if there isn't an error value for that variable   
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas dataframe with error values replaced with NaNs
    """
    df = original_df.copy()
    vars_filename = config.get_var_names_path()
    try:
        # ffill dataframe holds ffill length and changepoint bool
        error_df = pd.read_csv(vars_filename)
    except FileNotFoundError:
        print("File Not Found: ", vars_filename)
        return df

    error_df = error_df.loc[:, [
        "variable_name", "error_value"]]
    # drop data without changepoint AND ffill_length
    error_df.dropna(axis=0, thresh=2, inplace=True)
    error_df.set_index(['variable_name'], inplace=True)
    error_df = error_df[error_df.index.notnull()]  # drop data without names
    for col in error_df.index:
        if col in df.columns:
            error_value = error_df.loc[col, 'error_value']
            df.loc[df[col] == error_value, col] = np.nan

    return df

#TODO investigate if this can be removed
def sensor_adjustment(df: pd.DataFrame, config : ConfigManager) -> pd.DataFrame:
    """
    TO BE DEPRICATED -- Reads in input/adjustments.csv and applies necessary adjustments to the dataframe

    Parameters
    ---------- 
    df : pd.DataFrame
        DataFrame to be adjusted
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called adjustments.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/adjustments.csv")
    
    Returns
    ------- 
    pd.DataFrame: 
        Adjusted Dataframe
    """
    adjustments_csv_path = f"{config.input_directory}adjustments.csv"
    try:
        adjustments = pd.read_csv(adjustments_csv_path)
    except FileNotFoundError:
        print(f"File Not Found: {adjustments_csv_path}")
        return df
    if adjustments.empty:
        return df

    adjustments["datetime_applied"] = pd.to_datetime(
        adjustments["datetime_applied"])
    df = df.sort_values(by="datetime_applied")

    for adjustment in adjustments:
        adjustment_datetime = adjustment["datetime_applied"]
        # NOTE: To access time, df.index (this returns a list of DateTime objects in a full df)
        # To access time object if you have located a series, it's series.name (ex: df.iloc[0].name -- this prints the DateTime for the first row in a df)
        df_pre = df.loc[df.index < adjustment_datetime]
        df_post = df.loc[df.index >= adjustment_datetime]
        match adjustment["adjustment_type"]:
            case "add":
                continue
            case "remove":
                df_post[adjustment["sensor_1"]] = np.nan
            case "swap":
                df_post[[adjustment["sensor_1"], adjustment["sensor_2"]]] = df_post[[
                    adjustment["sensor_2"], adjustment["sensor_1"]]]
        df = pd.concat([df_pre, df_post], ignore_index=True)

    return df

def cop_method_1(df: pd.DataFrame, recircLosses, heatout_primary_column : str = 'HeatOut_Primary', total_input_power_column : str = 'PowerIn_Total') -> pd.DataFrame:
    """
    Performs COP calculation method 1 (original AWS method).

    Parameters
    ----------
    df: pd.Dataframe
        Pandas dataframe representing daily averaged values from datastream to add COP columns to. Adds column called 'COP_DHWSys_1' to the dataframe in place
        The dataframe needs to already have two columns, 'HeatOut_Primary' and 'PowerIn_Total' to calculate COP_DHWSys_1
    recircLosses: float or pd.Series
        If fixed tempurature maintanance reciculation loss value from spot measurement, this should be a float.
        If reciculation losses measurements are in datastream, this should be a column of df.
        Units should be in kW.
    heatout_primary_column : str
        Name of the column that contains the output power of the primary system in kW. Defaults to 'HeatOut_Primary'
    total_input_power_column : str
        Name of the column that contains the total input power of the system in kW. Defaults to 'PowerIn_Total'

    Returns
    -------
    pd.DataFrame: Dataframe with added column for system COP called COP_DHWSys_1
    """
    columns_to_check = [heatout_primary_column, total_input_power_column]

    missing_columns = [col for col in columns_to_check if col not in df.columns]

    if missing_columns:
        print('Cannot calculate COP as the following columns are missing from the DataFrame:', missing_columns)
        return df
    
    df['COP_DHWSys_1'] = (df[heatout_primary_column] + recircLosses) / df[total_input_power_column]
    
    return df

def cop_method_2(df: pd.DataFrame, cop_tm, cop_primary_column_name) -> pd.DataFrame:
    """
    Performs COP calculation method 2 as defined by Scott's whiteboard image
    COP = COP_primary(ELEC_primary/ELEC_total) + COP_tm(ELEC_tm/ELEC_total)

    Parameters
    ---------- 
    df: pd.DataFrame
        Pandas DataFrame to add COP columns to. The dataframe needs to have a column for the COP of the primary system (see cop_primary_column_name)
        as well as a column called 'PowerIn_Total' for the total system power and columns prefixed with 'PowerIn_HPWH' or 'PowerIn_SecLoopPump' for 
        power readings taken for HPWHs/primary systems and columns prefixed with 'PowerIn_SwingTank' or 'PowerIn_ERTank' for power readings taken for 
        Temperature Maintenance systems
    cop_tm: float
        fixed COP value for temputure Maintenece system
    cop_primary_column_name: str
        Name of the column used for COP_Primary values

    Returns
    -------
    pd.DataFrame: Dataframe with added column for system COP called COP_DHWSys_2 
    """
    columns_to_check = [cop_primary_column_name, 'PowerIn_Total']

    missing_columns = [col for col in columns_to_check if col not in df.columns]

    if missing_columns:
        print('Cannot calculate COP as the following columns are missing from the DataFrame:', missing_columns)
        return df
    
    # Create list of column names to sum
    sum_primary_cols = [col for col in df.columns if col.startswith('PowerIn_HPWH') or col == 'PowerIn_SecLoopPump']
    sum_tm_cols = [col for col in df.columns if col.startswith('PowerIn_SwingTank') or col.startswith('PowerIn_ERTank')]

    if len(sum_primary_cols) == 0:
        print('Cannot calculate COP as the primary power columns (such as PowerIn_HPWH and PowerIn_SecLoopPump) are missing from the DataFrame')
        return df

    if len(sum_tm_cols) == 0:
        print('Cannot calculate COP as the temperature maintenance power columns (such as PowerIn_SwingTank) are missing from the DataFrame')
        return df
    
    # Create new DataFrame with one column called 'PowerIn_Primary' that contains the sum of the specified columns
    sum_power_in_df = pd.DataFrame({'PowerIn_Primary': df[sum_primary_cols].sum(axis=1),
                                    'PowerIn_TM': df[sum_tm_cols].sum(axis=1)
                                    })

    df['COP_DHWSys_2'] = (df[cop_primary_column_name] * (sum_power_in_df['PowerIn_Primary']/df['PowerIn_Total'])) + (cop_tm * (sum_power_in_df['PowerIn_TM']/df['PowerIn_Total']))

    return df

def convert_on_off_col_to_bool(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Function takes in a pandas dataframe of data and a list of column names to convert from the strings 
    "ON" and "OFF" to boolean values True and False resperctively.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of sensor data.
    column_names : list of stings
        list of columns with data currently in strings "ON" and "OFF" that need to be converted to boolean values

    Returns
    -------
    pd.DataFrame: Dataframe with specified columns converted from Celsius to Farenhiet.
    """
    
    mapping = {'ON': True, 'OFF': False}
    
    for column_name in column_names: 
        df[column_name] = df[column_name].map(mapping).where(df[column_name].notna(), df[column_name])
    
    return df

def convert_c_to_f(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Function takes in a pandas dataframe of data and a list of column names to convert from degrees Celsius to Farenhiet.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of sensor data.
    column_names : list of stings
        list of columns with data currently in Celsius that need to be converted to Farenhiet

    Returns
    -------
    pd.DataFrame: Dataframe with specified columns converted from Celsius to Farenhiet.
    """
    for col in column_names:
        if col in df.columns.to_list():
            try:
                pd.to_numeric(df[col])
                df[col] = df[col].apply(temp_c_to_f_non_noaa)
            except ValueError:
                print(f"{col} is not a numeric value column and could not be converted.")
        else:
            print(f"{col} is not included in this data set.")
    return df

def convert_btuhr_to_kw(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Function takes in a pandas dataframe of data and a list of column names to convert from BTU HR to kW.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of sensor data.
    column_names : list of stings
        list of columns with data currently in BTU HR that need to be converted to kW

    Returns
    -------
    pd.DataFrame: Dataframe with specified columns converted from BTU HR to kW.
    """
    for col in column_names:
        if col in df.columns.to_list():
            try:
                pd.to_numeric(df[col])
                df[col] = df[col].apply(power_btuhr_to_kw)
            except ValueError:
                print(f"{col} is not a numeric value column and could not be converted.")
        else:
            print(f"{col} is not included in this data set.")
    return df

def convert_l_to_g(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Function takes in a pandas dataframe of data and a list of column names to convert from Liters to Gallons.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of sensor data.
    column_names : list of stings
        list of columns with data currently in Liters that need to be converted to Gallons

    Returns
    -------
    pd.DataFrame: Dataframe with specified columns converted from Liters to Gallons.
    """
    for col in column_names:
        if col in df.columns.to_list():
            try:
                pd.to_numeric(df[col])
                df[col] = df[col].apply(volume_l_to_g)
            except ValueError:
                print(f"{col} is not a numeric value column and could not be converted.")
        else:
            print(f"{col} is not included in this data set.")
    return df

def flag_dhw_outage(df: pd.DataFrame, daily_df : pd.DataFrame, dhw_outlet_column : str, supply_temp : int = 110, consecutive_minutes : int = 15) -> pd.DataFrame:
    """
     Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of sensor data on minute intervals.
    daily_df : pd.DataFrame
        Single pandas dataframe of sensor data on daily intervals.
    dhw_outlet_column : str
        Name of the column in df and daily_df that contains temperature of DHW supplied to building occupants
    supply_temp : int
        the minimum DHW temperature acceptable to supply to building occupants
    consecutive_minutes : int
        the number of minutes in a row that DHW is not delivered to tenants to qualify as a DHW Outage

    Returns
    -------
    event_df : pd.DataFrame
        Dataframe with 'HW_OUTAGE' events on the days in which there was a DHW Outage.
    """
    # TODO edge case for outage that spans over a day
    events = {
        'time_pt' : [],
        'event_type' : [],
        'event_detail' : [],
    }
    mask = df[dhw_outlet_column] < supply_temp
    for day in daily_df.index:
        print(day)
        next_day = day + pd.Timedelta(days=1)
        filtered_df = mask.loc[(mask.index >= day) & (mask.index < next_day)]

        consecutive_condition = filtered_df.rolling(window=consecutive_minutes).min() == 1
        if consecutive_condition.any():
            # first_true_index = consecutive_condition['supply_temp'].idxmax()
            first_true_index = consecutive_condition.idxmax()
            adjusted_time = first_true_index - pd.Timedelta(minutes=consecutive_minutes-1)
            events['time_pt'].append(day)
            events['event_type'].append("HW_OUTAGE")
            events['event_detail'].append(f"Hot Water Outage Occured (first one starting at {adjusted_time.strftime('%H:%M')})")
    event_df = pd.DataFrame(events)
    event_df.set_index('time_pt', inplace=True)
    return event_df


def aggregate_df(df: pd.DataFrame, ls_filename: str = "", complete_hour_threshold : float = 0.8, complete_day_threshold : float = 1.0, remove_partial : bool = True) -> (pd.DataFrame, pd.DataFrame):
    """
    Function takes in a pandas dataframe of minute data, aggregates it into hourly and daily 
    dataframes, appends 'load_shift_day' column onto the daily_df and the 'system_state' column to
    hourly_df to keep track of the loadshift schedule for the system, and then returns those dataframes.
    The function will only trim the returned dataframes such that only averages from complete hours and
    complete days are returned rather than agregated data from partial datasets.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of minute-by-minute sensor data.
    ls_filename : str
        Path to csv file containing load shift schedule (e.g. "full/path/to/pipeline/input/loadshift_matrix.csv"),
        There should be at least four columns in this csv: 'date', 'startTime', 'endTime', and 'event'
    complete_hour_threshold : float
        Default to 0.8. percent of minutes in an hour needed to count as a complete hour. Percent as a float (e.g. 80% = 0.8) 
        Only applicable if remove_partial set to True
    complete_day_threshold : float
        Default to 1.0. percent of hours in a day needed to count as a complete day. Percent as a float (e.g. 80% = 0.8) 
        Only applicable if remove_partial set to True
    remove_partial : bool
        Default to True. Removes parial days and hours from aggregated dfs 
    
    Returns
    -------
    daily_df : pd.DataFrame
        agregated daily dataframe that contains all daily information as well as the 'load_shift_day' column if
        relevant to the data set.
    hourly_df : pd.DataFrame
        agregated hourly dataframe that contains all hourly information as well as the 'system_state' column if
        relevant to the data set.
    """
    # If df passed in empty, we just return empty dfs for hourly_df and daily_df
    if (df.empty):
        return pd.DataFrame(), pd.DataFrame()

    # Start by splitting the dataframe into sum, which has all energy related vars, and mean, which has everything else. Time is calc'd differently because it's the index
    sum_df = (df.filter(regex=".*Energy.*")).filter(regex="^(?!.*EnergyRate).*(?<!BTU)$")
    # NEEDS TO INCLUDE: EnergyOut_PrimaryPlant_BTU
    mean_df = df.filter(regex="^((?!Energy)(?!EnergyOut_PrimaryPlant_BTU).)*$")

    # Resample downsamples the columns of the df into 1 hour bins and sums/means the values of the timestamps falling within that bin
    hourly_sum = sum_df.resample('H').sum()
    hourly_mean = mean_df.resample('H').mean(numeric_only=True)
    # Same thing as for hours, but for a whole day
    daily_sum = sum_df.resample("D").sum()
    daily_mean = mean_df.resample('D').mean(numeric_only=True)

    # combine sum_df and mean_df into one hourly_df, then try and print that and see if it breaks
    hourly_df = pd.concat([hourly_sum, hourly_mean], axis=1)
    daily_df = pd.concat([daily_sum, daily_mean], axis=1)

    partial_day_removal_exclusion = []

    # appending loadshift data
    if ls_filename != "" and os.path.exists(ls_filename):
        ls_df = pd.read_csv(ls_filename)
        # Parse 'date' and 'startTime' columns to create 'startDateTime'
        ls_df['startDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['startTime'])
        # Parse 'date' and 'endTime' columns to create 'endDateTime'
        ls_df['endDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['endTime'])
        daily_df["load_shift_day"] = False
        hourly_df["system_state"] = 'normal'
        partial_day_removal_exclusion = ["load_shift_day","system_state"]
        for index, row in ls_df.iterrows():
            startDateTime = row['startDateTime']
            endDateTime = row['endDateTime']
            event = row['event']

            # Update 'system_state' in 'hourly_df' and 'load_shift_day' in 'daily_df' based on conditions
            hourly_df.loc[(hourly_df.index >= startDateTime) & (hourly_df.index < endDateTime), 'system_state'] = event
            daily_df.loc[daily_df.index.date == startDateTime.date(), 'load_shift_day'] = True
            daily_df.loc[daily_df.index.date == endDateTime.date(), 'load_shift_day'] = True
    else:
        print(f"The loadshift file '{ls_filename}' does not exist. Thus loadshifting will not be added to daily dataframe.")
    
    # if any day in hourly table is incomplete, we should delete that day from the daily table as the averaged data it contains will be from an incomplete day.
    if remove_partial:
        hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df, complete_hour_threshold, complete_day_threshold, partial_day_removal_exclusion = partial_day_removal_exclusion)
    return hourly_df, daily_df


def create_summary_tables(df: pd.DataFrame):
    """
    Revamped version of "aggregate_data" function. Creates hourly and daily summary tables.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of minute-by-minute sensor data.
    
    Returns
    ------- 
    pd.DataFrame: 
        Two pandas dataframes, one of by the hour and one of by the day aggregated sensor data. 
    """
    # If df passed in empty, we just return empty dfs for hourly_df and daily_df
    if (df.empty):
        return pd.DataFrame(), pd.DataFrame()
    
    hourly_df = df.resample('H').mean()
    daily_df = df.resample('D').mean()

    hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df)
    return hourly_df, daily_df

def remove_partial_days(df, hourly_df, daily_df, complete_hour_threshold : float = 0.8, complete_day_threshold : float = 1.0, partial_day_removal_exclusion : list = []):
    '''
    Helper function for removing daily and hourly values that are calculated from incomplete data.

    Parameters
    ----------
    df : pd.DataFrame
        Single pandas dataframe of minute-by-minute sensor data.
    daily_df : pd.DataFrame
        agregated daily dataframe that contains all daily information.
    hourly_df : pd.DataFrame
        agregated hourly dataframe that contains all hourly information.
    complete_hour_threshold : float
        Default to 0.8. percent of minutes in an hour needed to count as a complete hour. Percent as a float (e.g. 80% = 0.8)
    complete_day_threshold : float
        Default to 1.0. percent of hours in a day needed to count as a complete day. Percent as a float (e.g. 80% = 0.8)
    partial_day_removal_exclusion : list[str]
        List of column names to ignore when searching through columns to remove sections without enough data
    '''
    if complete_hour_threshold < 0.0 or complete_hour_threshold > 1.0:
        raise Exception("complete_hour_threshold must be a float between 0 and 1 to represent a percent (e.g. 80% = 0.8)")
    if complete_day_threshold < 0.0 or complete_day_threshold > 1.0:
        raise Exception("complete_day_threshold must be a float between 0 and 1 to represent a percent (e.g. 80% = 0.8)")
    
    num_minutes_required = 60.0 * complete_hour_threshold
    incomplete_hours = []
    for hour in hourly_df.index:
        next_hour = hour + pd.Timedelta(hours=1)
        filtered_df = df.loc[(df.index >= hour) & (df.index < next_hour)]
        if len(filtered_df.index) < num_minutes_required:
            incomplete_hours.append(hour)
        else:
            for column in hourly_df.columns.to_list():
                if column not in partial_day_removal_exclusion:
                    not_null_count = filtered_df[column].notna().sum()
                    if not_null_count < num_minutes_required:
                        hourly_df.loc[hour, column] = np.nan

    hourly_df = hourly_df.drop(incomplete_hours)
    
    num_complete_hours_required = 24.0 * complete_day_threshold
    incomplete_days = []
    for day in daily_df.index:
        next_day = day + pd.Timedelta(days=1)
        filtered_df = hourly_df.loc[(hourly_df.index >= day) & (hourly_df.index < next_day)]
        if len(filtered_df.index) < num_complete_hours_required:
            incomplete_days.append(day)
        else:
            for column in daily_df.columns.to_list():
                if column not in partial_day_removal_exclusion:
                    not_null_count = filtered_df[column].notna().sum()
                    if not_null_count < num_complete_hours_required:
                        daily_df.loc[day, column] = np.nan
    daily_df = daily_df.drop(incomplete_days)

    return hourly_df, daily_df


def join_to_hourly(hourly_data: pd.DataFrame, noaa_data: pd.DataFrame) -> pd.DataFrame:
    """
    Function left-joins the weather data to the hourly dataframe.

    Parameters
    ---------- 
    hourly_data : pd.DataFrame
        Hourly dataframe
    noaa_data : pd.DataFrame
        noaa dataframe
    
    Returns
    -------
    pd.DataFrame:
        A single, joined dataframe
    """
    out_df = hourly_data.join(noaa_data)
    return out_df


def join_to_daily(daily_data: pd.DataFrame, cop_data: pd.DataFrame) -> pd.DataFrame:
    """
    Function left-joins the the daily data and COP data.

    Parameters
    ---------- 
    daily_data : pd.DataFrame
        Daily dataframe
    cop_data : pd.DataFrame
        cop_values dataframe
    
    Returns
    -------
    pd.DataFrame
        A single, joined dataframe
    """
    out_df = daily_data.join(cop_data)
    return out_df
