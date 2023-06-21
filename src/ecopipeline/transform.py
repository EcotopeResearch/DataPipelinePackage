import pandas as pd
import numpy as np
import datetime as dt
import csv
import os
from ecopipeline.unit_convert import energy_to_power, energy_btu_to_kwh, energy_kwh_to_kbtu, power_flow_to_kW
from ecopipeline.config import _input_directory, _output_directory

pd.set_option('display.max_columns', None)


def concat_last_row(df: pd.DataFrame, last_row: pd.DataFrame) -> pd.DataFrame:
    """
    Function takes in a dataframe and the last row from the SQL database and concatenates the last row
    to the start of the dataframe

    Args: 
        df (pd.DataFrame): Pandas dataframe  
        last_row (pd.DataFrame): last row Pandas dataframe
    Returns: 
        pd.DataFrame: Pandas dataframe with last row concatenated
    """
    df = pd.concat([last_row, df], join="inner")
    return df


def round_time(df: pd.DataFrame):
    """
    Function takes in a dataframe and rounds dataTime index to the nearest minute. Works in place

    Args: 
        df (pd.DataFrame): Pandas dataframe
    Returns: 
        None
    """
    if (df.empty):
        return False
    df.index = df.index.round('T')
    return True


def rename_sensors(df: pd.DataFrame, variable_names_path: str = f"{_input_directory}Variable_Names.csv", site: str = ""):
    """
    Function will take in a dataframe and a string representation of a file path and renames
    sensors from their alias to their true name.

    Args: 
        df (pd.DataFrame): Pandas dataframe
        variable_names_path (str): file location of file containing sensor aliases to their corresponding name (default value of Variable_Names.csv)
        site (str): strin of site name (default to empty string)
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
    try:
        variable_data = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return

    if (site != ""):
        variable_data = variable_data.loc[variable_data['site'] == site]
    
    variable_data = variable_data[['variable_alias', 'variable_name']]
    variable_data.dropna(axis=0, inplace=True)
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))

    df.rename(columns=variable_alias_true_dict, inplace=True)

    # drop columns that do not have a corresponding true name
    df.drop(columns=[col for col in df if col in variable_alias], inplace=True)

    # drop columns that are not documented in variable names csv file at all
    df.drop(columns=[col for col in df if col not in variable_true], inplace=True)


def avg_duplicate_times(df: pd.DataFrame, timezone : str) -> pd.DataFrame:
    """
    Function will take in a dataframe and look for duplicate timestamps due to 
    daylight savings. Takes the average values between the duplicate timestamps.

    Args: 
        df (pd.DataFrame): Pandas dataframe
        timezone (str): Timezone as a string
    Returns: 
        pd.DataFrame: Pandas dataframe 
    """
    df.index = pd.DatetimeIndex(df.index).tz_localize(None)
    df = df.groupby(df.index).mean()
    df.index = (df.index).tz_localize(timezone)
    return df

def _rm_cols(col, bounds_df):  # Helper function for remove_outliers
    """
    Function will take in a pandas series and bounds information
    stored in a dataframe, then check each element of that column and set it to nan
    if it is outside the given bounds. 

    Args: 
        col (pd.Series): Pandas series
        bounds_df (pd.DataFrame): Pandas dataframe
    Returns: 
        None 
    """
    if (col.name in bounds_df.index):
        c_lower = float(bounds_df.loc[col.name]["lower_bound"])
        c_upper = float(bounds_df.loc[col.name]["upper_bound"])
        # for this to be one line, it could be the following:
        #col.mask((col > float(bounds_df.loc[col.name]["upper_bound"])) | (col < float(bounds_df.loc[col.name]["lower_bound"])), other = np.NaN, inplace = True)
        col.mask((col > c_upper) | (col < c_lower), other=np.NaN, inplace=True)

# TODO: remove_outliers STRETCH GOAL: Functionality for alarms being raised based on bounds needs to happen here.


def remove_outliers(df: pd.DataFrame, variable_names_path: str = f"{_input_directory}Variable_Names.csv", site: str = "") -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of bounds information in a csv,
    store the bounds data in a dataframe, then remove outliers above or below bounds as 
    designated by the csv. Function then returns the resulting dataframe. 

    Args: 
        df (pd.DataFrame): Pandas dataframe
        variable_names_path (str): file location of file containing sensor aliases to their corresponding name (default value of Variable_Names.csv)
        site (str): strin of site name (default to empty string)
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
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


def ffill_missing(df: pd.DataFrame, vars_filename: str = f"{_input_directory}Variable_Names.csv", previous_fill: pd.DataFrame = None) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 
    Args: 
        df (pd.DataFrame): Pandas dataframe
        variable_names_path (str): file location of file containing sensor aliases to their corresponding name (default value of Variable_Names.csv)
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
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

def nullify_erroneous(df: pd.DataFrame, vars_filename: str = f"{_input_directory}Variable_Names.csv") -> pd.DataFrame:
    """
    Function will take a pandas dataframe and make erroneous values NaN. 
    Args: 
        df (pd.DataFrame): Pandas dataframe
        variable_names_path (str): file location of file containing sensor aliases to their corresponding name (default value of Variable_Names.csv)
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
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
            df.loc[df[col] <= error_value, col] = np.nan

    return df

def sensor_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reads in input/adjustments.csv and applies necessary adjustments to the dataframe

    Args: 
        df (pd.DataFrame): DataFrame to be adjusted
    Returns: 
        pd.DataFrame: Adjusted Dataframe
    """
    try:
        adjustments = pd.read_csv(f"{_input_directory}adjustments.csv")
    except FileNotFoundError:
        print("File Not Found: ", f"{_input_directory}adjustments.csv")
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

def cop_method_1(df: pd.DataFrame, recircLosses):
    """
    Performs COP calculation method 1 (original AWS method).

    Args:
        df (pd.Dataframe): Pandas dataframe to add COP columns (daily)
        recircLosses (float or pd.Series): If fixedTM from spot measurement,
            float. If recic measurements in datastream, column of daily df.
            Units should be in kW.
    """
    df['COP_DHWSys_1'] = (df['HeatOut_Primary'] + recircLosses) / df['PowerIn_Total']
    
    
    return df

def cop_method_2(df: pd.DataFrame, cop_tm, cop_primary_column_name):
    """
    Performs COP calculation method 2 as defined by Scott's whiteboard image
    COP = COP_primary(ELEC_primary/ELEC_total) + COP_tm(ELEC_tm/ELEC_total)

    Args: 
        df (pd.DataFrame): Pandas DataFrame to add COP columns to
        cop_tm (float): fixed COP value for temputure Maintenece system
        cop_primary_column_name (str): Name of the column used for COP_Primary values

    Returns: 
        pd.DataFrame: Pandas DataFrame with the added COP columns. 
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
                                    'PowerIn_TM': df[sum_tm_cols].sum(axis=1)})

    df['COP_DHWSys_2'] = (df[cop_primary_column_name] * (sum_power_in_df['PowerIn_Primary']/df['PowerIn_Total'])) + (cop_tm * (sum_power_in_df['PowerIn_TM']/df['PowerIn_Total']))
    return df

# NOTE: Move to bayview.py
# loops through a list of dateTime objects, compares if the date of that object matches the
# date of the row name, which is also a dateTime object. If it matches, load_shift is True (happened that day)
def _ls_helper(row, dt_list):
    """
    Function takes in a pandas series and a list of dates, then checks
    each entry in the series and if it matches a date in the list of dates,
    sets the series load_shift_day to True. 
    Args: 
        row (pd.Series): Pandas series 
        list (<class 'list'>): Python list
    Output: 
        row (pd.Series): Pandas series
    """
    for date in dt_list:
        if (row.name.date() == date.date()):
            row.loc["load_shift_day"] = True
    return row

# NOTE: Move to bayview.py
def aggregate_df(df: pd.DataFrame):
    """
    Function takes in a pandas dataframe of minute data, aggregates it into hourly and daily 
    dataframes, appends some loadshift data onto the daily df, and then returns those. 
    Args: 
        df (pd.DataFrame): Single pandas dataframe of minute-by-minute sensor data.
    Returns: 
        pd.DataFrame: Two pandas dataframes, one of by the hour and one of by the day aggregated sensor data.
    """
    # If df passed in empty, we just return empty dfs for hourly_df and daily_df
    if (df.empty):
        return pd.DataFrame(), pd.DataFrame()

    # Start by splitting the dataframe into sum, which has all energy related vars, and mean, which has everything else. Time is calc'd differently because it's the index
    sum_df = (df.filter(regex=".*Energy.*")).filter(regex=".*[^BTU]$")
    # NEEDS TO INCLUDE: EnergyOut_PrimaryPlant_BTU
    mean_df = df.filter(regex="^((?!Energy)(?!EnergyOut_PrimaryPlant_BTU).)*$")

    # Resample downsamples the columns of the df into 1 hour bins and sums/means the values of the timestamps falling within that bin
    hourly_sum = sum_df.resample('H').sum()
    hourly_mean = mean_df.resample('H').mean()
    # Same thing as for hours, but for a whole day
    daily_sum = sum_df.resample("D").sum()
    daily_mean = mean_df.resample('D').mean()

    # combine sum_df and mean_df into one hourly_df, then try and print that and see if it breaks
    hourly_df = pd.concat([hourly_sum, hourly_mean], axis=1)
    daily_df = pd.concat([daily_sum, daily_mean], axis=1)

    # appending loadshift data
    filename = f"{_input_directory}loadshift_matrix.csv"
    date_list = []
    with open(filename) as datefile:
        readCSV = csv.reader(datefile, delimiter=',')
        for row in readCSV:
            date_list.append(row[0])
        date_list.pop(0)
    # date_list is a list of strings in the following format: "1/19/2023", OR "%m/%d/%Y", now we convert to datetime!
    format = "%m/%d/%Y"
    dt_list = []
    for date in date_list:
        dt_list.append(dt.datetime.strptime(date, format))
    daily_df["load_shift_day"] = False
    daily_df = daily_df.apply(_ls_helper, axis=1, args=(dt_list,))
    
    # if any day in hourly table is incomplete, we should delete that day from the daily table as the averaged data it contains will be from an incomplete day.
    hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df)
    return hourly_df, daily_df


def create_summary_tables(df: pd.DataFrame):
    """
    Revamped version of "aggregate_data" function. Creates hourly and daily summary tables.
    Args: 
        df (pd.DataFrame): Single pandas dataframe of minute-by-minute sensor data.
    Returns: 
        pd.DataFrame: Two pandas dataframes, one of by the hour and one of by the day aggregated sensor data. 
    """
    # If df passed in empty, we just return empty dfs for hourly_df and daily_df
    if (df.empty):
        return pd.DataFrame(), pd.DataFrame()
    
    hourly_df = df.resample('H').mean()
    daily_df = df.resample('D').mean()

    hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df)
    return hourly_df, daily_df

def remove_partial_days(df, hourly_df, daily_df):
    '''
    Helper function for removing daily values that are calculated from incomplete data.
    '''

    hourly_start = df.index[0].ceil("H") 
    hourly_end = df.index[-1].floor("H") - pd.DateOffset(hours=1)
    hourly_df = hourly_df[hourly_start: (hourly_end)]

    daily_start = df.index[0].ceil("D")
    daily_end = df.index[-1].floor("D") - pd.DateOffset(days=1)
    daily_df = daily_df[daily_start: (daily_end)]

    return hourly_df, daily_df


def join_to_hourly(hourly_data: pd.DataFrame, noaa_data: pd.DataFrame) -> pd.DataFrame:
    """
    Function left-joins the weather data to the hourly dataframe.

    Args: 
        hourly_data (pd.DataFrame):Hourly dataframe
        noaa_data (pd.DataFrame): noaa dataframe
    Returns: 
        pd.DataFrame: A single, joined dataframe
    """
    out_df = hourly_data.join(noaa_data)
    return out_df


def join_to_daily(daily_data: pd.DataFrame, cop_data: pd.DataFrame) -> pd.DataFrame:
    """
    Function left-joins the the daily data and COP data.

    Args: 
        daily_data (pd.DataFrame): Daily dataframe
        cop_data (pd.DataFrame): cop_values dataframe
    Returns: 
        pd.DataFrame: A single, joined dataframe
    """
    out_df = daily_data.join(cop_data)
    return out_df
