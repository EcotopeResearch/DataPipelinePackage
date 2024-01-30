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


def rename_sensors(original_df: pd.DataFrame, variable_names_path: str = f"{_input_directory}Variable_Names.csv", site: str = "", system: str = ""):
    """
    Function will take in a dataframe and a string representation of a file path and renames
    sensors from their alias to their true name. Also filters the dataframe by site and system if specified.

    Parameters
    ---------- 
    original_df: pd.DataFrame
        A dataframe that contains data labeled by the raw varriable names to be renamed.
    variable_names_path: str 
        file location of file containing sensor aliases to their corresponding name (default value of {_input_directory from your config file}Variable_Names.csv)
        the csv this points to should have at least 2 columns called "variable_alias" (the raw name to be changed from) and "variable_name"
        (the name to be changed to). All columns without a cooresponding variable_name will be dropped from the datframe.
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
        Varriable_Names.csv.
    """
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
    df.drop(columns=[col for col in df if col in variable_alias], inplace=True)

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
def remove_outliers(original_df: pd.DataFrame, variable_names_path: str = f"{_input_directory}Variable_Names.csv", site: str = "") -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of bounds information in a csv,
    store the bounds data in a dataframe, then remove outliers above or below bounds as 
    designated by the csv. Function then returns the resulting dataframe. 

    Parameters
    ----------
    original_df: pd.DataFrame
        Pandas dataframe for which outliers need to be removed
    variable_names_path: str
        Path to csv file containing sensor names and cooresponding upper/lower boundaries (default value of Variable_Names.csv in configured input location)
    site: str
        string of site name if processing a particular site in a Variable_Names.csv file with multiple sites

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with outliers removed and replaced with nans
    """
    df = original_df.copy()
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


def ffill_missing(original_df: pd.DataFrame, vars_filename: str = f"{_input_directory}Variable_Names.csv", previous_fill: pd.DataFrame = None) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 
    
    Parameters
    ----------
    original_df: pd.DataFrame
        Pandas dataframe that needs to be forward filled
    variable_names_path: str
        Path to csv file containing variable names and cooresponding changepoint and ffill_length (default value of Variable_Names.csv in configured input location),
        There should be at least three columns in this csv: "variable_name", "changepoint", "ffill_length"
    previous_fill: pd.DataFrame (default None)
        A pandas dataframe with the same index type and at least some of the same columns as original_df (usually taken as the last entry from the pipeline that has been put
        into the destination database). The values of this will be used to forward fill into the new set of data if applicable.
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas dataframe that has been forward filled to the specifications detailed in the vars_filename csv
    """
    df = original_df.copy()
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

def nullify_erroneous(original_df: pd.DataFrame, vars_filename: str = f"{_input_directory}Variable_Names.csv") -> pd.DataFrame:
    """
    Function will take a pandas dataframe and make erroneous values NaN. 

    Parameters
    ---------- 
    original_df: pd.DataFrame
        Pandas dataframe that needs to be forward filled
    variable_names_path: str
        Path to csv file containing variable names and cooresponding error values (default value of Variable_Names.csv in configured input location),
        There should be at least two columns in this csv: "variable_name" and "error_value"
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas dataframe with error values replaced with NaNs
    """
    df = original_df.copy()
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

#TODO investigate if this can be removed
def sensor_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    TO BE DEPRICATED -- Reads in input/adjustments.csv and applies necessary adjustments to the dataframe

    Parameters
    ---------- 
    df : pd.DataFrame
        DataFrame to be adjusted
    
    Returns
    ------- 
    pd.DataFrame: 
        Adjusted Dataframe
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

def cop_method_1(df: pd.DataFrame, recircLosses) -> pd.DataFrame:
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

    Returns
    -------
    pd.DataFrame: Dataframe with added column for system COP called COP_DHWSys_1
    """
    columns_to_check = ['HeatOut_Primary', 'PowerIn_Total']

    missing_columns = [col for col in columns_to_check if col not in df.columns]

    if missing_columns:
        print('Cannot calculate COP as the following columns are missing from the DataFrame:', missing_columns)
        return df
    
    df['COP_DHWSys_1'] = (df['HeatOut_Primary'] + recircLosses) / df['PowerIn_Total']
    
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

def aggregate_df(df: pd.DataFrame, ls_filename: str = f"{_input_directory}loadshift_matrix.csv") -> (pd.DataFrame, pd.DataFrame):
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
        Path to csv file containing load shift schedule (default value of loadshift_matrix.csv in configured input location),
        There should be at least four columns in this csv: 'date', 'startTime', 'endTime', and 'event'
    
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
    hourly_mean = mean_df.resample('H').mean()
    # Same thing as for hours, but for a whole day
    daily_sum = sum_df.resample("D").sum()
    daily_mean = mean_df.resample('D').mean()

    # combine sum_df and mean_df into one hourly_df, then try and print that and see if it breaks
    hourly_df = pd.concat([hourly_sum, hourly_mean], axis=1)
    daily_df = pd.concat([daily_sum, daily_mean], axis=1)

    # appending loadshift data
    if os.path.exists(ls_filename):
        
        ls_df = pd.read_csv(ls_filename)
        # Parse 'date' and 'startTime' columns to create 'startDateTime'
        ls_df['startDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['startTime'])
        # Parse 'date' and 'endTime' columns to create 'endDateTime'
        ls_df['endDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['endTime'])
        daily_df["load_shift_day"] = False
        hourly_df["system_state"] = 'normal'
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
    hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df)
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
