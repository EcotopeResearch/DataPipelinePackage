import pandas as pd
import numpy as np
import datetime as dt
import csv
import os
from ecotope_package_cs2306.unit_convert import energy_to_power, energy_btu_to_kwh, energy_kwh_to_kbtu
from ecotope_package_cs2306.config import _input_directory, _output_directory

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


def avg_duplicate_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function will take in a dataframe and look for duplicate timestamps due to 
    daylight savings. Takes the average values between the duplicate timestamps.
    The resulting dataframe will be timezone naive. 

    Args: 
        df (pd.DataFrame): Pandas dataframe
    Returns: 
        pd.DataFrame: Pandas dataframe 
    """
    df.index = pd.DatetimeIndex(df.index).tz_localize(None)
    df = df.groupby(df.index).mean()
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


def _ffill(col, ffill_df):  # Helper function for ffill_missing
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
        cp = ffill_df.loc[col.name]["changepoint"]
        length = ffill_df.loc[col.name]["ffill_length"]
        if (length != length):  # check for nan, set to 0
            length = 0
        length = int(length)  # casting to int to avoid float errors
        if (cp == 1):  # ffill unconditionally
            col.fillna(method='ffill', inplace=True)
        elif (cp == 0):  # ffill only up to length
            col.fillna(method='ffill', inplace=True, limit=length)


def ffill_missing(df: pd.DataFrame, vars_filename: str = f"{_input_directory}Variable_Names.csv") -> pd.DataFrame:
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

    df.apply(_ffill, args=(ffill_df,))
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

    return hourly_df, daily_df

# NOTE: Move to bayview.py
def set_zone_vol(location: pd.Series, gals: int, total: int, zones: pd.Series) -> pd.DataFrame:
    """
    Function that initializes the dataframe that holds the volumes of each zone.

    Args:
        location (pd.Series)
        gals (int) 
        total (int) 
        zones (pd.Series)
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
    relative_loc = location
    tank_frxn = relative_loc.subtract(relative_loc.shift(-1))
    gal_per_tank = gals
    tot_storage = total
    zone_gals = tank_frxn * tot_storage
    zone_gals = pd.Series.dropna(zone_gals)  # remove NA from leading math
    zone_list = zones
    gals_per_zone = pd.DataFrame({'Zone': zone_list, 'Zone_vol_g': zone_gals})
    return gals_per_zone

# NOTE: Move to bayview.py
def _largest_less_than(df_row: pd.Series, target: int) -> str:
    """
    Function takes a list of gz/json filenames and a target temperature and determines
    the zone with the highest temperature < 120 degrees.

    Args: 
        df_row (pd.DataFrame): A single row of a sensor Pandas Dataframe in a series 
        target (int): integer target
    Output: 
        str: A string of the name of the zone.
    """
    count = 0
    largest_less_than_120_tmp = []
    for val in df_row:
        if val < target:
            largest_less_than_120_tmp = df_row.index[count]
            break
        count = count + 1

    return largest_less_than_120_tmp

# NOTE: Move to bayview.py
def _get_vol_equivalent_to_120(df_row: pd.Series, location: pd.Series, gals: int, total: int, zones: pd.Series) -> float:
    """
    Function takes a row of sensor data and finds the total volume of water > 120 degrees.

    Args: 
        df_row (pd.Series) 
        location (pd.Series)
        gals (int)
        total (int)
        zones (pd.Series)
    Returns: 
        float: A float of the total volume of water > 120 degrees
    """
    try:
        tvadder = 0
        vadder = 0
        gals_per_zone = set_zone_vol(location, gals, total, zones)
        dfcheck = df_row.filter(regex='top|mid|bottom')
        # An empty or invalid dataframe would have Vol120 and ZoneTemp120 as columns with
        # values of 0, so we check if the size is 0 without those columns if the dataframe has no data.
        if (dfcheck.size == 0):
            return 0
        dftemp = df_row.filter(
            regex='Temp_CityWater_atSkid|HPWHOutlet$|top|mid|bottom|120')
        count = 1
        for val in dftemp:
            if dftemp.index[count] == "Temp_low":
                vadder += gals_per_zone[gals_per_zone.columns[1]][count]
                tvadder += val * gals_per_zone[gals_per_zone.columns[1]][count]
                break
            elif dftemp[dftemp.index[count + 1]] >= 120:
                vadder += gals_per_zone[gals_per_zone.columns[1]][count]
                tvadder += (dftemp[dftemp.index[count + 1]] + val) / \
                    2 * gals_per_zone[gals_per_zone.columns[1]][count]
            elif dftemp[dftemp.index[count + 1]] < 120:
                vadder += dftemp.get('Vol120')
                tvadder += dftemp.get('Vol120') * dftemp.get('ZoneTemp120')
                break
            count += 1
        avg_temp_above_120 = tvadder / vadder
        temp_ratio = (avg_temp_above_120 - dftemp[0]) / (120 - dftemp[0])
        return (temp_ratio * vadder)
    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return 0

# NOTE: Move to bayview.py
def _get_V120(df_row: pd.Series, location: pd.Series, gals: int, total: int, zones: pd.Series):
    """
    Function takes a row of sensor data and determines the volume of water > 120 degrees
    in the zone that has the highest sensor < 120 degrees.

    Args: 
        df_row (pd.Series): A single row of a sensor Pandas Dataframe in a series
        location (pd.Series)
        gals (int)
        total (int)
        zones (pd.Series)
    Returns: 
        float: A float of the total volume of water > 120 degrees     
    """
    try:
        gals_per_zone = set_zone_vol(location, gals, total, zones)
        temp_cols = df_row.filter(regex='HPWHOutlet$|top|mid|bottom')
        if (temp_cols.size <= 3):
            return 0
        name_cols = ""
        name_cols = _largest_less_than(temp_cols, 120)
        count = 0
        for index in temp_cols.index:
            if index == name_cols:
                name_col_index = count
                break
            count += 1
        dV = gals_per_zone['Zone_vol_g'][name_col_index]
        V120 = (temp_cols[temp_cols.index[name_col_index]] - 120) / (
            temp_cols[temp_cols.index[name_col_index]] - temp_cols[temp_cols.index[name_col_index - 1]]) * dV
        return V120
    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return 0

# NOTE: Move to bayview.py
def _get_zone_Temp120(df_row: pd.Series) -> float:
    """
    Function takes a row of sensor data and determines the highest sensor < 120 degrees.

    Args: 
        df_row (pd.Series): A single row of a sensor Pandas Dataframe in a series
    Returns: 
        float: A float of the average temperature of the zone < 120 degrees
    """
    # if df_row["Temp_120"] != 120:
    #    return 0
    temp_cols = df_row.filter(regex='HPWHOutlet$|top|mid|bottom')
    if (temp_cols.size <= 3):
        return 0
    name_cols = _largest_less_than(temp_cols, 120)
    count = 0
    for index in temp_cols.index:
        if index == name_cols:
            name_col_index = count
            break
        count += 1

    zone_Temp_120 = (120 + temp_cols[temp_cols.index[name_col_index - 1]]) / 2
    return zone_Temp_120

# NOTE: Move to bayview.py
def get_storage_gals120(df: pd.DataFrame, location: pd.Series, gals: int, total: int, zones: pd.Series) -> pd.DataFrame:
    """
    Function that creates and appends the Gals120 data onto the Dataframe

    Args: 
        df (pd.Series): A Pandas Dataframe
        location (pd.Series)
        gals (int)
        total (int)
        zones (pd.Series)
    Returns: 
        pd.DataFrame: a Pandas Dataframe
    """
    if (len(df) > 0):
        df['Vol120'] = df.apply(_get_V120, args=(
            location, gals, total, zones), axis=1)
        df['ZoneTemp120'] = df.apply(_get_zone_Temp120, axis=1)
        df['Vol_Equivalent_to_120'] = df.apply(
            _get_vol_equivalent_to_120, args=(location, gals, total, zones), axis=1)

    return df

# NOTE: Move to bayview.py
def _calculate_average_zone_temp(df: pd.DataFrame, substring: str):
    """
    Function that calculates the average temperature of the inputted zone.

    Args: 
        df (pd.Series): A Pandas Dataframe
        substring (str)
    Returns: 
        pd.DataFrame: a Pandas Dataframe
    """
    try:
        df_subset = df[[x for x in df if substring in x]]
        result = df_subset.sum(axis=1, skipna=True) / df_subset.count(axis=1)
        return result
    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return 0

# NOTE: Move to bayview.py
def get_temp_zones120(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function that keeps track of the average temperature of each zone.

    Args: 
        df (pd.Series): A Pandas Dataframe
    Returns: 
        pd.DataFrame: a Pandas Dataframe
    """
    df['Temp_top'] = _calculate_average_zone_temp(df, "Temp1")
    df['Temp_midtop'] = _calculate_average_zone_temp(df, "Temp2")
    df['Temp_mid'] = _calculate_average_zone_temp(df, "Temp3")
    df['Temp_midbottom'] = _calculate_average_zone_temp(df, "Temp4")
    df['Temp_bottom'] = _calculate_average_zone_temp(df, "Temp5")
    return df


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
